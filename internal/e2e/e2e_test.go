// Package e2e runs the whole delivery path in one process: the API, the relay, the broker,
// the sender, the simulated provider and its webhooks, against real Postgres and RabbitMQ.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mansonxasthur/notifier/internal/adapters/postgres"
	"github.com/mansonxasthur/notifier/internal/adapters/postgres/pgtest"
	"github.com/mansonxasthur/notifier/internal/adapters/provider"
	"github.com/mansonxasthur/notifier/internal/adapters/rabbitmq"
	"github.com/mansonxasthur/notifier/internal/adapters/rabbitmq/rmqtest"
	"github.com/mansonxasthur/notifier/internal/adapters/webhookauth"
	"github.com/mansonxasthur/notifier/internal/config"
	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/httpserver"
	"github.com/mansonxasthur/notifier/internal/httpserver/requests"
	"github.com/mansonxasthur/notifier/internal/ports"
	"github.com/mansonxasthur/notifier/internal/relay"
	"github.com/mansonxasthur/notifier/internal/sender"
	"github.com/mansonxasthur/notifier/internal/simprovider"
)

const (
	shippedConfig = "../../config/config.yaml"
	recipients    = 60
)

var quiet = slog.New(slog.DiscardHandler)

type component interface {
	Start(context.Context) error
	Stop(context.Context) error
}

func start(t *testing.T, c component) {
	t.Helper()
	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Stop(ctx) })
}

func serve(t *testing.T, h http.Handler) string {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL
}

// client has a timeout, so a hung handler fails the test instead of stalling it.
var client = &http.Client{Timeout: 10 * time.Second}

func do(t *testing.T, method, url string, body []byte, header ...string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	resp, err := client.Do(req) //nolint:gosec // a test server on localhost
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(resp.Body)
	return resp.StatusCode, buf.Bytes()
}

// stack is every component, wired as the three entry points and the simulator wire them.
type stack struct {
	db     *postgres.Pool
	apiURL string
	simURL string
}

func newStack(t *testing.T) *stack {
	t.Helper()
	ctx := context.Background()

	env := func(k string) string {
		if strings.HasPrefix(k, "WEBHOOK_SECRET_") {
			return "secret-" + k
		}
		return ""
	}
	cfg, err := config.Load(shippedConfig, env)
	if err != nil {
		t.Fatal(err)
	}
	// Every scenario equally likely, from a fixed seed, so the run is repeatable.
	cfg.SimProvider.Seed = 42
	cfg.SimProvider.Weights = map[domain.Scenario]int{}
	for _, s := range domain.Scenarios() {
		cfg.SimProvider.Weights[s] = 1
	}
	cfg.SimProvider.WebhookDelay = 100 * time.Millisecond

	db := postgres.New(pgtest.NewMigratedDB(t))
	if err := db.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Stop(ctx) })

	// The API.
	authenticators, err := webhookauth.FromConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	expiry := map[string]time.Duration{}
	for name, nt := range cfg.NotificationTypes {
		expiry[name] = nt.Expiry
	}
	mux := http.NewServeMux()
	if err := httpserver.RegisterNotifications(mux, httpserver.NotificationsOptions{
		Log: quiet, Store: postgres.NewNotificationStore(db, cfg.Accept.ChunkSize), APIKey: "k",
		Policy: requests.Policy{BatchLimit: cfg.BatchLimit, Expiry: expiry}, MaxBodyBytes: cfg.Accept.MaxBodyBytes,
	}); err != nil {
		t.Fatal(err)
	}
	if err := httpserver.RegisterWebhooks(mux, httpserver.WebhooksOptions{
		Log: quiet, Store: postgres.NewWebhookStore(db), Authenticators: authenticators, Channels: cfg.ProviderChannels(),
	}); err != nil {
		t.Fatal(err)
	}
	apiURL := serve(t, mux)

	// The simulated provider, sending its webhooks to that API.
	cfg.SimProvider.WebhookURL = apiURL
	hooks := simprovider.NewWebhooks(cfg, quiet)
	start(t, hooks)
	simMux := http.NewServeMux()
	simprovider.NewServer(cfg, hooks, quiet).Register(simMux)
	simURL := serve(t, simMux)
	cfg.SimulationURL = simURL

	// The broker, the relay and the sender.
	broker := rmqtest.Start(t)
	mq := rabbitmq.New(broker.URL)
	start(t, mq)
	pub := rabbitmq.NewPublisher(mq, 5*time.Second)
	t.Cleanup(pub.Stop)
	backoff := domain.Backoff{Initial: 20 * time.Millisecond, Max: 100 * time.Millisecond, Multiplier: 2}
	start(t, relay.New(postgres.NewOutboxStore(db), func() ports.Publisher { return pub },
		relay.Config{PollInterval: 10 * time.Millisecond, BatchSize: 100, Workers: 2, MaxAttempts: 10, Backoff: backoff}, quiet))

	inbox := postgres.NewInboxStore(db)
	attempt := sender.NewAttempt(postgres.NewDeliveryStore(db), inbox, provider.NewRegistry(cfg, nil), cfg.ProviderBackoffs(), quiet)
	intake := sender.NewIntake(inbox, cfg.PrimaryProviders(), cfg.Sender.MaxClaims, attempt.Send, quiet)
	start(t, sender.New(rabbitmq.NewConsumer(mq, cfg.Sender.Prefetch, quiet), intake))

	return &stack{db: db, apiURL: apiURL, simURL: simURL}
}

// submit sends one notification with `recipients` recipients on sms and email, and returns
// how many deliveries were accepted.
func (s *stack) submit(t *testing.T) int {
	t.Helper()
	var rs []string
	for i := range recipients {
		rs = append(rs, fmt.Sprintf(`{"recipient_id":"u_%03d","channels":{"sms":"+2010%08d","email":"u%03d@example.com"}}`, i, i, i))
	}
	body := fmt.Sprintf(`{"idempotency_key":"7c9e6679-7425-40de-944b-e07fc1f90ae7","type":"match_alert",
		"content":{"sms":{"text":"مباراة الهلال والنصر"},"email":{"template_id":"match","variables":{"team":"الهلال"}}},
		"recipients":[%s]}`, strings.Join(rs, ","))
	status, resp := do(t, http.MethodPost, s.apiURL+"/v1/notifications", []byte(body), "X-API-KEY", "k", "Content-Type", "application/json")
	if status != http.StatusAccepted {
		t.Fatalf("submit: %d %s", status, resp)
	}
	var out struct {
		Accepted int `json:"accepted"`
	}
	if err := json.Unmarshal(resp, &out); err != nil {
		t.Fatal(err)
	}
	return out.Accepted
}

// decisions reads what the simulator decided, by delivery id.
func (s *stack) decisions(t *testing.T) map[int64]domain.Scenario {
	t.Helper()
	status, body := do(t, http.MethodGet, s.simURL+"/scenarios", nil)
	if status != http.StatusOK {
		t.Fatalf("scenarios: %d", status)
	}
	var out struct {
		Entries []simprovider.Entry `json:"entries"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	m := map[int64]domain.Scenario{}
	for _, e := range out.Entries {
		m[e.DeliveryID] = e.Scenario
	}
	return m
}

type row struct {
	state, reason, inbox, class string
}

func (s *stack) rows(t *testing.T) map[int64]row {
	t.Helper()
	q, err := s.db.Pool().Query(context.Background(), `
		SELECT d.id, d.state, coalesce(d.failure_reason, ''), coalesce(i.state, ''), coalesce(i.failure_class, '')
		FROM deliveries d LEFT JOIN inbox i ON i.delivery_id = d.id`)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	out := map[int64]row{}
	for q.Next() {
		var id int64
		var r row
		if err := q.Scan(&id, &r.state, &r.reason, &r.inbox, &r.class); err != nil {
			t.Fatal(err)
		}
		out[id] = r
	}
	return out
}

// expected is the final state each scenario must lead to in Phase 4. Phase 5 adds the retry
// scan and failover, which move the parked rows on; these expectations change with it.
var expected = map[domain.Scenario]row{
	domain.ScenarioSucceed:       {state: "DELIVERED", inbox: "DONE"},
	domain.ScenarioSlow:          {state: "DELIVERED", inbox: "DONE"},
	domain.ScenarioLateFailure:   {state: "FAILED", reason: "BOUNCED", inbox: "DONE"},
	domain.ScenarioFailPermanent: {state: "FAILED", reason: "PROVIDER_REJECTED", inbox: "DEAD", class: "message"},
	// A transient failure is parked for the retry scan, which Phase 5 adds.
	domain.ScenarioFailTransient: {state: "PENDING", inbox: "WAITING_RETRY", class: "provider"},
	// The call timed out and was parked, then the provider's own webhook finished it.
	domain.ScenarioTimeout: {state: "DELIVERED", inbox: "WAITING_RETRY", class: "timeout"},
}

// A message flows from the request to its final state through the simulated provider, in
// every scenario, including the late failure webhook. The simulator's own record of what it
// decided is the oracle: every delivery must end where its scenario says.
func TestRequestToFinalStateThroughTheSimulator(t *testing.T) {
	s := newStack(t)
	want := s.submit(t)
	if want != recipients*2 {
		t.Fatalf("accepted = %d, want %d", want, recipients*2)
	}

	var rows map[int64]row
	var decided map[int64]domain.Scenario
	settled := func() bool {
		decided, rows = s.decisions(t), s.rows(t)
		if len(decided) != want || len(rows) != want {
			return false
		}
		for id, sc := range decided {
			if rows[id] != expected[sc] {
				return false
			}
		}
		return true
	}
	deadline := time.Now().Add(60 * time.Second)
	for !settled() {
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	seen := map[domain.Scenario]int{}
	for id, sc := range decided {
		seen[sc]++
		if rows[id] != expected[sc] {
			t.Errorf("delivery %d (%s) ended %+v, want %+v", id, sc, rows[id], expected[sc])
		}
	}
	for _, sc := range domain.Scenarios() {
		if seen[sc] == 0 {
			t.Errorf("scenario %s was never drawn; the run proves nothing about it", sc)
		}
	}
	if len(decided) != want {
		t.Errorf("simulator saw %d deliveries, want %d", len(decided), want)
	}

	// Every webhook is kept, and the guarded transition applied each at most once.
	var events, applied int
	if err := s.db.Pool().QueryRow(context.Background(),
		`SELECT count(*), count(*) FILTER (WHERE applied) FROM webhook_events`).Scan(&events, &applied); err != nil {
		t.Fatal(err)
	}
	webhooks := seen[domain.ScenarioSucceed] + seen[domain.ScenarioSlow] + seen[domain.ScenarioTimeout] + seen[domain.ScenarioLateFailure]
	if events != webhooks {
		t.Errorf("webhook_events = %d, want %d: one per delivery the provider accepted", events, webhooks)
	}
	if applied != events {
		t.Errorf("applied = %d of %d: each delivery gets one webhook, so each moves it", applied, events)
	}
}
