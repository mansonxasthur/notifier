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
	"github.com/mansonxasthur/notifier/internal/adapters/ratelimit"
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
	status, _, resp := doResp(t, method, url, body, header...)
	return status, resp
}

// doResp is do, and also returns the response headers.
func doResp(t *testing.T, method, url string, body []byte, header ...string) (int, http.Header, []byte) {
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
	return resp.StatusCode, resp.Header, buf.Bytes()
}

// stack is every component, wired as the three entry points and the simulator wire them.
type stack struct {
	db     *postgres.Pool
	apiURL string
	simURL string
}

// newStack wires the whole stack from the shipped configuration. Each tune changes that
// configuration before anything is built from it.
func newStack(t *testing.T, tune ...func(*config.Config)) *stack {
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
	// The breaker has its own tests. Here it must not open, or a run of failures would change
	// which provider a delivery uses and the oracle below could not predict it.
	cfg.Sender.Breaker.FailureThreshold = 1_000_000
	for _, f := range tune {
		f(cfg)
	}

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
		Limiter: ratelimit.NewLocal(cfg.Limits.Global.Policy(), time.Now),
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
	providers := provider.NewRegistry(cfg, nil)
	attempt := sender.NewAttempt(postgres.NewDeliveryStore(db), inbox, providers, providers,
		sender.NewBreakers(cfg.Sender.Breaker.Policy(), time.Now),
		sender.NewProviderLimits(cfg.ProviderRateLimits(), time.Now),
		sender.Routing{Order: cfg.ProviderOrder(), Policies: cfg.ProviderRetryPolicies()}, quiet)
	intake := sender.NewIntake(inbox, cfg.PrimaryProviders(), cfg.Sender.MaxClaims, attempt.Send, quiet)
	scan := sender.NewScan(inbox, attempt.Send, cfg.Sender.PollInterval, cfg.Sender.ScanBatch, cfg.Sender.MaxClaims, quiet)
	start(t, sender.New(rabbitmq.NewConsumer(mq, cfg.Sender.Prefetch, quiet), intake, scan))

	return &stack{db: db, apiURL: apiURL, simURL: simURL}
}

const defaultKey = "7c9e6679-7425-40de-944b-e07fc1f90ae7"

// submit sends one notification with `recipients` recipients on sms and email, and returns
// how many deliveries were accepted.
func (s *stack) submit(t *testing.T) int {
	t.Helper()
	status, _, resp := s.post(t, defaultKey)
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

// post sends the notification of submit under the given idempotency key and returns the raw answer.
func (s *stack) post(t *testing.T, key string) (int, http.Header, []byte) {
	t.Helper()
	var rs []string
	for i := range recipients {
		rs = append(rs, fmt.Sprintf(`{"recipient_id":"u_%03d","channels":{"sms":"+2010%08d","email":"u%03d@example.com"}}`, i, i, i))
	}
	body := fmt.Sprintf(`{"idempotency_key":%q,"type":"match_alert",
		"content":{"sms":{"text":"مباراة الهلال والنصر"},"email":{"template_id":"match","variables":{"team":"الهلال"}}},
		"recipients":[%s]}`, key, strings.Join(rs, ","))
	return doResp(t, http.MethodPost, s.apiURL+"/v1/notifications", []byte(body), "X-API-KEY", "k", "Content-Type", "application/json")
}

// entries reads the simulator's record of every delivery it received, at each provider.
func (s *stack) entries(t *testing.T) []simprovider.Entry {
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
	return out.Entries
}

// decisions reads what the simulator decided, by delivery id and then provider.
func (s *stack) decisions(t *testing.T) map[int64]map[string]domain.Scenario {
	t.Helper()
	m := map[int64]map[string]domain.Scenario{}
	for _, e := range s.entries(t) {
		if m[e.DeliveryID] == nil {
			m[e.DeliveryID] = map[string]domain.Scenario{}
		}
		m[e.DeliveryID][e.Provider] = e.Scenario
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

// outcome is where one provider's scenario leaves a delivery. moveOn means the sender gives
// up on this provider and tries the next one. last is whether this is the channel's last
// provider, and lookup whether it answers status lookups.
func outcome(sc domain.Scenario, last, lookup bool) (r row, moveOn bool) {
	switch sc {
	case domain.ScenarioSucceed, domain.ScenarioSlow:
		return row{state: "DELIVERED", inbox: "DONE"}, false
	case domain.ScenarioLateFailure:
		return row{state: "FAILED", reason: "BOUNCED", inbox: "DONE"}, false
	case domain.ScenarioFailPermanent:
		return row{state: "FAILED", reason: "PROVIDER_REJECTED", inbox: "DEAD", class: "message"}, false
	case domain.ScenarioFailTransient:
		// Retried on the same provider (the scenario replays), then the next provider; on the
		// last one the delivery ends.
		if !last {
			return row{}, true
		}
		return row{state: "FAILED", reason: "PROVIDER_FAILED", inbox: "DEAD", class: "provider"}, false
	case domain.ScenarioTimeout:
		// The provider holds the message and says so when asked, and its own webhook finishes it.
		if lookup {
			return row{state: "DELIVERED", inbox: "DONE"}, false
		}
		if !last {
			return row{}, true
		}
		// No answer from the last provider leaves the delivery UNRESOLVED until its late
		// webhook settles it.
		return row{state: "DELIVERED", inbox: "DEAD", class: "timeout"}, false
	}
	panic("unknown scenario " + string(sc))
}

// expected is the final row for a delivery given what the simulator decided at its primary
// provider and, if the sender moved on, at the fallback. ok is false when the decisions
// contradict the path the sender must have taken.
func expected(primary string, d map[string]domain.Scenario) (r row, usedFallback, ok bool) {
	fallback := strings.TrimSuffix(primary, "-a") + "-b"
	pr, move := outcome(d[primary], false, true)
	if !move {
		_, touched := d[fallback]
		return pr, false, !touched
	}
	fb, seen := d[fallback]
	if !seen {
		return row{}, true, false
	}
	r, _ = outcome(fb, true, false)
	if r.inbox == "DONE" {
		r.class = "provider" // the switch recorded why the primary was given up on
	}
	return r, true, true
}

// primaryOf returns the provider a delivery was first sent to: the "-a" one.
func primaryOf(d map[string]domain.Scenario) string {
	for name := range d {
		if strings.HasSuffix(name, "-a") {
			return name
		}
	}
	return ""
}

// A message flows from the request to its final state through the simulated provider, in
// every scenario, including retries, failover to the second provider and late webhooks. The
// simulator's own record of what it decided, at each provider the sender used, is the
// oracle: every delivery must end where those decisions say.
func TestRequestToFinalStateThroughTheSimulator(t *testing.T) {
	s := newStack(t)
	want := s.submit(t)
	if want != recipients*2 {
		t.Fatalf("accepted = %d, want %d", want, recipients*2)
	}

	var rows map[int64]row
	var decided map[int64]map[string]domain.Scenario
	settled := func() bool {
		decided, rows = s.decisions(t), s.rows(t)
		if len(decided) != want || len(rows) != want {
			return false
		}
		for id, d := range decided {
			if r, _, ok := expected(primaryOf(d), d); !ok || rows[id] != r {
				return false
			}
		}
		return true
	}
	deadline := time.Now().Add(90 * time.Second)
	for !settled() {
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	primaries := map[domain.Scenario]int{}
	fallbacks := map[domain.Scenario]int{}
	holding := 0
	for id, d := range decided {
		primary := primaryOf(d)
		primaries[d[primary]]++
		r, usedFallback, ok := expected(primary, d)
		if !ok {
			t.Errorf("delivery %d: decisions %v do not fit any path the sender can take", id, d)
			continue
		}
		if usedFallback {
			fallbacks[d[strings.TrimSuffix(primary, "-a")+"-b"]]++
		}
		if rows[id] != r {
			t.Errorf("delivery %d (%v) ended %+v, want %+v", id, d, rows[id], r)
		}
		for _, sc := range d {
			switch sc {
			case domain.ScenarioSucceed, domain.ScenarioSlow, domain.ScenarioTimeout, domain.ScenarioLateFailure:
				holding++
			}
		}
	}
	for _, sc := range domain.Scenarios() {
		if primaries[sc] == 0 {
			t.Errorf("scenario %s was never drawn at a primary; the run proves nothing about it", sc)
		}
		if fallbacks[sc] == 0 {
			t.Errorf("scenario %s was never drawn at a fallback; the run proves nothing about failover", sc)
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
	if events != holding {
		t.Errorf("webhook_events = %d, want %d: one per provider that took the message", events, holding)
	}
	if applied != events {
		t.Errorf("applied = %d of %d: each delivery gets one webhook, so each moves it", applied, events)
	}
}
