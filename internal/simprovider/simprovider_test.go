package simprovider_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/mansonxasthur/notifier/internal/adapters/provider"
	"github.com/mansonxasthur/notifier/internal/adapters/webhookauth"
	"github.com/mansonxasthur/notifier/internal/config"
	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/simprovider"
)

var quiet = slog.New(slog.DiscardHandler)

// received is a webhook the fake API got.
type received struct {
	provider string
	header   http.Header
	body     []byte
}

// api is a fake API that records the webhooks it receives.
type api struct {
	srv *httptest.Server
	mu  sync.Mutex
	got []received
}

func newAPI(t *testing.T) *api {
	t.Helper()
	a := &api{}
	a.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		a.mu.Lock()
		a.got = append(a.got, received{provider: r.URL.Path, header: r.Header.Clone(), body: body})
		a.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(a.srv.Close)
	return a
}

func (a *api) waitFor(t *testing.T, n int) []received {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		a.mu.Lock()
		if len(a.got) >= n {
			out := append([]received(nil), a.got...)
			a.mu.Unlock()
			return out
		}
		a.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("webhooks received = %d, want %d", len(a.got), n)
	return nil
}

func (a *api) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.got)
}

// newConfig is a two-provider SMS setup: sms-a has a status lookup and signs with HMAC,
// sms-b has none and uses a shared secret header.
func newConfig(mode string, api *api) *config.Config {
	return &config.Config{
		SimProvider: config.SimProvider{
			Mode: mode, Seed: 1,
			Weights:   map[domain.Scenario]int{domain.ScenarioSucceed: 1},
			SlowDelay: 120 * time.Millisecond, TimeoutDelay: 250 * time.Millisecond,
			WebhookDelay: 20 * time.Millisecond, WebhookURL: api.srv.URL,
		},
		WebhookSecrets: map[string]config.Secret{"sms-a": "secret-a", "sms-b": "secret-b"},
		Channels: map[domain.Channel]config.Channel{domain.ChannelSMS: {Providers: []config.Provider{
			{Name: "sms-a", Primary: true, StatusLookup: true,
				Webhook: config.Webhook{Auth: config.WebhookHMAC, Header: "X-Signature", SecretEnv: "A"}},
			{Name: "sms-b",
				Webhook: config.Webhook{Auth: config.WebhookSharedSecret, Header: "X-Webhook-Token", SecretEnv: "B"}},
		}}},
	}
}

// sim starts the simulator and returns its URL.
func sim(t *testing.T, cfg *config.Config) string {
	t.Helper()
	hooks := simprovider.NewWebhooks(cfg, quiet)
	if err := hooks.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hooks.Stop(context.Background()) })

	mux := http.NewServeMux()
	simprovider.NewServer(cfg, hooks, quiet).Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL
}

func do(method, url string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(context.Background(), method, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return http.DefaultClient.Do(req) //nolint:gosec // a test server on localhost
}

func post(t *testing.T, base, name string, id int64, payload provider.SendPayload) (int, time.Duration) {
	t.Helper()
	payload.DeliveryID = id
	body, _ := json.Marshal(payload)
	start := time.Now()
	resp, err := do(http.MethodPost, base+"/providers/"+name+"/send", body)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode, time.Since(start)
}

func get(t *testing.T, url string) (int, map[string]any) {
	t.Helper()
	resp, err := do(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

var sms = provider.SendPayload{Channel: domain.ChannelSMS, Contact: "+201000000000", Content: provider.ContentPayload{Text: "مرحبا"}}

func TestScenarioResponses(t *testing.T) {
	tests := []struct {
		scenario   domain.Scenario
		wantStatus int
		wantHook   string // "" for none
		minTime    time.Duration
	}{
		{domain.ScenarioSucceed, http.StatusAccepted, provider.WebhookDelivered, 0},
		{domain.ScenarioFailPermanent, http.StatusBadRequest, "", 0},
		{domain.ScenarioFailTransient, http.StatusServiceUnavailable, "", 0},
		{domain.ScenarioSlow, http.StatusAccepted, provider.WebhookDelivered, 120 * time.Millisecond},
		{domain.ScenarioTimeout, http.StatusAccepted, provider.WebhookDelivered, 250 * time.Millisecond},
		{domain.ScenarioLateFailure, http.StatusAccepted, provider.WebhookFailed, 0},
	}
	for _, tt := range tests {
		t.Run(string(tt.scenario), func(t *testing.T) {
			a := newAPI(t)
			base := sim(t, newConfig(string(tt.scenario), a))

			status, took := post(t, base, "sms-a", 7, sms)
			if status != tt.wantStatus {
				t.Errorf("status = %d, want %d", status, tt.wantStatus)
			}
			if took < tt.minTime {
				t.Errorf("answered in %v, want at least %v", took, tt.minTime)
			}

			if tt.wantHook == "" {
				time.Sleep(150 * time.Millisecond)
				if a.count() != 0 {
					t.Errorf("sent %d webhooks, want none", a.count())
				}
				return
			}
			var p provider.WebhookPayload
			if err := json.Unmarshal(a.waitFor(t, 1)[0].body, &p); err != nil {
				t.Fatal(err)
			}
			if p.DeliveryID != 7 || p.Status != tt.wantHook {
				t.Errorf("webhook = %+v, want delivery 7 %s", p, tt.wantHook)
			}
			if tt.wantHook == provider.WebhookFailed && !domain.FailureReason(p.Reason).Valid() {
				t.Errorf("reason %q is not in the failure enum", p.Reason)
			}
		})
	}
}

func TestSlowAndTimedOutCallsSendTheirWebhookAfterAnswering(t *testing.T) {
	for scenario, delay := range map[domain.Scenario]time.Duration{
		domain.ScenarioSlow: 120 * time.Millisecond, domain.ScenarioTimeout: 250 * time.Millisecond,
	} {
		t.Run(string(scenario), func(t *testing.T) {
			a := newAPI(t)
			base := sim(t, newConfig(string(scenario), a))

			start := time.Now()
			post(t, base, "sms-a", 1, sms)
			answered := time.Since(start)
			if a.count() != 0 {
				t.Fatalf("a webhook arrived before the %v answer", delay)
			}
			a.waitFor(t, 1)
			if got := time.Since(start); got < delay+20*time.Millisecond || got < answered {
				t.Errorf("webhook after %v, want after the answer plus the webhook delay", got)
			}
		})
	}
}

func TestWebhookRejectedByTheAPIIsNotRetried(t *testing.T) {
	var mu sync.Mutex
	var hits int
	rejecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		w.WriteHeader(http.StatusUnauthorized)
	}))
	t.Cleanup(rejecting.Close)

	cfg := newConfig(string(domain.ScenarioSucceed), newAPI(t))
	cfg.SimProvider.WebhookURL = rejecting.URL
	base := sim(t, cfg)
	post(t, base, "sms-a", 1, sms)

	time.Sleep(600 * time.Millisecond) // longer than all retries would take
	mu.Lock()
	defer mu.Unlock()
	if hits != 1 {
		t.Errorf("API hit %d times, want 1: a 401 cannot be fixed by sending again", hits)
	}
}

func TestWebhooksStopWithoutStart(t *testing.T) {
	hooks := simprovider.NewWebhooks(newConfig(string(domain.ScenarioSucceed), newAPI(t)), quiet)
	if err := hooks.Stop(context.Background()); err != nil {
		t.Errorf("Stop before Start = %v", err)
	}
}

func TestWebhooksAreSignedPerProvider(t *testing.T) {
	a := newAPI(t)
	cfg := newConfig(string(domain.ScenarioSucceed), a)
	base := sim(t, cfg)

	post(t, base, "sms-a", 1, sms)
	post(t, base, "sms-b", 2, sms)
	got := a.waitFor(t, 2)

	for _, r := range got {
		switch r.provider {
		case "/v1/webhooks/sms-a":
			want, _ := webhookauth.Sign(config.WebhookHMAC, "secret-a", r.body)
			if r.header.Get("X-Signature") != want {
				t.Errorf("sms-a signature = %q, want the HMAC of the body", r.header.Get("X-Signature"))
			}
		case "/v1/webhooks/sms-b":
			if r.header.Get("X-Webhook-Token") != "secret-b" {
				t.Errorf("sms-b token = %q, want its shared secret", r.header.Get("X-Webhook-Token"))
			}
		default:
			t.Errorf("unexpected path %q", r.provider)
		}
	}
}

func TestStatusLookup(t *testing.T) {
	a := newAPI(t)
	base := sim(t, newConfig(string(domain.ScenarioSucceed), a))

	if status, _ := get(t, base+"/providers/sms-a/status/5"); status != http.StatusNotFound {
		t.Errorf("before a send: status = %d, want 404", status)
	}
	post(t, base, "sms-a", 5, sms)
	if status, _ := get(t, base+"/providers/sms-a/status/5"); status != http.StatusOK {
		t.Errorf("after a send: status = %d, want 200", status)
	}
	if status, _ := get(t, base+"/providers/sms-a/status/6"); status != http.StatusNotFound {
		t.Errorf("another delivery: status = %d, want 404", status)
	}

	post(t, base, "sms-b", 5, sms)
	if status, _ := get(t, base+"/providers/sms-b/status/5"); status != http.StatusNotImplemented {
		t.Errorf("provider without lookup: status = %d, want 501", status)
	}
	if status, _ := get(t, base+"/providers/nope/status/5"); status != http.StatusNotFound {
		t.Errorf("unknown provider: status = %d, want 404", status)
	}
}

func TestStatusLookupOnlyFindsHeldMessages(t *testing.T) {
	for scenario, want := range map[domain.Scenario]int{
		domain.ScenarioFailPermanent: http.StatusNotFound,
		domain.ScenarioFailTransient: http.StatusNotFound,
		domain.ScenarioTimeout:       http.StatusOK, // too slow to answer, but it got the message
		domain.ScenarioLateFailure:   http.StatusOK,
	} {
		t.Run(string(scenario), func(t *testing.T) {
			base := sim(t, newConfig(string(scenario), newAPI(t)))
			post(t, base, "sms-a", 9, sms)
			if status, _ := get(t, base+"/providers/sms-a/status/9"); status != want {
				t.Errorf("status = %d, want %d", status, want)
			}
		})
	}
}

func TestRepeatedSendReplaysTheScenario(t *testing.T) {
	a := newAPI(t)
	cfg := newConfig(config.RandomMode, a)
	cfg.SimProvider.Weights = map[domain.Scenario]int{domain.ScenarioFailTransient: 1, domain.ScenarioSucceed: 1}
	base := sim(t, cfg)

	first, _ := post(t, base, "sms-a", 3, sms)
	for range 10 {
		if again, _ := post(t, base, "sms-a", 3, sms); again != first {
			t.Fatalf("a repeat answered %d, first answered %d: the scenario must be replayed", again, first)
		}
	}
	if first == http.StatusAccepted {
		a.waitFor(t, 1)
		time.Sleep(100 * time.Millisecond)
		if a.count() != 1 {
			t.Errorf("sent %d webhooks for one delivery, want 1", a.count())
		}
	}

	_, body := get(t, base+"/scenarios/3")
	entries := body["entries"].([]any)
	if len(entries) != 1 || entries[0].(map[string]any)["sends"].(float64) != 11 {
		t.Errorf("entries = %v, want one entry with 11 sends", entries)
	}
}

func TestEachProviderGetsItsOwnScenario(t *testing.T) {
	cfg := newConfig(config.RandomMode, newAPI(t))
	base := sim(t, cfg)
	post(t, base, "sms-a", 4, sms)
	post(t, base, "sms-b", 4, sms)

	_, body := get(t, base+"/scenarios/4")
	if got := len(body["entries"].([]any)); got != 2 {
		t.Errorf("entries = %d, want one per provider", got)
	}
	if status, _ := get(t, base+"/scenarios/404"); status != http.StatusNotFound {
		t.Errorf("unknown delivery: status = %d, want 404", status)
	}
}

func TestScenariosListsCounts(t *testing.T) {
	base := sim(t, newConfig(string(domain.ScenarioFailPermanent), newAPI(t)))
	post(t, base, "sms-a", 1, sms)
	post(t, base, "sms-a", 2, sms)

	_, body := get(t, base+"/scenarios")
	counts := body["counts"].(map[string]any)
	if counts["fail_permanent"].(float64) != 2 || len(body["entries"].([]any)) != 2 {
		t.Errorf("body = %v, want two fail_permanent entries", body)
	}
}

func TestSendValidation(t *testing.T) {
	base := sim(t, newConfig(string(domain.ScenarioSucceed), newAPI(t)))
	tests := []struct {
		name string
		path string
		body string
		want int
	}{
		{"unknown provider", "/providers/nope/send", `{}`, http.StatusNotFound},
		{"malformed", "/providers/sms-a/send", `{`, http.StatusBadRequest},
		{"no reference key", "/providers/sms-a/send", `{"channel":"sms","contact":"+2010","content":{"text":"hi"}}`, http.StatusBadRequest},
		{"unknown channel", "/providers/sms-a/send", `{"delivery_id":1,"channel":"fax","contact":"x","content":{"text":"hi"}}`, http.StatusBadRequest},
		{"no contact", "/providers/sms-a/send", `{"delivery_id":1,"channel":"sms","content":{"text":"hi"}}`, http.StatusBadRequest},
		{"sms with a template", "/providers/sms-a/send", `{"delivery_id":1,"channel":"sms","contact":"x","content":{"template_id":"t"}}`, http.StatusBadRequest},
		{"unknown field", "/providers/sms-a/send", `{"delivery_id":1,"channel":"sms","contact":"x","content":{"text":"hi"},"extra":1}`, http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := do(http.MethodPost, base+tt.path, []byte(tt.body))
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != tt.want {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.want)
			}
		})
	}
}

func TestAcceptsTemplateContent(t *testing.T) {
	base := sim(t, newConfig(string(domain.ScenarioSucceed), newAPI(t)))
	for _, ch := range []domain.Channel{domain.ChannelEmail, domain.ChannelWhatsApp} {
		p := provider.SendPayload{Channel: ch, Contact: "x", Content: provider.ContentPayload{
			TemplateID: "welcome", Variables: map[string]string{"name": "مرحبا"},
		}}
		if status, _ := post(t, base, "sms-a", int64(10+len(ch)), p); status != http.StatusAccepted {
			t.Errorf("%s template: status = %d, want 202", ch, status)
		}
	}
	inline := provider.SendPayload{Channel: domain.ChannelEmail, Contact: "x", Content: provider.ContentPayload{Subject: "s", Body: "b"}}
	if status, _ := post(t, base, "sms-a", 99, inline); status != http.StatusAccepted {
		t.Errorf("inline email: status = %d, want 202", status)
	}
}

func TestChooser(t *testing.T) {
	t.Run("pinned", func(t *testing.T) {
		c := simprovider.NewChooser(config.SimProvider{Mode: string(domain.ScenarioTimeout)})
		for range 5 {
			if got := c.Pick(); got != domain.ScenarioTimeout {
				t.Fatalf("pick = %s, want timeout", got)
			}
		}
	})

	t.Run("a provider's own mode wins over the global one", func(t *testing.T) {
		c := simprovider.NewChooser(config.SimProvider{
			Mode: string(domain.ScenarioSucceed), Providers: map[string]string{"sms-a": string(domain.ScenarioFailTransient)},
		})
		for range 5 {
			if got := c.PickFor("sms-a"); got != domain.ScenarioFailTransient {
				t.Fatalf("sms-a pick = %s, want fail_transient", got)
			}
			if got := c.PickFor("sms-b"); got != domain.ScenarioSucceed {
				t.Fatalf("sms-b pick = %s, want the global succeed", got)
			}
		}
	})

	cfg := config.SimProvider{Mode: config.RandomMode, Seed: 42, Weights: map[domain.Scenario]int{
		domain.ScenarioSucceed: 70, domain.ScenarioTimeout: 30, domain.ScenarioSlow: 0,
	}}

	t.Run("a seed reproduces the sequence", func(t *testing.T) {
		a, b := simprovider.NewChooser(cfg), simprovider.NewChooser(cfg)
		for i := range 50 {
			if x, y := a.Pick(), b.Pick(); x != y {
				t.Fatalf("pick %d: %s vs %s", i, x, y)
			}
		}
	})

	t.Run("weights are respected", func(t *testing.T) {
		c := simprovider.NewChooser(cfg)
		counts := map[domain.Scenario]int{}
		for range 10000 {
			counts[c.Pick()]++
		}
		if counts[domain.ScenarioSlow] != 0 || counts[domain.ScenarioFailPermanent] != 0 {
			t.Errorf("zero-weight scenarios were picked: %v", counts)
		}
		if got := float64(counts[domain.ScenarioSucceed]) / 10000; got < 0.66 || got > 0.74 {
			t.Errorf("succeed share = %.2f, want about 0.70", got)
		}
	})
}
