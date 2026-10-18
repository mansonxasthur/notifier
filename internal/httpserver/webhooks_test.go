package httpserver_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mansonxasthur/notifier/internal/adapters/postgres"
	"github.com/mansonxasthur/notifier/internal/adapters/postgres/pgtest"
	"github.com/mansonxasthur/notifier/internal/adapters/webhookauth"
	"github.com/mansonxasthur/notifier/internal/config"
	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/httpserver"
	"github.com/mansonxasthur/notifier/internal/ports"
)

// fakeWebhookStore records the events it is given.
type fakeWebhookStore struct {
	mu      sync.Mutex
	events  []ports.WebhookEvent
	applied bool
	err     error
}

func (f *fakeWebhookStore) Apply(_ context.Context, e ports.WebhookEvent) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, e)
	return f.applied, f.err
}

func authenticators(t *testing.T) map[string]ports.WebhookAuthenticator {
	t.Helper()
	cfg := &config.Config{
		WebhookSecrets: map[string]config.Secret{"sms-a": "secret-a", "sms-b": "secret-b"},
		Channels: map[domain.Channel]config.Channel{domain.ChannelSMS: {Providers: []config.Provider{
			{Name: "sms-a", Webhook: config.Webhook{Auth: config.WebhookHMAC, Header: "X-Signature", SecretEnv: "A"}},
			{Name: "sms-b", Webhook: config.Webhook{Auth: config.WebhookSharedSecret, Header: "X-Webhook-Token", SecretEnv: "B"}},
		}}},
	}
	a, err := webhookauth.FromConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func webhookMux(t *testing.T, store ports.WebhookStore, maxBody int64) *http.ServeMux {
	t.Helper()
	mux := http.NewServeMux()
	err := httpserver.RegisterWebhooks(mux, httpserver.WebhooksOptions{
		Log: slog.New(slog.DiscardHandler), Store: store, Authenticators: authenticators(t), MaxBodyBytes: maxBody,
		Channels: map[string]domain.Channel{"sms-a": domain.ChannelSMS, "sms-b": domain.ChannelSMS},
	})
	if err != nil {
		t.Fatal(err)
	}
	return mux
}

// signedA sends body to sms-a signed with its HMAC secret.
func signedA(t *testing.T, mux http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	sig, _ := webhookauth.Sign(config.WebhookHMAC, "secret-a", []byte(body))
	return post(t, mux, "/v1/webhooks/sms-a", body, "X-Signature", sig)
}

func post(t *testing.T, mux http.Handler, path, body string, kv ...string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, path, bytes.NewReader([]byte(body)))
	for i := 0; i+1 < len(kv); i += 2 {
		req.Header.Set(kv[i], kv[i+1])
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func TestWebhookEndpointRejections(t *testing.T) {
	good := `{"delivery_id":1,"status":"delivered"}`
	tests := []struct {
		name string
		do   func(mux http.Handler) *httptest.ResponseRecorder
		want int
	}{
		{"unknown provider", func(m http.Handler) *httptest.ResponseRecorder {
			return post(t, m, "/v1/webhooks/nope", good, "X-Signature", "x")
		}, http.StatusNotFound},
		{"no signature", func(m http.Handler) *httptest.ResponseRecorder {
			return post(t, m, "/v1/webhooks/sms-a", good)
		}, http.StatusUnauthorized},
		{"wrong signature", func(m http.Handler) *httptest.ResponseRecorder {
			return post(t, m, "/v1/webhooks/sms-a", good, "X-Signature", strings.Repeat("0", 64))
		}, http.StatusUnauthorized},
		{"signature of another body", func(m http.Handler) *httptest.ResponseRecorder {
			sig, _ := webhookauth.Sign(config.WebhookHMAC, "secret-a", []byte(`{"delivery_id":2,"status":"delivered"}`))
			return post(t, m, "/v1/webhooks/sms-a", good, "X-Signature", sig)
		}, http.StatusUnauthorized},
		{"signature of another provider", func(m http.Handler) *httptest.ResponseRecorder {
			sig, _ := webhookauth.Sign(config.WebhookHMAC, "secret-b", []byte(good))
			return post(t, m, "/v1/webhooks/sms-a", good, "X-Signature", sig)
		}, http.StatusUnauthorized},
		{"shared secret provider with the wrong token", func(m http.Handler) *httptest.ResponseRecorder {
			return post(t, m, "/v1/webhooks/sms-b", good, "X-Webhook-Token", "nope")
		}, http.StatusUnauthorized},
		{"not JSON", func(m http.Handler) *httptest.ResponseRecorder { return signedA(t, m, `{`) }, http.StatusBadRequest},
		{"no delivery id", func(m http.Handler) *httptest.ResponseRecorder {
			return signedA(t, m, `{"status":"delivered"}`)
		}, http.StatusBadRequest},
		{"unknown status", func(m http.Handler) *httptest.ResponseRecorder {
			return signedA(t, m, `{"delivery_id":1,"status":"queued"}`)
		}, http.StatusBadRequest},
		{"too large", func(m http.Handler) *httptest.ResponseRecorder {
			return signedA(t, m, `{"delivery_id":1,"status":"delivered","pad":"`+strings.Repeat("x", 600)+`"}`)
		}, http.StatusRequestEntityTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeWebhookStore{applied: true}
			rec := tt.do(webhookMux(t, store, 512))
			if rec.Code != tt.want {
				t.Errorf("status = %d, want %d (body %s)", rec.Code, tt.want, rec.Body)
			}
			if len(store.events) != 0 {
				t.Errorf("store got %d events from a refused webhook, want none", len(store.events))
			}
		})
	}
}

func TestWebhookEndpointAppliesEvents(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		wantOutcome domain.DeliveryState
		wantReason  domain.FailureReason
	}{
		{"delivered", `{"delivery_id":7,"status":"delivered"}`, domain.DeliveryDelivered, ""},
		{"failed with a known reason", `{"delivery_id":7,"status":"failed","reason":"BOUNCED"}`, domain.DeliveryFailed, domain.ReasonBounced},
		{"failed with the provider's own wording", `{"delivery_id":7,"status":"failed","reason":"mailbox_full"}`, domain.DeliveryFailed, domain.ReasonProviderFailed},
		{"failed with no reason", `{"delivery_id":7,"status":"failed"}`, domain.DeliveryFailed, domain.ReasonProviderFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeWebhookStore{applied: true}
			rec := signedA(t, webhookMux(t, store, 4096), tt.body)
			if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"applied":true`) {
				t.Fatalf("response = %d %s", rec.Code, rec.Body)
			}
			if len(store.events) != 1 {
				t.Fatalf("store got %d events, want 1", len(store.events))
			}
			e := store.events[0]
			if e.DeliveryID != 7 || e.Provider != "sms-a" || e.Channel != domain.ChannelSMS || e.Outcome != tt.wantOutcome || e.Reason != tt.wantReason {
				t.Errorf("event = %+v", e)
			}
			if string(e.Payload) != tt.body {
				t.Errorf("payload = %q, want the body as received", e.Payload)
			}
		})
	}
}

func TestWebhookEndpointAcknowledgesIgnoredEvents(t *testing.T) {
	store := &fakeWebhookStore{applied: false}
	rec := signedA(t, webhookMux(t, store, 4096), `{"delivery_id":7,"status":"delivered"}`)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"applied":false`) {
		t.Errorf("response = %d %s, want 200 with applied false: the provider cannot act on a duplicate", rec.Code, rec.Body)
	}
}

func TestWebhookEndpointStoreFailures(t *testing.T) {
	tests := map[string]struct {
		err  error
		want int
	}{
		"unknown delivery": {ports.ErrDeliveryNotFound, http.StatusNotFound},
		"database down":    {errors.New("connection refused"), http.StatusInternalServerError},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			rec := signedA(t, webhookMux(t, &fakeWebhookStore{err: tt.err}, 4096), `{"delivery_id":7,"status":"delivered"}`)
			if rec.Code != tt.want {
				t.Errorf("status = %d, want %d: a 5xx makes the provider retry", rec.Code, tt.want)
			}
		})
	}
}

func TestRegisterWebhooksRefusesAnEmptySetup(t *testing.T) {
	log := slog.New(slog.DiscardHandler)
	if err := httpserver.RegisterWebhooks(http.NewServeMux(), httpserver.WebhooksOptions{Log: log, Store: &fakeWebhookStore{}}); err == nil {
		t.Error("want an error with no authenticators")
	}
	if err := httpserver.RegisterWebhooks(http.NewServeMux(), httpserver.WebhooksOptions{Log: log, Authenticators: authenticators(t)}); err == nil {
		t.Error("want an error with no store")
	}
	if err := httpserver.RegisterWebhooks(http.NewServeMux(), httpserver.WebhooksOptions{
		Log: log, Store: &fakeWebhookStore{}, Authenticators: authenticators(t),
	}); err == nil {
		t.Error("want an error when a provider has no channel")
	}
}

// A webhook that arrives before the sender's own write finds the delivery in PENDING and
// finishes it; a later contradictory one is stored and ignored. Both events are kept.
func TestWebhookEndpointWithTheDatabase(t *testing.T) {
	ctx := context.Background()
	db := postgres.New(pgtest.NewMigratedDB(t))
	if err := db.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Stop(ctx) })

	n := &domain.Notification{
		IdempotencyKey: "0b6f6b3e-6d4e-4d6e-9d57-2f1d3f0f6f10", Type: "match_alert",
		Content: map[domain.Channel]domain.Content{domain.ChannelSMS: {Text: "hi"}},
		Deliveries: []domain.Delivery{{
			RecipientID: "u_1", Channel: domain.ChannelSMS, Contact: "+201000000000", ExpiresAt: time.Now().Add(time.Hour),
		}},
	}
	if _, err := postgres.NewNotificationStore(db, 5000).Create(ctx, n); err != nil {
		t.Fatal(err)
	}
	var id int64
	if err := db.Pool().QueryRow(ctx, `SELECT id FROM deliveries`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	mux := webhookMux(t, postgres.NewWebhookStore(db), 4096)

	first := signedA(t, mux, fmt.Sprintf(`{"delivery_id":%d,"status":"delivered"}`, id))
	second := signedA(t, mux, fmt.Sprintf(`{"delivery_id":%d,"status":"failed","reason":"BOUNCED"}`, id))
	if !strings.Contains(first.Body.String(), `"applied":true`) || !strings.Contains(second.Body.String(), `"applied":false`) {
		t.Fatalf("responses = %s %s, want applied true then false", first.Body, second.Body)
	}

	var state string
	var reason *string
	if err := db.Pool().QueryRow(ctx, `SELECT state, failure_reason FROM deliveries WHERE id = $1`, id).Scan(&state, &reason); err != nil {
		t.Fatal(err)
	}
	if state != "DELIVERED" || reason != nil {
		t.Errorf("delivery = %s, reason %v; want DELIVERED with none: the first terminal webhook wins", state, reason)
	}
	var events int
	if err := db.Pool().QueryRow(ctx, `SELECT count(*) FROM webhook_events WHERE delivery_id = $1`, id).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 2 {
		t.Errorf("webhook_events = %d, want both kept", events)
	}

	missing := signedA(t, mux, `{"delivery_id":999999,"status":"delivered"}`)
	if missing.Code != http.StatusNotFound {
		t.Errorf("unknown delivery: status = %d, want 404", missing.Code)
	}
}
