package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mansonxasthur/notifier/internal/adapters/ratelimit"
	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/httpserver/requests"
	"github.com/mansonxasthur/notifier/internal/ports"
)

const (
	testAPIKey = "secret-key"
	testKey    = "0b6f6b3e-6d4e-4d6e-9d57-2f1d3f0f6f10"
)

var testNow = time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)

type fakeStore struct {
	res   ports.CreateResult
	err   error
	calls int
	got   *domain.Notification
}

func (f *fakeStore) Create(_ context.Context, n *domain.Notification) (ports.CreateResult, error) {
	f.calls++
	f.got = n
	n.ID = 42
	return f.res, f.err
}

// fakeLimiter records the charges it is asked for and answers with a fixed decision.
type fakeLimiter struct {
	decision ports.Decision
	err      error
	charges  []int
}

func (f *fakeLimiter) Allow(_ context.Context, n int) (ports.Decision, error) {
	f.charges = append(f.charges, n)
	return f.decision, f.err
}

func newHandler(store ports.NotificationStore, maxBody int64) http.Handler {
	return newHandlerWith(store, maxBody, &fakeLimiter{decision: ports.Decision{Allowed: true}})
}

func newHandlerWith(store ports.NotificationStore, maxBody int64, limiter ports.Limiter) http.Handler {
	mux := Health(slog.New(slog.DiscardHandler), pinger{})
	err := RegisterNotifications(mux, NotificationsOptions{
		Log:    slog.New(slog.DiscardHandler),
		Store:  store,
		APIKey: testAPIKey,
		Policy: requests.Policy{
			BatchLimit: 3,
			Expiry:     map[string]time.Duration{"match_alert": 2 * time.Hour},
		},
		Limiter:      limiter,
		MaxBodyBytes: maxBody,
		Now:          func() time.Time { return testNow },
	})
	if err != nil {
		panic(err)
	}
	return mux
}

func body(recipients string, mutate ...func(m map[string]any)) string {
	m := map[string]any{
		"idempotency_key": testKey,
		"type":            "match_alert",
		"content":         map[string]any{"sms": map[string]any{"text": "مرحبا"}},
	}
	var rs []any
	if err := json.Unmarshal([]byte(recipients), &rs); err != nil {
		panic(err)
	}
	m["recipients"] = rs
	for _, f := range mutate {
		f(m)
	}
	b, _ := json.Marshal(m)
	return string(b)
}

const (
	okRecipient  = `{"recipient_id":"u_1","channels":{"sms":"+201000000000"}}`
	badRecipient = `{"recipient_id":"u_bad","channels":{"sms":"nope"}}`
)

func post(t *testing.T, h http.Handler, key, payload string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/notifications", strings.NewReader(payload))
	if key != "" {
		req.Header.Set("X-API-KEY", key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("body %q: %v", rec.Body.String(), err)
	}
	return v
}

func TestCreateAuth(t *testing.T) {
	for name, key := range map[string]string{"missing": "", "wrong": "nope", "prefix": "secret"} {
		t.Run(name, func(t *testing.T) {
			store := &fakeStore{}
			rec := post(t, newHandler(store, 1<<20), key, body("["+okRecipient+"]"))
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", rec.Code)
			}
			if got := decode[map[string]string](t, rec)["error"]; got != "UNAUTHORIZED" {
				t.Errorf("error = %q", got)
			}
			if store.calls != 0 {
				t.Error("an unauthenticated request must not reach the store")
			}
		})
	}
}

func TestRegisterRefusesEmptyKey(t *testing.T) {
	err := RegisterNotifications(http.NewServeMux(), NotificationsOptions{Log: slog.New(slog.DiscardHandler), Store: &fakeStore{}})
	if err == nil {
		t.Fatal("want an error for an empty API key")
	}
}

func TestRegisterRefusesNilLimiter(t *testing.T) {
	err := RegisterNotifications(http.NewServeMux(), NotificationsOptions{Log: slog.New(slog.DiscardHandler), Store: &fakeStore{}, APIKey: testAPIKey})
	if err == nil {
		t.Fatal("want an error for a nil limiter: intake would be unlimited")
	}
}

func TestCreateRateLimited(t *testing.T) {
	two := okRecipient + `,{"recipient_id":"u_2","channels":{"sms":"+201000000001"}}`
	tests := []struct {
		name       string
		wait       time.Duration
		retryAfter string
	}{
		{"rounds up to whole seconds", 1500 * time.Millisecond, "2"},
		{"never below one second", 20 * time.Millisecond, "1"},
		{"exact seconds stay", 3 * time.Second, "3"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeStore{res: ports.CreateResult{Created: 2}}
			limiter := &fakeLimiter{decision: ports.Decision{RetryAfter: tc.wait}}
			rec := post(t, newHandlerWith(store, 1<<20, limiter), testAPIKey, body("["+two+"]"))
			if rec.Code != http.StatusTooManyRequests {
				t.Fatalf("status = %d, want 429", rec.Code)
			}
			if got := rec.Header().Get("Retry-After"); got != tc.retryAfter {
				t.Errorf("Retry-After = %q, want %q", got, tc.retryAfter)
			}
			if got := decode[map[string]string](t, rec)["error"]; got != "RATE_LIMITED" {
				t.Errorf("error = %q, want RATE_LIMITED", got)
			}
			if store.calls != 0 {
				t.Error("a refused request must not reach the store")
			}
		})
	}
}

func TestCreateChargesTheDeliveriesThatWouldBeStored(t *testing.T) {
	limiter := &fakeLimiter{decision: ports.Decision{Allowed: true}}
	store := &fakeStore{res: ports.CreateResult{Created: 3}}
	h := newHandlerWith(store, 1<<20, limiter)

	// Two valid recipients on two channels and one invalid one: 4 deliveries, not 5.
	recipients := `[
		{"recipient_id":"u_1","channels":{"sms":"+201000000001","email":"a@example.com"}},
		{"recipient_id":"u_2","channels":{"sms":"+201000000002","email":"b@example.com"}},
		` + badRecipient + `]`
	payload := body(recipients, func(m map[string]any) {
		m["content"] = map[string]any{
			"sms":   map[string]any{"text": "hi"},
			"email": map[string]any{"subject": "s", "body": "b"},
		}
	})
	if rec := post(t, h, testAPIKey, payload); rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
	}
	if len(limiter.charges) != 1 || limiter.charges[0] != 4 {
		t.Fatalf("charges = %v, want one charge of 4", limiter.charges)
	}
}

func TestCreateDoesNotChargeWhatItRefusesFirst(t *testing.T) {
	tests := map[string]struct {
		key     string
		payload string
	}{
		"unauthenticated":   {"", body("[" + okRecipient + "]")},
		"malformed":         {testAPIKey, `{`},
		"every row invalid": {testAPIKey, body("[" + badRecipient + "]")},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			limiter := &fakeLimiter{decision: ports.Decision{Allowed: true}}
			post(t, newHandlerWith(&fakeStore{}, 1<<20, limiter), tc.key, tc.payload)
			if len(limiter.charges) != 0 {
				t.Fatalf("charged %v for a request that never had deliveries to store", limiter.charges)
			}
		})
	}
}

func TestCreateWhenTheLimiterFails(t *testing.T) {
	store := &fakeStore{}
	limiter := &fakeLimiter{err: errors.New("boom")}
	rec := post(t, newHandlerWith(store, 1<<20, limiter), testAPIKey, body("["+okRecipient+"]"))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if store.calls != 0 {
		t.Error("a request the limiter could not judge must not be stored")
	}
}

func TestBurstAboveTheLimitIsRefusedThenAdmittedAfterTheWait(t *testing.T) {
	now := testNow
	limiter := ratelimit.NewLocal(domain.BucketPolicy{Rate: 1, Burst: 2}, func() time.Time { return now })
	store := &fakeStore{res: ports.CreateResult{Created: 1}}
	h := newHandlerWith(store, 1<<20, limiter)
	payload := body("[" + okRecipient + "]")

	for i := range 2 {
		if rec := post(t, h, testAPIKey, payload); rec.Code != http.StatusAccepted {
			t.Fatalf("request %d within the burst = %d, want 202", i, rec.Code)
		}
	}
	rec := post(t, h, testAPIKey, payload)
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") != "2" {
		t.Fatalf("request above the burst = %d, Retry-After %q; want 429 and 2", rec.Code, rec.Header().Get("Retry-After"))
	}
	if store.calls != 2 {
		t.Fatalf("store saw %d requests, want the 2 admitted ones", store.calls)
	}
	now = now.Add(2 * time.Second)
	if rec := post(t, h, testAPIKey, payload); rec.Code != http.StatusAccepted {
		t.Fatalf("request after the Retry-After wait = %d, want 202", rec.Code)
	}
}

func TestRetryAfterSeconds(t *testing.T) {
	for wait, want := range map[time.Duration]string{0: "1", time.Nanosecond: "1", time.Second: "1", time.Second + 1: "2", 90 * time.Second: "90"} {
		if got := retryAfterSeconds(wait); got != want {
			t.Errorf("retryAfterSeconds(%v) = %s, want %s", wait, got, want)
		}
	}
}

func TestCreateStatuses(t *testing.T) {
	four := okRecipient + "," + okRecipient + "," + okRecipient + "," + okRecipient
	past := func(m map[string]any) { m["expires_at"] = "2026-10-20T11:00:00Z" }
	tests := []struct {
		name    string
		payload string
		want    int
		code    string
	}{
		{"malformed JSON", `{"idempotency_key":`, 400, "MALFORMED_BODY"},
		{"unknown field", body("["+okRecipient+"]", func(m map[string]any) { m["surprise"] = 1 }), 400, "MALFORMED_BODY"},
		{"trailing data", body("["+okRecipient+"]") + `{}`, 400, "MALFORMED_BODY"},
		{"wrong field type", `{"recipients":"nope"}`, 400, "MALFORMED_BODY"},
		{"too many recipients", body("[" + four + "]"), 413, "TOO_MANY_RECIPIENTS"},
		{"no recipients", body("[]"), 422, "NO_RECIPIENTS"},
		{"unknown type", body("["+okRecipient+"]", func(m map[string]any) { m["type"] = "nope" }), 422, "UNKNOWN_TYPE"},
		{"deadline in the past", body("["+okRecipient+"]", past), 422, "EXPIRED_DEADLINE"},
		{"bad idempotency key", body("["+okRecipient+"]", func(m map[string]any) { m["idempotency_key"] = "x" }), 422, "INVALID_IDEMPOTENCY_KEY"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeStore{}
			rec := post(t, newHandler(store, 1<<20), testAPIKey, tc.payload)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d (%s)", rec.Code, tc.want, rec.Body)
			}
			if got := decode[map[string]string](t, rec)["error"]; got != tc.code {
				t.Errorf("error = %q, want %q", got, tc.code)
			}
			if store.calls != 0 {
				t.Error("a rejected request must not reach the store")
			}
		})
	}
}

func TestCreateBodyCap(t *testing.T) {
	store := &fakeStore{}
	rec := post(t, newHandler(store, 100), testAPIKey, body("["+okRecipient+"]"))
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rec.Code)
	}
	if got := decode[map[string]string](t, rec)["error"]; got != "BODY_TOO_LARGE" {
		t.Errorf("error = %q", got)
	}
}

func TestCreateAllRejected(t *testing.T) {
	store := &fakeStore{}
	rec := post(t, newHandler(store, 1<<20), testAPIKey, body("["+badRecipient+"]"))
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", rec.Code)
	}
	got := decode[createResponse](t, rec)
	if got.Accepted != 0 || len(got.Rejected) != 1 || got.Rejected[0].Reason != requests.ReasonInvalidContact {
		t.Errorf("body = %+v", got)
	}
	if store.calls != 0 {
		t.Error("nothing valid, so the store must not be called")
	}
}

func TestCreateAccepted(t *testing.T) {
	store := &fakeStore{res: ports.CreateResult{Created: 1, Duplicates: 2}}
	// u_1 twice: the repeat is dropped before the store; u_bad is rejected.
	rec := post(t, newHandler(store, 1<<20), testAPIKey, body("["+okRecipient+","+badRecipient+","+okRecipient+"]"))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", rec.Code, rec.Body)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content type = %q", ct)
	}
	got := decode[createResponse](t, rec)
	if got.Accepted != 1 || got.Duplicates != 3 {
		t.Errorf("accepted %d, duplicates %d; want 1 and 3 (2 from the store, 1 repeated in the payload)", got.Accepted, got.Duplicates)
	}
	if len(got.Rejected) != 1 || got.Rejected[0].RecipientID != "u_bad" {
		t.Errorf("rejected = %+v", got.Rejected)
	}
	if store.calls != 1 || len(store.got.Deliveries) != 1 {
		t.Fatalf("store calls = %d, notification = %+v", store.calls, store.got)
	}
	if !store.got.Deliveries[0].ExpiresAt.Equal(testNow.Add(2 * time.Hour)) {
		t.Errorf("expiry = %s, want the type default", store.got.Deliveries[0].ExpiresAt)
	}
}

func TestCreateAcceptedHasEmptyRejectedList(t *testing.T) {
	rec := post(t, newHandler(&fakeStore{res: ports.CreateResult{Created: 1}}, 1<<20), testAPIKey, body("["+okRecipient+"]"))
	if !bytes.Contains(rec.Body.Bytes(), []byte(`"rejected":[]`)) {
		t.Errorf("rejected should be an empty list, not null: %s", rec.Body)
	}
}

func TestCreateStoreFailure(t *testing.T) {
	rec := post(t, newHandler(&fakeStore{err: errors.New("db down")}, 1<<20), testAPIKey, body("["+okRecipient+"]"))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "db down") {
		t.Errorf("the internal error must not leak: %s", rec.Body)
	}
}

func TestCreateOnlyAcceptsPost(t *testing.T) {
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/notifications", nil)
	req.Header.Set("X-API-KEY", testAPIKey)
	rec := httptest.NewRecorder()
	newHandler(&fakeStore{}, 1<<20).ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

func TestHealthStaysOpenWithoutKey(t *testing.T) {
	if got := status(t, newHandler(&fakeStore{}, 1<<20), "/healthz"); got != http.StatusOK {
		t.Errorf("healthz = %d, want 200 without an API key", got)
	}
}
