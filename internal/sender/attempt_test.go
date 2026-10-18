package sender

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/mansonxasthur/notifier/internal/adapters/postgres"
	"github.com/mansonxasthur/notifier/internal/adapters/postgres/pgtest"
	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

var quiet = slog.New(slog.DiscardHandler)

// fakeProvider records its calls and answers with err.
type fakeProvider struct {
	mu    sync.Mutex
	calls []ports.SendRequest
	err   error
}

func (f *fakeProvider) Name() string { return "sms-a" }

func (f *fakeProvider) Send(_ context.Context, r ports.SendRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, r)
	return f.err
}

type providers map[string]ports.Provider

func (p providers) Provider(name string) (ports.Provider, bool) { v, ok := p[name]; return v, ok }

type fixture struct {
	db      *postgres.Pool
	inbox   *postgres.InboxStore
	id      int64
	claim   Claimed
	attempt *Attempt
	prov    *fakeProvider
	now     time.Time
}

// newFixture seeds one SMS delivery, claims it as a sender would, and returns an Attempt
// with a fixed clock and a jitter of 0.5 (which leaves the backoff delay unchanged).
func newFixture(t *testing.T, sendErr error) *fixture {
	t.Helper()
	ctx := context.Background()
	db := postgres.New(pgtest.NewMigratedDB(t))
	if err := db.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Stop(ctx) })

	n := &domain.Notification{
		IdempotencyKey: uuid.NewString(), Type: "match_alert",
		Content: map[domain.Channel]domain.Content{domain.ChannelSMS: {Text: "مرحبا"}},
		Deliveries: []domain.Delivery{{
			RecipientID: "u_1", Channel: domain.ChannelSMS, Contact: "+201000000000",
			ExpiresAt: time.Now().Add(time.Hour),
		}},
	}
	if _, err := postgres.NewNotificationStore(db, 5000).Create(ctx, n); err != nil {
		t.Fatal(err)
	}
	var id int64
	if err := db.Pool().QueryRow(ctx, `SELECT id FROM deliveries`).Scan(&id); err != nil {
		t.Fatal(err)
	}

	inbox := postgres.NewInboxStore(db)
	c, err := inbox.Claim(ctx, id, "sms-a", 5)
	if err != nil || c.Result != domain.InboxClaimWon {
		t.Fatalf("claim = %+v, %v", c, err)
	}

	prov := &fakeProvider{err: sendErr}
	f := &fixture{
		db: db, inbox: inbox, id: id, prov: prov,
		claim: Claimed{DeliveryID: id, Provider: "sms-a", ClaimCount: c.ClaimCount},
		now:   time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC),
	}
	backoff := map[string]domain.Backoff{"sms-a": {Initial: 2 * time.Second, Max: time.Minute, Multiplier: 2, Jitter: 0.2}}
	f.attempt = NewAttempt(postgres.NewDeliveryStore(db), inbox, providers{"sms-a": prov}, backoff, quiet)
	f.attempt.now = func() time.Time { return f.now }
	f.attempt.jitter = func() float64 { return 0.5 }
	return f
}

func (f *fixture) text(t *testing.T, query string) string {
	t.Helper()
	var s *string
	if err := f.db.Pool().QueryRow(context.Background(), query, f.id).Scan(&s); err != nil {
		t.Fatal(err)
	}
	if s == nil {
		return ""
	}
	return *s
}

func (f *fixture) inboxState(t *testing.T) string {
	return f.text(t, `SELECT state FROM inbox WHERE delivery_id = $1`)
}

func (f *fixture) deliveryState(t *testing.T) string {
	return f.text(t, `SELECT state FROM deliveries WHERE id = $1`)
}

func TestAttemptAccepted(t *testing.T) {
	f := newFixture(t, nil)
	if err := f.attempt.Send(context.Background(), f.claim); err != nil {
		t.Fatal(err)
	}
	if got := f.inboxState(t); got != "DONE" {
		t.Errorf("inbox = %s, want DONE", got)
	}
	if got := f.deliveryState(t); got != "ACCEPTED" {
		t.Errorf("delivery = %s, want ACCEPTED", got)
	}

	if len(f.prov.calls) != 1 {
		t.Fatalf("provider called %d times, want once", len(f.prov.calls))
	}
	got := f.prov.calls[0]
	if got.DeliveryID != f.id || got.Channel != domain.ChannelSMS || got.Contact != "+201000000000" || got.Content.Text != "مرحبا" {
		t.Errorf("provider got %+v: want the delivery_id as reference key and the content intact", got)
	}
}

func TestAttemptMessageLevelFailureEndsTheDelivery(t *testing.T) {
	f := newFixture(t, &ports.SendError{Class: domain.FailureMessage, Err: errors.New("status 400")})
	if err := f.attempt.Send(context.Background(), f.claim); err != nil {
		t.Fatal(err)
	}
	if got := f.inboxState(t); got != "DEAD" {
		t.Errorf("inbox = %s, want DEAD", got)
	}
	if got := f.deliveryState(t); got != "FAILED" {
		t.Errorf("delivery = %s, want FAILED", got)
	}
	if got := f.text(t, `SELECT failure_reason FROM deliveries WHERE id = $1`); got != "PROVIDER_REJECTED" {
		t.Errorf("failure_reason = %q, want PROVIDER_REJECTED", got)
	}
	if got := f.text(t, `SELECT failure_class FROM inbox WHERE delivery_id = $1`); got != "message" {
		t.Errorf("failure_class = %q, want message", got)
	}
}

func TestAttemptProviderFailureAndTimeoutAreParked(t *testing.T) {
	for _, class := range []domain.FailureClass{domain.FailureProvider, domain.FailureTimeout} {
		t.Run(string(class), func(t *testing.T) {
			f := newFixture(t, &ports.SendError{Class: class, Err: errors.New("boom")})
			if err := f.attempt.Send(context.Background(), f.claim); err != nil {
				t.Fatal(err)
			}
			if got := f.inboxState(t); got != "WAITING_RETRY" {
				t.Errorf("inbox = %s, want WAITING_RETRY", got)
			}
			if got := f.deliveryState(t); got != "PENDING" {
				t.Errorf("delivery = %s, want PENDING: nothing is final yet", got)
			}
			if got := f.text(t, `SELECT failure_class FROM inbox WHERE delivery_id = $1`); got != string(class) {
				t.Errorf("failure_class = %q, want %q", got, class)
			}

			var retries int
			var due time.Time
			if err := f.db.Pool().QueryRow(context.Background(),
				`SELECT retries, next_attempt_at FROM inbox WHERE delivery_id = $1`, f.id).Scan(&retries, &due); err != nil {
				t.Fatal(err)
			}
			if want := f.now.Add(2 * time.Second); retries != 1 || !due.Equal(want) {
				t.Errorf("retries = %d, next_attempt_at = %v; want 1 and %v (first backoff step)", retries, due, want)
			}
		})
	}
}

func TestAttemptCallerCancellationLeavesTheRowForTheJanitor(t *testing.T) {
	f := newFixture(t, context.Canceled)
	if err := f.attempt.Send(context.Background(), f.claim); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want the cancellation", err)
	}
	if got := f.inboxState(t); got != "PROCESSING" {
		t.Errorf("inbox = %s, want PROCESSING so the janitor can recover it", got)
	}
	if got := f.deliveryState(t); got != "PENDING" {
		t.Errorf("delivery = %s, want PENDING", got)
	}
}

func TestAttemptSkipsADeliveryAWebhookAlreadyFinished(t *testing.T) {
	f := newFixture(t, nil)
	if _, err := f.db.Pool().Exec(context.Background(), `UPDATE deliveries SET state = 'DELIVERED'`); err != nil {
		t.Fatal(err)
	}
	if err := f.attempt.Send(context.Background(), f.claim); err != nil {
		t.Fatal(err)
	}
	if len(f.prov.calls) != 0 {
		t.Errorf("provider called %d times, want none: sending would only duplicate", len(f.prov.calls))
	}
	if got := f.inboxState(t); got != "DONE" {
		t.Errorf("inbox = %s, want DONE", got)
	}
	if got := f.deliveryState(t); got != "DELIVERED" {
		t.Errorf("delivery = %s, want DELIVERED to stay", got)
	}
}

func TestAttemptStaleClaimChangesNothing(t *testing.T) {
	f := newFixture(t, &ports.SendError{Class: domain.FailureProvider, Err: errors.New("boom")})
	f.claim.ClaimCount++ // the row was reclaimed by another sender since this one claimed it
	if err := f.attempt.Send(context.Background(), f.claim); err != nil {
		t.Fatal(err)
	}
	if got := f.inboxState(t); got != "PROCESSING" {
		t.Errorf("inbox = %s, want the newer claim's PROCESSING left alone", got)
	}
}

func TestAttemptUnknownProvider(t *testing.T) {
	f := newFixture(t, nil)
	f.claim.Provider = "nope"
	if err := f.attempt.Send(context.Background(), f.claim); err == nil {
		t.Error("want an error for an unknown provider")
	}
}

func TestAttemptUnknownDelivery(t *testing.T) {
	f := newFixture(t, nil)
	f.claim.DeliveryID += 1000
	if err := f.attempt.Send(context.Background(), f.claim); !errors.Is(err, ports.ErrDeliveryNotFound) {
		t.Errorf("err = %v, want ErrDeliveryNotFound", err)
	}
}

func TestAttemptBackoffGrowsWithRetriesNotClaims(t *testing.T) {
	f := newFixture(t, &ports.SendError{Class: domain.FailureProvider, Err: errors.New("boom")})
	f.claim.Retries = 2 // two retries already used; claim_count is whatever the janitor made it
	if err := f.attempt.Send(context.Background(), f.claim); err != nil {
		t.Fatal(err)
	}
	var due time.Time
	if err := f.db.Pool().QueryRow(context.Background(),
		`SELECT next_attempt_at FROM inbox WHERE delivery_id = $1`, f.id).Scan(&due); err != nil {
		t.Fatal(err)
	}
	if want := f.now.Add(8 * time.Second); !due.Equal(want) { // 2s * 2^2
		t.Errorf("next_attempt_at = %v, want %v (third backoff step)", due, want)
	}
}

func TestAttemptRecordsTheOutcomeAfterShutdownStarts(t *testing.T) {
	f := newFixture(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	f.attempt.providers = providers{"sms-a": cancelOnSend{fakeProvider: f.prov, cancel: cancel}}

	if err := f.attempt.Send(ctx, f.claim); err != nil {
		t.Fatalf("Send = %v: the accepted message must still be recorded", err)
	}
	if got := f.inboxState(t); got != "DONE" {
		t.Errorf("inbox = %s, want DONE: a lost record would make the janitor resend", got)
	}
}

// cancelOnSend cancels the caller's context as the provider answers, like a SIGTERM that
// lands between the provider call and the inbox write.
type cancelOnSend struct {
	*fakeProvider
	cancel context.CancelFunc
}

func (c cancelOnSend) Send(ctx context.Context, r ports.SendRequest) error {
	err := c.fakeProvider.Send(ctx, r)
	c.cancel()
	return err
}

func TestAttemptRefusesAProviderWithoutBackoff(t *testing.T) {
	f := newFixture(t, nil)
	f.attempt.backoff = nil
	if err := f.attempt.Send(context.Background(), f.claim); err == nil {
		t.Error("want an error when the provider has no backoff policy")
	}
	if len(f.prov.calls) != 0 {
		t.Error("the provider must not be called without a retry policy")
	}
}
