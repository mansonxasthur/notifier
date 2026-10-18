package sender

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/mansonxasthur/notifier/internal/adapters/postgres"
	"github.com/mansonxasthur/notifier/internal/domain"
)

func TestProviderLimitsAreIndependentAndRefill(t *testing.T) {
	c := newClock()
	l := NewProviderLimits(map[string]domain.BucketPolicy{
		"a": {Rate: 2, Burst: 2},
		"b": {Rate: 2, Burst: 1},
	}, c.now)

	for range 2 {
		if ok, _ := l.Take("a"); !ok {
			t.Fatal("refused inside a's burst")
		}
	}
	ok, wait := l.Take("a")
	if ok || wait < 500*time.Millisecond || wait > 501*time.Millisecond {
		t.Fatalf("empty bucket = %v, %v; want refused with a wait of about 500ms", ok, wait)
	}
	if ok, _ := l.Take("b"); !ok {
		t.Fatal("a's limit affected b")
	}
	c.advance(wait)
	if ok, _ := l.Take("a"); !ok {
		t.Fatal("refused after the returned wait")
	}
}

func TestProviderLimitsLeaveUnlistedProvidersAlone(t *testing.T) {
	var none *ProviderLimits
	if ok, _ := none.Take("a"); !ok {
		t.Fatal("a nil ProviderLimits refused a call")
	}
	l := NewProviderLimits(map[string]domain.BucketPolicy{"a": {Rate: 1, Burst: 1}}, newClock().now)
	for range 10 {
		if ok, _ := l.Take("unlisted"); !ok {
			t.Fatal("a provider without a bucket was limited")
		}
	}
}

// limitedFixture is a fixture whose Attempt has a bucket for sms-a.
func limitedFixture(t *testing.T, policy domain.BucketPolicy) *fixture {
	t.Helper()
	f := newFixture(t, nil)
	f.attempt.limits = NewProviderLimits(map[string]domain.BucketPolicy{"sms-a": policy}, f.clock.now)
	return f
}

// seed adds n more SMS deliveries, each with a distinct recipient.
func (f *fixture) seed(t *testing.T, n int) {
	t.Helper()
	notification := &domain.Notification{
		IdempotencyKey: uuid.NewString(), Type: "match_alert",
		Content: map[domain.Channel]domain.Content{domain.ChannelSMS: {Text: "hi"}},
	}
	for i := range n {
		notification.Deliveries = append(notification.Deliveries, domain.Delivery{
			RecipientID: fmt.Sprintf("spike_%03d", i), Channel: domain.ChannelSMS, Contact: "+201000000000",
			ExpiresAt: time.Now().Add(time.Hour),
		})
	}
	if _, err := postgres.NewNotificationStore(f.db, 5000).Create(context.Background(), notification); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) spend(t *testing.T) {
	t.Helper()
	// The bucket's only token goes to a different delivery's call.
	if ok, _ := f.attempt.limits.Take("sms-a"); !ok {
		t.Fatal("bucket was already empty")
	}
}

func TestThrottledSendIsDeferredNotFailed(t *testing.T) {
	f := limitedFixture(t, domain.BucketPolicy{Rate: 1, Burst: 1})
	f.spend(t)

	if err := f.attempt.Send(context.Background(), f.claim); err != nil {
		t.Fatal(err)
	}
	if len(f.prov.calls) != 0 {
		t.Fatalf("provider called %d times while throttled", len(f.prov.calls))
	}
	if got := f.inboxState(t); got != "WAITING_RETRY" {
		t.Errorf("inbox = %s, want WAITING_RETRY", got)
	}
	if got := f.deliveryState(t); got != "PENDING" {
		t.Errorf("delivery = %s, want PENDING", got)
	}

	var retries, claims, deferrals int
	var class *string
	var due time.Time
	if err := f.db.Pool().QueryRow(context.Background(),
		`SELECT retries, claim_count, deferrals, failure_class, next_attempt_at FROM inbox WHERE delivery_id = $1`, f.id).
		Scan(&retries, &claims, &deferrals, &class, &due); err != nil {
		t.Fatal(err)
	}
	if retries != 0 || class != nil || deferrals != 1 || claims != f.claim.ClaimCount {
		t.Errorf("retries %d, class %v, deferrals %d, claims %d; want no retry or failure spent and one deferral", retries, class, deferrals, claims)
	}
	// A token a second away, plus the jitter of 0.5 over the spread.
	if want := f.now.Add(time.Second + throttleSpread/2); due.Sub(want).Abs() > 10*time.Millisecond {
		t.Errorf("next_attempt_at = %v, want about %v", due, want)
	}
	if f.logs.count(slog.LevelWarn) != 0 {
		t.Error("a throttled send was logged as a warning")
	}
}

func TestThrottleNeverWaitsPastTheDeadline(t *testing.T) {
	f := limitedFixture(t, domain.BucketPolicy{Rate: 0.1, Burst: 1}) // a token every ten seconds
	f.spend(t)
	deadline := f.now.Add(3 * time.Second)
	if _, err := f.db.Pool().Exec(context.Background(), `UPDATE deliveries SET expires_at = $1`, deadline); err != nil {
		t.Fatal(err)
	}
	if err := f.attempt.Send(context.Background(), f.claim); err != nil {
		t.Fatal(err)
	}
	var due time.Time
	if err := f.db.Pool().QueryRow(context.Background(), `SELECT next_attempt_at FROM inbox WHERE delivery_id = $1`, f.id).Scan(&due); err != nil {
		t.Fatal(err)
	}
	if !due.Equal(deadline) {
		t.Fatalf("next_attempt_at = %v, want the deadline %v", due, deadline)
	}
}

func TestAnOpenBreakerWastesNoToken(t *testing.T) {
	f := limitedFixture(t, domain.BucketPolicy{Rate: 0.001, Burst: 1})
	tripBreaker(f.breaker, "sms-a")
	if err := f.attempt.Send(context.Background(), f.claim); err != nil {
		t.Fatal(err)
	}
	if got := f.text(t, `SELECT provider FROM inbox WHERE delivery_id = $1`); got != "sms-b" {
		t.Fatalf("provider = %s, want the row switched to sms-b by the open breaker", got)
	}
	if ok, _ := f.attempt.limits.Take("sms-a"); !ok {
		t.Fatal("the open breaker's refusal spent sms-a's only token")
	}
}

func TestThrottleDoesNotTouchTheBreaker(t *testing.T) {
	f := limitedFixture(t, domain.BucketPolicy{Rate: 1, Burst: 1})
	f.spend(t)
	// Put the breaker in its half-open state, with a probe available.
	tripBreaker(f.breaker, "sms-a")
	f.clock.advance(10 * time.Second)
	f.attempt.limits = NewProviderLimits(map[string]domain.BucketPolicy{"sms-a": {Rate: 1, Burst: 1}}, f.clock.now)
	f.spend(t)

	if err := f.attempt.Send(context.Background(), f.claim); err != nil {
		t.Fatal(err)
	}
	if !f.breaker.can("sms-a") {
		t.Fatal("a throttled send took the half-open probe slot")
	}
}

func TestExpiredDeliveryIsFailedNotDeferred(t *testing.T) {
	f := limitedFixture(t, domain.BucketPolicy{Rate: 1, Burst: 1})
	f.spend(t)
	if _, err := f.db.Pool().Exec(context.Background(), `UPDATE deliveries SET expires_at = $1`, f.now.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := f.attempt.Send(context.Background(), f.claim); err != nil {
		t.Fatal(err)
	}
	if got := f.deliveryState(t); got != "FAILED" {
		t.Errorf("delivery = %s, want FAILED: an expired delivery is not worth waiting for a token", got)
	}
}

func TestDeferralsDoNotCountTowardsTheClaimCap(t *testing.T) {
	f := limitedFixture(t, domain.BucketPolicy{Rate: 1, Burst: 1})
	ctx := context.Background()
	const maxClaims = 3
	claim := f.claim
	// Far more deferrals than the cap allows claims.
	for i := range 3 * maxClaims {
		f.spend(t)
		if err := f.attempt.Send(ctx, claim); err != nil {
			t.Fatal(err)
		}
		if _, err := f.db.Pool().Exec(ctx, `UPDATE inbox SET next_attempt_at = now() - interval '1 second'`); err != nil {
			t.Fatal(err)
		}
		due, err := f.inbox.ClaimDue(ctx, 10, maxClaims)
		if err != nil || len(due) != 1 {
			t.Fatalf("deferral %d: ClaimDue = %v, %v; want the row claimable again", i, due, err)
		}
		claim = Claimed{DeliveryID: due[0].DeliveryID, Provider: due[0].Provider, ClaimCount: due[0].ClaimCount, Retries: due[0].Retries}
		f.clock.advance(time.Second) // the bucket refills while the row waits, then is drained again by spend
	}
	if got := f.deliveryState(t); got != "PENDING" {
		t.Fatalf("delivery = %s after %d deferrals, want PENDING", got, 3*maxClaims)
	}
}

// TestSpikeDrainsWithoutExceedingTheProviderRate sends a spike through the retry scan against a
// bucket of 10 calls a second and a burst of 5, moving the clock forward in small steps. At no
// point may the provider have been called more often than the bucket allows, and the whole
// spike must eventually go out.
func TestSpikeDrainsWithoutExceedingTheProviderRate(t *testing.T) {
	const (
		spike = 40
		rate  = 10.0
		burst = 5
	)
	ctx := context.Background()
	f := limitedFixture(t, domain.BucketPolicy{Rate: rate, Burst: burst})
	f.seed(t, spike-1)
	if _, err := f.db.Pool().Exec(ctx, `UPDATE deliveries SET expires_at = now() + interval '1 hour'`); err != nil {
		t.Fatal(err)
	}
	// Every delivery waits in the inbox, already due, as a spike of retries would.
	rows, err := f.db.Pool().Query(ctx, `SELECT id FROM deliveries`)
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		if id == f.id {
			continue
		}
		if _, err := f.inbox.Claim(ctx, id, "sms-a", 100); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.db.Pool().Exec(ctx, `UPDATE inbox SET state = 'WAITING_RETRY', next_attempt_at = now() - interval '1 second', claim_count = 1`); err != nil {
		t.Fatal(err)
	}

	scan := NewScan(f.inbox, f.attempt.Send, time.Hour, 10, 100, quiet)
	start := f.clock.now()
	for step := 0; step < 400 && len(f.prov.calls) < spike; step++ {
		if _, err := scan.Once(ctx); err != nil {
			t.Fatal(err)
		}
		// Make every deferred row due, as the real clock would after its wait.
		if _, err := f.db.Pool().Exec(ctx, `UPDATE inbox SET next_attempt_at = now() - interval '1 second' WHERE state = 'WAITING_RETRY'`); err != nil {
			t.Fatal(err)
		}
		elapsed := f.clock.now().Sub(start).Seconds()
		if limit := burst + rate*elapsed; float64(len(f.prov.calls)) > limit+1e-9 {
			t.Fatalf("after %.2fs the provider took %d calls, more than the bucket allows (%.1f)", elapsed, len(f.prov.calls), limit)
		}
		f.clock.advance(100 * time.Millisecond)
	}
	if len(f.prov.calls) != spike {
		t.Fatalf("provider took %d of %d calls: the spike did not drain", len(f.prov.calls), spike)
	}
}

func newClock() *clock { return &clock{t: time.Unix(1_000_000, 0)} }
