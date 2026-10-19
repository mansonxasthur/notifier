package sender

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
)

var quiet = slog.New(slog.DiscardHandler)

type recordedSends struct {
	mu   sync.Mutex
	sent []Claimed
}

func (r *recordedSends) send(_ context.Context, c Claimed) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sent = append(r.sent, c)
	return nil
}

func (r *recordedSends) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sent)
}

// park puts the fixture's claimed row in WAITING_RETRY, due at the given time.
func (f *fixture) park(t *testing.T, due time.Time) {
	t.Helper()
	if ok, err := f.inbox.Park(context.Background(), f.id, f.claim.ClaimCount, domain.FailureProvider, due); err != nil || !ok {
		t.Fatalf("park = %v, %v", ok, err)
	}
}

func TestScanSendsOnlyDueRetries(t *testing.T) {
	f := newFixture(t, nil)
	rec := &recordedSends{}
	scan := NewScan(f.inbox, rec.send, time.Hour, 10, domain.Claims(5), quiet)

	f.park(t, time.Now().Add(time.Hour))
	if n, err := scan.Once(context.Background(), domain.ChannelSMS); err != nil || n != 0 || rec.count() != 0 {
		t.Fatalf("scan before due = %d, %v, sent %d; want nothing", n, err, rec.count())
	}

	if _, err := f.db.Pool().Exec(context.Background(), `UPDATE inbox SET next_attempt_at = now() - interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	if n, err := scan.Once(context.Background(), domain.ChannelSMS); err != nil || n != 1 {
		t.Fatalf("scan when due = %d, %v; want 1", n, err)
	}
	want := Claimed{DeliveryID: f.id, Provider: "sms-a", ClaimCount: f.claim.ClaimCount + 1, Retries: 1}
	if rec.count() != 1 || rec.sent[0] != want {
		t.Errorf("sent = %+v, want %+v", rec.sent, want)
	}
	if n, _ := scan.Once(context.Background(), domain.ChannelSMS); n != 0 {
		t.Errorf("a claimed retry was scanned again (%d)", n)
	}
}

func TestScanRunPicksUpRetriesAndStopsWithItsContext(t *testing.T) {
	f := newFixture(t, nil)
	rec := &recordedSends{}
	scan := NewScan(f.inbox, rec.send, 10*time.Millisecond, 10, domain.Claims(5), quiet)
	f.park(t, time.Now().Add(-time.Second))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { scan.Run(ctx); close(done) }()

	deadline := time.Now().Add(10 * time.Second)
	for rec.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if rec.count() != 1 {
		t.Fatalf("sent %d retries, want 1", rec.count())
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after its context was cancelled")
	}
}

// TestScanRunServesEveryChannelEvenWhenOneHasABacklog gives SMS far more due rows than a batch
// holds and email one row: the email row must still go out on the first scans.
func TestScanRunServesEveryChannelEvenWhenOneHasABacklog(t *testing.T) {
	f := newFixture(t, nil)
	f.seed(t, 20) // 21 SMS deliveries in all
	ctx := context.Background()
	rows, err := f.db.Pool().Query(ctx, `SELECT id FROM deliveries ORDER BY id`)
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
	emailID := ids[len(ids)-1]
	if _, err := f.db.Pool().Exec(ctx, `UPDATE deliveries SET channel = 'email' WHERE id = $1`, emailID); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if id == f.id {
			continue
		}
		if _, err := f.inbox.Claim(ctx, id, "any", domain.Claims(20)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.db.Pool().Exec(ctx, `UPDATE inbox SET state = 'WAITING_RETRY', next_attempt_at = now() - interval '1 second'`); err != nil {
		t.Fatal(err)
	}

	// A send that holds every SMS row, so the SMS loop cannot finish its first batch.
	release := make(chan struct{})
	var mu sync.Mutex
	sent := map[int64]bool{}
	send := func(_ context.Context, c Claimed) error {
		if c.DeliveryID != emailID {
			<-release
		}
		mu.Lock()
		sent[c.DeliveryID] = true
		mu.Unlock()
		return nil
	}
	scan := NewScan(f.inbox, send, 10*time.Millisecond, 5, domain.Claims(20), quiet)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { scan.Run(runCtx); close(done) }()
	t.Cleanup(func() { cancel(); close(release); <-done })

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		ok := sent[emailID]
		mu.Unlock()
		if ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the email retry waited behind the SMS backlog")
}

func TestScanOnceClaimsOnlyItsChannel(t *testing.T) {
	f := newFixture(t, nil)
	rec := &recordedSends{}
	scan := NewScan(f.inbox, rec.send, time.Hour, 10, domain.Claims(5), quiet)
	f.park(t, time.Now().Add(-time.Second))

	if n, err := scan.Once(context.Background(), domain.ChannelEmail); err != nil || n != 0 || rec.count() != 0 {
		t.Fatalf("email scan = %d, %v, sent %d; want nothing: the row is SMS", n, err, rec.count())
	}
	if n, err := scan.Once(context.Background(), domain.ChannelSMS); err != nil || n != 1 {
		t.Fatalf("sms scan = %d, %v; want the row", n, err)
	}
}
