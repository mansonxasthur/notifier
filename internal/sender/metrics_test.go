package sender

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

type accepted struct {
	ch    domain.Channel
	first bool
	d     time.Duration
}

// recorder keeps what Attempt reports.
type recorder struct {
	ports.NopRecorder
	mu  sync.Mutex
	got []accepted
}

func (r *recorder) AcceptLatency(ch domain.Channel, first bool, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got = append(r.got, accepted{ch, first, d})
}

// acceptedAfter puts the fixture's clock the given time after the delivery was created.
func (f *fixture) acceptedAfter(t *testing.T, d time.Duration) *recorder {
	t.Helper()
	var created time.Time
	if err := f.db.Pool().QueryRow(context.Background(), `SELECT created_at FROM deliveries WHERE id = $1`, f.id).Scan(&created); err != nil {
		t.Fatal(err)
	}
	f.now = created.Add(d)
	rec := &recorder{}
	f.attempt.WithRecorder(rec)
	return rec
}

func TestAcceptLatencyIsReportedWithItsAttempt(t *testing.T) {
	tests := []struct {
		name      string
		retries   int
		provider  string
		wantFirst bool
	}{
		{"first try on the primary", 0, "sms-a", true},
		{"a retry on the primary", 1, "sms-a", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, nil)
			rec := f.acceptedAfter(t, 1500*time.Millisecond)
			f.claim.Retries = tc.retries
			f.send(t)
			want := []accepted{{domain.ChannelSMS, tc.wantFirst, 1500 * time.Millisecond}}
			if len(rec.got) != 1 || rec.got[0] != want[0] {
				t.Fatalf("reported %+v, want %+v", rec.got, want)
			}
		})
	}
}

func TestAcceptLatencyIsNotReportedForAFailureOrAStaleClaim(t *testing.T) {
	t.Run("a failed send", func(t *testing.T) {
		f := newFixture(t, &ports.SendError{Class: domain.FailureMessage, Err: errors.New("status 400")})
		rec := f.acceptedAfter(t, time.Second)
		f.send(t)
		if len(rec.got) != 0 {
			t.Errorf("reported %+v for a delivery that was not accepted", rec.got)
		}
	})
	t.Run("a stale claim", func(t *testing.T) {
		f := newFixture(t, nil)
		rec := f.acceptedAfter(t, time.Second)
		f.claim.ClaimCount++
		f.send(t)
		if len(rec.got) != 0 {
			t.Errorf("reported %+v for a result that was discarded", rec.got)
		}
	})
}

func TestBreakerStates(t *testing.T) {
	clk := &clock{t: time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)}
	b := NewBreakers(domain.BreakerPolicy{FailureThreshold: 1, OpenFor: 10 * time.Second}, clk.now)
	tk, _ := b.Allow("sms-a")
	b.Failure("sms-a", tk)
	tk, _ = b.Allow("sms-b")
	b.Success("sms-b", tk)

	got := b.States()
	if got["sms-a"] != 2 || got["sms-b"] != 0 {
		t.Fatalf("states = %v, want sms-a open (2) and sms-b closed (0)", got)
	}
	clk.t = clk.t.Add(10 * time.Second)
	if got := b.States()["sms-a"]; got != 1 {
		t.Errorf("after the open period sms-a = %d, want half-open (1) before any call moves it", got)
	}
}
