package e2e

import (
	"context"
	"net/http"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/mansonxasthur/notifier/internal/config"
	"github.com/mansonxasthur/notifier/internal/domain"
)

// succeedOnly makes the simulator accept every call, so a test about limits sees one call per
// delivery and no retries.
func succeedOnly(cfg *config.Config) {
	cfg.SimProvider.Mode = string(domain.ScenarioSucceed)
	cfg.SimProvider.Weights = nil
}

func (s *stack) count(t *testing.T, query string) int {
	t.Helper()
	var n int
	if err := s.db.Pool().QueryRow(context.Background(), query).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// A burst above the global limit is refused whole with 429 and Retry-After and leaves nothing
// behind, and the same request goes through once the advertised wait has passed.
func TestBurstAboveTheGlobalLimitIsRefusedThenAdmitted(t *testing.T) {
	const deliveries = recipients * 2
	s := newStack(t, succeedOnly, func(cfg *config.Config) {
		// Room for one request and a quarter of the next; 50 deliveries a second refill.
		cfg.Limits.Global = config.RateLimit{Rate: 50, Burst: deliveries + deliveries/4}
	})

	if got := s.submit(t); got != deliveries {
		t.Fatalf("first request accepted %d, want %d", got, deliveries)
	}

	status, header, body := s.post(t, "11111111-2222-4333-8444-555555555555")
	if status != http.StatusTooManyRequests {
		t.Fatalf("second request = %d %s, want 429", status, body)
	}
	wait, err := strconv.Atoi(header.Get("Retry-After"))
	if err != nil || wait < 1 || wait > 3 {
		t.Fatalf("Retry-After = %q, want a few seconds", header.Get("Retry-After"))
	}
	if got := s.count(t, `SELECT count(*) FROM deliveries`); got != deliveries {
		t.Fatalf("deliveries = %d after the refusal, want the %d of the first request only", got, deliveries)
	}
	if got := s.count(t, `SELECT count(*) FROM notifications`); got != 1 {
		t.Fatalf("notifications = %d after the refusal, want 1", got)
	}

	time.Sleep(time.Duration(wait) * time.Second)
	status, _, body = s.post(t, "11111111-2222-4333-8444-555555555555")
	if status != http.StatusAccepted {
		t.Fatalf("retry after Retry-After = %d %s, want 202", status, body)
	}
	if got := s.count(t, `SELECT count(*) FROM deliveries`); got != 2*deliveries {
		t.Fatalf("deliveries = %d after the retry, want %d", got, 2*deliveries)
	}
}

// A spike that the global limit lets in drains through the sender no faster than each
// provider's rate, and every delivery still ends DELIVERED with one call and no retry spent.
func TestSpikeDrainsAtTheProviderRate(t *testing.T) {
	const (
		rate  = 20.0
		burst = 5
	)
	s := newStack(t, succeedOnly, func(cfg *config.Config) {
		for _, cc := range cfg.Channels {
			for i := range cc.Providers {
				cc.Providers[i].RateLimit = config.RateLimit{Rate: rate, Burst: burst}
			}
		}
	})
	want := s.submit(t)

	deadline := time.Now().Add(60 * time.Second)
	for s.count(t, `SELECT count(*) FROM deliveries WHERE state = 'DELIVERED'`) != want {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d deliveries reached DELIVERED", s.count(t, `SELECT count(*) FROM deliveries WHERE state = 'DELIVERED'`), want)
		}
		time.Sleep(100 * time.Millisecond)
	}

	byProvider := map[string][]time.Time{}
	for _, e := range s.entries(t) {
		if e.Sends != 1 {
			t.Errorf("delivery %d reached %s %d times, want once: a throttled send must not be retried", e.DeliveryID, e.Provider, e.Sends)
		}
		byProvider[e.Provider] = append(byProvider[e.Provider], e.ReceivedAt)
	}
	if got := s.count(t, `SELECT count(*) FROM inbox WHERE retries > 0`); got != 0 {
		t.Errorf("%d inbox rows spent a retry; throttling must not", got)
	}
	if got := s.count(t, `SELECT count(*) FROM inbox WHERE deferrals > 0`); got == 0 {
		t.Error("no row was ever deferred, so the spike never met the limit")
	}

	if len(byProvider) != 2 {
		t.Fatalf("calls reached %d providers, want 2: %v", len(byProvider), byProvider)
	}
	for name, times := range byProvider {
		slices.SortFunc(times, func(a, b time.Time) int { return a.Compare(b) })
		// In any window, a bucket allows at most its burst plus what it refilled. Two calls of
		// slack cover the gap between a sender taking a token and the simulator receiving the call.
		for i := range times {
			for j := i; j < len(times); j++ {
				allowed := burst + rate*times[j].Sub(times[i]).Seconds() + 2
				if float64(j-i+1) > allowed {
					t.Fatalf("%s took %d calls in %v, more than its bucket allows (%.1f)", name, j-i+1, times[j].Sub(times[i]), allowed)
				}
			}
		}
		// recipients calls at 20 a second after a burst of 5 cannot finish in under two seconds.
		if span := times[len(times)-1].Sub(times[0]); span < 2*time.Second {
			t.Errorf("%s drained %d calls in %v; the limit did not slow it", name, len(times), span)
		}
	}
}
