package sender

import (
	"testing"
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestBreakers() (*Breakers, *clock) {
	c := &clock{t: time.Unix(1_000_000, 0)}
	return NewBreakers(domain.BreakerPolicy{FailureThreshold: 3, OpenFor: 10 * time.Second}, c.now), c
}

// can reports whether a call would be allowed, taking the probe slot if it is the probe.
func (b *Breakers) can(provider string) bool {
	_, ok := b.Allow(provider)
	return ok
}

// tripBreaker fails enough calls in a row to open the provider's breaker.
func tripBreaker(b *Breakers, provider string) {
	for range b.policy.FailureThreshold {
		t, _ := b.Allow(provider)
		b.Failure(provider, t)
	}
}

func TestBreakerOpensAfterConsecutiveFailures(t *testing.T) {
	b, _ := newTestBreakers()
	for i := 0; i < 2; i++ {
		tk, ok := b.Allow("a")
		if !ok {
			t.Fatalf("call %d refused while closed", i)
		}
		if got := b.Failure("a", tk); got != NoChange {
			t.Fatalf("failure %d: transition %v, want NoChange", i, got)
		}
	}
	// A success resets the count, so two more failures still do not open it.
	tk, _ := b.Allow("a")
	b.Success("a", tk)
	tk, _ = b.Allow("a")
	b.Failure("a", tk)
	tk, _ = b.Allow("a")
	if got := b.Failure("a", tk); got != NoChange || !b.can("a") {
		t.Fatalf("breaker opened on non-consecutive failures")
	}
	tk, _ = b.Allow("a")
	if got := b.Failure("a", tk); got != Opened {
		t.Fatalf("third consecutive failure: transition %v, want Opened", got)
	}
	if b.can("a") {
		t.Fatal("open breaker allowed a call")
	}
}

func TestBreakersAreIndependentPerProvider(t *testing.T) {
	b, _ := newTestBreakers()
	tripBreaker(b, "a")
	if b.can("a") || !b.can("b") {
		t.Fatal("one provider's breaker affected another")
	}
}

func TestBreakerHalfOpenAdmitsOneProbe(t *testing.T) {
	b, c := newTestBreakers()
	tripBreaker(b, "a")
	c.advance(9 * time.Second)
	if b.can("a") {
		t.Fatal("allowed before the open period ended")
	}
	c.advance(time.Second)
	probe, ok := b.Allow("a")
	if !ok || !probe.probe {
		t.Fatal("probe refused after the open period")
	}
	if b.can("a") {
		t.Fatal("second call allowed while the probe is in flight")
	}
	if got := b.Success("a", probe); got != Closed {
		t.Fatalf("probe success: transition %v, want Closed", got)
	}
	for i := 0; i < 2; i++ {
		if !b.can("a") {
			t.Fatal("closed breaker refused calls")
		}
	}
}

func TestBreakerProbeFailureReopens(t *testing.T) {
	b, c := newTestBreakers()
	tripBreaker(b, "a")
	c.advance(10 * time.Second)
	probe, _ := b.Allow("a")
	if got := b.Failure("a", probe); got != Opened {
		t.Fatalf("probe failure: transition %v, want Opened", got)
	}
	if b.can("a") {
		t.Fatal("reopened breaker allowed a call")
	}
	c.advance(10 * time.Second)
	if !b.can("a") {
		t.Fatal("no new probe after the second open period")
	}
}

func TestBreakerReleaseFreesTheProbeSlot(t *testing.T) {
	b, c := newTestBreakers()
	tripBreaker(b, "a")
	c.advance(10 * time.Second)
	probe, _ := b.Allow("a")
	b.Release("a", probe)
	if !b.can("a") {
		t.Fatal("probe slot stayed taken after Release")
	}
}

func TestBreakerIgnoresCallsFromBeforeItOpened(t *testing.T) {
	b, c := newTestBreakers()
	// Two calls are in flight when the breaker opens.
	stale1, _ := b.Allow("a")
	stale2, _ := b.Allow("a")
	tripBreaker(b, "a")

	if got := b.Success("a", stale1); got != NoChange || b.can("a") {
		t.Fatal("a success from before the breaker opened ended the open period")
	}

	// Half-open, with the probe in flight: a late success is not the probe's.
	c.advance(10 * time.Second)
	probe, _ := b.Allow("a")
	if got := b.Success("a", stale2); got != NoChange {
		t.Fatalf("stale success while half-open: transition %v, want NoChange", got)
	}
	if b.can("a") {
		t.Fatal("a stale success freed the probe slot")
	}
	// A late failure is not the probe's either, and must not reopen the breaker.
	if got := b.Failure("a", stale1); got != NoChange {
		t.Fatalf("stale failure while half-open: transition %v, want NoChange", got)
	}
	if got := b.Success("a", probe); got != Closed {
		t.Fatalf("probe success: transition %v, want Closed", got)
	}
}

func TestBreakerReleaseOfAStaleTicketKeepsTheProbeSlot(t *testing.T) {
	b, c := newTestBreakers()
	stale, _ := b.Allow("a")
	tripBreaker(b, "a")
	c.advance(10 * time.Second)
	b.Allow("a") // the probe
	b.Release("a", stale)
	if b.can("a") {
		t.Fatal("releasing a stale ticket freed the real probe's slot")
	}
}
