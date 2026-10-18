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

func TestBreakerOpensAfterConsecutiveFailures(t *testing.T) {
	b, _ := newTestBreakers()
	for i := 0; i < 2; i++ {
		if !b.Allow("a") {
			t.Fatalf("call %d refused while closed", i)
		}
		if got := b.Failure("a"); got != NoChange {
			t.Fatalf("failure %d: transition %v, want NoChange", i, got)
		}
	}
	// A success resets the count, so two more failures still do not open it.
	b.Success("a")
	b.Failure("a")
	if got := b.Failure("a"); got != NoChange || !b.Allow("a") {
		t.Fatalf("breaker opened on non-consecutive failures")
	}
	if got := b.Failure("a"); got != Opened {
		t.Fatalf("third consecutive failure: transition %v, want Opened", got)
	}
	if b.Allow("a") {
		t.Fatal("open breaker allowed a call")
	}
}

func TestBreakersAreIndependentPerProvider(t *testing.T) {
	b, _ := newTestBreakers()
	for i := 0; i < 3; i++ {
		b.Failure("a")
	}
	if b.Allow("a") || !b.Allow("b") {
		t.Fatal("one provider's breaker affected another")
	}
}

func TestBreakerHalfOpenAdmitsOneProbe(t *testing.T) {
	b, c := newTestBreakers()
	for i := 0; i < 3; i++ {
		b.Failure("a")
	}
	c.advance(9 * time.Second)
	if b.Allow("a") {
		t.Fatal("allowed before the open period ended")
	}
	c.advance(time.Second)
	if !b.Allow("a") {
		t.Fatal("probe refused after the open period")
	}
	if b.Allow("a") {
		t.Fatal("second call allowed while the probe is in flight")
	}
	if got := b.Success("a"); got != Closed {
		t.Fatalf("probe success: transition %v, want Closed", got)
	}
	for i := 0; i < 2; i++ {
		if !b.Allow("a") {
			t.Fatal("closed breaker refused calls")
		}
	}
}

func TestBreakerProbeFailureReopens(t *testing.T) {
	b, c := newTestBreakers()
	for i := 0; i < 3; i++ {
		b.Failure("a")
	}
	c.advance(10 * time.Second)
	b.Allow("a")
	if got := b.Failure("a"); got != Opened {
		t.Fatalf("probe failure: transition %v, want Opened", got)
	}
	if b.Allow("a") {
		t.Fatal("reopened breaker allowed a call")
	}
	c.advance(10 * time.Second)
	if !b.Allow("a") {
		t.Fatal("no new probe after the second open period")
	}
}

func TestBreakerReleaseFreesTheProbeSlot(t *testing.T) {
	b, c := newTestBreakers()
	for i := 0; i < 3; i++ {
		b.Failure("a")
	}
	c.advance(10 * time.Second)
	b.Allow("a")
	b.Release("a")
	if !b.Allow("a") {
		t.Fatal("probe slot stayed taken after Release")
	}
}

func TestBreakerIgnoresAStaleSuccessWhileOpen(t *testing.T) {
	b, c := newTestBreakers()
	for i := 0; i < 3; i++ {
		b.Failure("a")
	}
	// A call made before the breaker opened finishes well.
	if got := b.Success("a"); got != NoChange {
		t.Fatalf("stale success: transition %v, want NoChange", got)
	}
	if b.Allow("a") {
		t.Fatal("a stale success ended the open period")
	}
	c.advance(10 * time.Second)
	if !b.Allow("a") {
		t.Fatal("no probe after the open period")
	}
}
