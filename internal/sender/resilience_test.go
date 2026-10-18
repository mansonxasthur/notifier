package sender

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

var (
	errProvider = &ports.SendError{Class: domain.FailureProvider, Err: errors.New("status 503")}
	errTimeout  = &ports.SendError{Class: domain.FailureTimeout, Err: context.DeadlineExceeded}
)

// onFallback puts the delivery's inbox row on sms-b, as a failover would, with the given
// retries already spent there, and points the claim at it.
func (f *fixture) onFallback(t *testing.T, retries int) {
	t.Helper()
	if _, err := f.db.Pool().Exec(context.Background(), `UPDATE inbox SET provider = 'sms-b', retries = $1`, retries); err != nil {
		t.Fatal(err)
	}
	f.claim.Provider, f.claim.Retries = "sms-b", retries
}

func (f *fixture) send(t *testing.T) {
	t.Helper()
	if err := f.attempt.Send(context.Background(), f.claim); err != nil {
		t.Fatal(err)
	}
}

// wantSwitched checks the row waits for a retry on provider with retries reset, due now.
func (f *fixture) wantSwitched(t *testing.T, provider string) {
	t.Helper()
	if got := f.inboxState(t); got != "WAITING_RETRY" {
		t.Errorf("inbox = %s, want WAITING_RETRY", got)
	}
	if got := f.text(t, `SELECT provider FROM inbox WHERE delivery_id = $1`); got != provider {
		t.Errorf("provider = %s, want %s", got, provider)
	}
	if got := f.text(t, `SELECT retries::text FROM inbox WHERE delivery_id = $1`); got != "0" {
		t.Errorf("retries = %s, want reset to 0", got)
	}
	var due time.Time
	if err := f.db.Pool().QueryRow(context.Background(), `SELECT next_attempt_at FROM inbox WHERE delivery_id = $1`, f.id).Scan(&due); err != nil {
		t.Fatal(err)
	}
	if !due.Equal(f.now) {
		t.Errorf("next_attempt_at = %v, want %v: a switch is due at once", due, f.now)
	}
	if got := f.deliveryState(t); got != "PENDING" {
		t.Errorf("delivery = %s, want PENDING", got)
	}
}

func (f *fixture) wantFailed(t *testing.T, reason string) {
	t.Helper()
	if got := f.inboxState(t); got != "DEAD" {
		t.Errorf("inbox = %s, want DEAD", got)
	}
	if got := f.deliveryState(t); got != "FAILED" {
		t.Errorf("delivery = %s, want FAILED", got)
	}
	if got := f.text(t, `SELECT failure_reason FROM deliveries WHERE id = $1`); got != reason {
		t.Errorf("failure_reason = %q, want %q", got, reason)
	}
}

func TestProviderFailureRetriesThenSwitchesThenFails(t *testing.T) {
	t.Run("retries left: parked on the same provider", func(t *testing.T) {
		f := newFixture(t, errProvider)
		f.send(t)
		if got := f.inboxState(t); got != "WAITING_RETRY" {
			t.Errorf("inbox = %s, want WAITING_RETRY", got)
		}
		if got := f.text(t, `SELECT provider FROM inbox WHERE delivery_id = $1`); got != "sms-a" {
			t.Errorf("provider = %s, want sms-a", got)
		}
	})

	t.Run("primary out of retries: switch to the fallback with retries reset", func(t *testing.T) {
		f := newFixture(t, errProvider)
		f.claim.Retries = 1 // max_retries is 1
		f.send(t)
		f.wantSwitched(t, "sms-b")
		if len(f.provB.calls) != 0 {
			t.Error("the fallback is called by the next attempt, not this one")
		}
	})

	t.Run("fallback has retries left: parked on the fallback", func(t *testing.T) {
		f := newFixture(t, nil)
		f.onFallback(t, 0)
		f.provB.err = errProvider
		f.send(t)
		if got := f.inboxState(t); got != "WAITING_RETRY" {
			t.Errorf("inbox = %s, want WAITING_RETRY", got)
		}
		if got := f.text(t, `SELECT provider FROM inbox WHERE delivery_id = $1`); got != "sms-b" {
			t.Errorf("provider = %s, want sms-b: never back to the primary", got)
		}
	})

	t.Run("fallback out of retries: FAILED at once", func(t *testing.T) {
		f := newFixture(t, nil)
		f.onFallback(t, 1)
		f.provB.err = errProvider
		f.send(t)
		f.wantFailed(t, "PROVIDER_FAILED")
	})
}

func TestTimeoutAsksTheProvider(t *testing.T) {
	t.Run("the provider has it: ACCEPTED, nothing is resent", func(t *testing.T) {
		f := newFixture(t, errTimeout)
		f.checker.res = ports.CheckAccepted
		f.send(t)
		if got := f.inboxState(t); got != "DONE" {
			t.Errorf("inbox = %s, want DONE", got)
		}
		if got := f.deliveryState(t); got != "ACCEPTED" {
			t.Errorf("delivery = %s, want ACCEPTED", got)
		}
		if len(f.prov.calls) != 1 || len(f.provB.calls) != 0 {
			t.Errorf("calls a=%d b=%d, want exactly the one timed-out call", len(f.prov.calls), len(f.provB.calls))
		}
		if len(f.checker.calls) != 1 || f.checker.calls[0] != "sms-a" {
			t.Errorf("lookups = %v, want one on sms-a", f.checker.calls)
		}
	})

	t.Run("the provider does not have it: same provider is retried", func(t *testing.T) {
		f := newFixture(t, errTimeout)
		f.checker.res = ports.CheckNotFound
		f.send(t)
		if got := f.inboxState(t); got != "WAITING_RETRY" {
			t.Errorf("inbox = %s, want WAITING_RETRY", got)
		}
		if got := f.text(t, `SELECT provider FROM inbox WHERE delivery_id = $1`); got != "sms-a" {
			t.Errorf("provider = %s, want sms-a", got)
		}
	})

	t.Run("not found and the primary is out of retries: switch", func(t *testing.T) {
		f := newFixture(t, errTimeout)
		f.claim.Retries = 1
		f.send(t)
		f.wantSwitched(t, "sms-b")
	})

	t.Run("no usable answer on the primary: switch without spending a retry", func(t *testing.T) {
		for name, tc := range map[string]struct {
			res ports.CheckResult
			err error
		}{
			"lookup unsupported": {res: ports.CheckUnknown},
			"lookup fails":       {err: errors.New("lookup 503")},
		} {
			t.Run(name, func(t *testing.T) {
				f := newFixture(t, errTimeout)
				f.checker.res, f.checker.err = tc.res, tc.err
				f.send(t)
				f.wantSwitched(t, "sms-b")
			})
		}
	})

	t.Run("fallback: no usable answer ends UNRESOLVED", func(t *testing.T) {
		f := newFixture(t, nil)
		f.onFallback(t, 0)
		f.provB.err = errTimeout
		f.checker.res = ports.CheckUnknown
		f.send(t)
		if got := f.inboxState(t); got != "DEAD" {
			t.Errorf("inbox = %s, want DEAD", got)
		}
		if got := f.deliveryState(t); got != "UNRESOLVED" {
			t.Errorf("delivery = %s, want UNRESOLVED", got)
		}
		if got := f.text(t, `SELECT failure_reason FROM deliveries WHERE id = $1`); got != "" {
			t.Errorf("failure_reason = %q, want none: nothing is known to have failed", got)
		}
	})

	t.Run("fallback: not found and out of retries ends FAILED", func(t *testing.T) {
		f := newFixture(t, nil)
		f.onFallback(t, 1)
		f.provB.err = errTimeout
		f.checker.res = ports.CheckNotFound
		f.send(t)
		f.wantFailed(t, "PROVIDER_FAILED")
	})

	t.Run("fallback: the provider has it", func(t *testing.T) {
		f := newFixture(t, nil)
		f.onFallback(t, 1)
		f.provB.err = errTimeout
		f.checker.res = ports.CheckAccepted
		f.send(t)
		if got := f.deliveryState(t); got != "ACCEPTED" {
			t.Errorf("delivery = %s, want ACCEPTED", got)
		}
	})

	t.Run("shutdown during the lookup leaves the row for the janitor", func(t *testing.T) {
		f := newFixture(t, errTimeout)
		ctx, cancel := context.WithCancel(context.Background())
		f.checker.err = context.Canceled
		cancel()
		if err := f.attempt.Send(ctx, f.claim); err == nil {
			t.Fatal("want the cancellation returned")
		}
		if got := f.inboxState(t); got != "PROCESSING" {
			t.Errorf("inbox = %s, want PROCESSING", got)
		}
	})
}

func TestBreakers(t *testing.T) {
	open := func(f *fixture, provider string) {
		tripBreaker(f.breaker, provider)
	}

	t.Run("open breaker on the primary: switch without a call or a spent retry", func(t *testing.T) {
		f := newFixture(t, nil)
		f.claim.Retries = 1
		open(f, "sms-a")
		f.send(t)
		f.wantSwitched(t, "sms-b")
		if len(f.prov.calls) != 0 {
			t.Error("an open breaker must not be called")
		}
	})

	t.Run("open breaker on the fallback: FAILED with NO_PROVIDER_AVAILABLE", func(t *testing.T) {
		f := newFixture(t, nil)
		f.onFallback(t, 0)
		open(f, "sms-b")
		f.send(t)
		f.wantFailed(t, "NO_PROVIDER_AVAILABLE")
		if len(f.provB.calls) != 0 {
			t.Error("an open breaker must not be called")
		}
	})

	t.Run("one critical line per outage", func(t *testing.T) {
		f := newFixture(t, nil)
		f.onFallback(t, 0)
		open(f, "sms-b")
		f.send(t)
		f.send(t) // a second delivery in the same outage; here the stale claim stands in for it
		if got := f.logs.count(levelCritical); got != 1 {
			t.Errorf("critical lines = %d, want 1", got)
		}
		// A provider that recovers ends the outage, so the next one is reported again.
		f.clock.advance(10 * time.Second)
		probe, ok := f.breaker.Allow("sms-b")
		if !ok {
			t.Fatal("probe refused")
		}
		f.attempt.outage.Store(true)
		// The claim is stale, so the inbox write changes nothing; only the breaker matters.
		if err := f.attempt.settle(context.Background(), attemptRun{c: f.claim, ticket: probe}, nil); err != nil {
			t.Fatal(err)
		}
		if f.attempt.outage.Load() {
			t.Error("a closed breaker should end the outage")
		}
	})

	t.Run("failures count toward the breaker, a message rejection does not", func(t *testing.T) {
		f := newFixture(t, &ports.SendError{Class: domain.FailureMessage, Err: errors.New("status 400")})
		for range 5 {
			f.send(t)
		}
		if !f.breaker.can("sms-a") {
			t.Error("message-level rejections must not open the breaker")
		}

		g := newFixture(t, errProvider)
		for range 3 {
			g.send(t)
			if _, err := g.db.Pool().Exec(context.Background(), `UPDATE inbox SET state = 'PROCESSING'`); err != nil {
				t.Fatal(err)
			}
		}
		if g.breaker.can("sms-a") {
			t.Error("three provider failures should open the breaker")
		}
	})
}

func TestExpiredDeliveryIsNotSent(t *testing.T) {
	f := newFixture(t, nil)
	f.now = time.Now().Add(2 * time.Hour) // past the fixture's one-hour deadline
	f.send(t)
	f.wantFailed(t, "EXPIRED")
	if len(f.prov.calls) != 0 {
		t.Error("an expired delivery must not be sent")
	}
}
