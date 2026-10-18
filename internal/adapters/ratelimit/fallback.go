package ratelimit

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/mansonxasthur/notifier/internal/ports"
)

// Fallback asks a shared limiter and, when that fails, a local one, so limiting becomes
// approximate instead of absent. With N instances the effective limit while the shared limiter
// is down is up to N times the rate. A shared call that failed after taking its charge, such as
// one that timed out, is charged again locally; that errs toward refusing.
//
// The shared limiter gets a deadline per call, and after a failure it is skipped for a cooldown,
// so a hung Redis costs one slow request per cooldown, not one per request. When the cooldown
// ends a single caller probes; the rest keep using the local limiter until the probe answers.
// The healthy path takes no lock.
type Fallback struct {
	shared, local ports.Limiter
	timeout       time.Duration
	cooldown      time.Duration
	log           *slog.Logger
	now           func() time.Time

	// skipTill is zero while the shared limiter is healthy. Otherwise it is the unix time in
	// nanoseconds before which the shared limiter is not tried; a probe pushes it forward.
	skipTill atomic.Int64
}

var _ ports.Limiter = (*Fallback)(nil)

// NewFallback wraps shared with local. timeout bounds each shared call; cooldown is how long
// the shared limiter is left alone after a failure.
func NewFallback(shared, local ports.Limiter, timeout, cooldown time.Duration, log *slog.Logger, now func() time.Time) *Fallback {
	return &Fallback{shared: shared, local: local, timeout: timeout, cooldown: cooldown, log: log, now: now}
}

// Allow answers from the local limiter when the shared one fails. It returns an error only when
// the caller's own context has ended, which says nothing about the shared limiter.
func (f *Fallback) Allow(ctx context.Context, n int) (ports.Decision, error) {
	if ok, probe := f.claimShared(); ok {
		callCtx, cancel := context.WithTimeout(ctx, f.timeout)
		d, err := f.shared.Allow(callCtx, n)
		cancel()
		if err == nil {
			// Only the probe ends an outage: a slow call that began before it says nothing now.
			if probe {
				f.recovered()
			}
			return d, nil
		}
		if ctx.Err() != nil {
			return ports.Decision{}, ctx.Err()
		}
		f.failed(err)
	}
	return f.local.Allow(ctx, n)
}

// claimShared reports whether this caller should try the shared limiter: always while it is
// healthy, and for exactly one caller, the probe, once the cooldown has passed.
func (f *Fallback) claimShared() (ok, probe bool) {
	until := f.skipTill.Load()
	if until == 0 {
		return true, false
	}
	now := f.now().UnixNano()
	if now >= until && f.skipTill.CompareAndSwap(until, now+f.cooldown.Nanoseconds()) {
		return true, true
	}
	return false, false
}

func (f *Fallback) failed(err error) {
	if f.skipTill.Swap(f.now().UnixNano()+f.cooldown.Nanoseconds()) == 0 {
		f.log.Warn("shared rate limiter unavailable; limiting locally, so the limit is approximate", "error", err)
	}
}

func (f *Fallback) recovered() {
	if f.skipTill.Swap(0) != 0 {
		f.log.Info("shared rate limiter recovered")
	}
}
