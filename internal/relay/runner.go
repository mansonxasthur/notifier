// Package relay moves outbox rows onto the queue and recovers rows that got stuck.
package relay

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"
)

// runner runs a loop in a goroutine and stops it between iterations, so a batch that has
// started always finishes before the process exits.
type runner struct {
	name string

	once sync.Once
	stop chan struct{}
	done chan struct{}
}

func newRunner(name string) *runner {
	return &runner{name: name, stop: make(chan struct{}), done: make(chan struct{})}
}

func (r *runner) Name() string { return r.name }

// start launches loop, which must return soon after stop is closed.
func (r *runner) start(loop func(stop <-chan struct{})) {
	go func() {
		defer close(r.done)
		loop(r.stop)
	}()
}

// Stop signals the loop and waits for it, up to ctx.
func (r *runner) Stop(ctx context.Context) error {
	r.once.Do(func() { close(r.stop) })
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return errors.New("stop " + r.name + ": " + ctx.Err().Error())
	}
}

// wait pauses for d and reports false if stop was closed first.
func wait(stop <-chan struct{}, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-stop:
		return false
	case <-t.C:
		return true
	}
}

// jitter returns a random value in [0, 1) for backoff delays.
func jitter() float64 {
	return rand.Float64() //nolint:gosec // jitter spreads retries, it is not a secret
}
