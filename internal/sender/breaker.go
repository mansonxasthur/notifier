package sender

import (
	"sync"
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
)

// Transition says whether a breaker call changed the breaker's state.
type Transition int

const (
	// NoChange: the breaker kept its state.
	NoChange Transition = iota
	// Opened: the breaker went from closed or half-open to open.
	Opened
	// Closed: the breaker went from half-open back to closed.
	Closed
)

type breakerState int

const (
	stateClosed breakerState = iota
	stateOpen
	stateHalfOpen
)

// Breakers holds one circuit breaker per provider, local to this sender. A breaker opens
// after FailureThreshold consecutive failures and refuses calls for OpenFor. It then lets
// exactly one probe through (half-open): a success closes it, a failure opens it again.
type Breakers struct {
	policy domain.BreakerPolicy
	now    func() time.Time

	mu sync.Mutex
	by map[string]*breaker
}

type breaker struct {
	state    breakerState
	failures int
	openedAt time.Time
	// probing is true while the half-open probe is in flight.
	probing bool
}

// NewBreakers returns breakers with the given policy. now is the clock, so tests can move it.
func NewBreakers(policy domain.BreakerPolicy, now func() time.Time) *Breakers {
	return &Breakers{policy: policy, now: now, by: map[string]*breaker{}}
}

// Allow reports whether a call to provider may go ahead. After the open period it admits
// one probe at a time; the caller must then report Success or Failure.
func (b *Breakers) Allow(provider string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	br := b.get(provider)
	switch br.state {
	case stateClosed:
		return true
	case stateOpen:
		if b.now().Sub(br.openedAt) < b.policy.OpenFor {
			return false
		}
		br.state = stateHalfOpen
		br.probing = true
		return true
	default: // half-open: only the probe already in flight may proceed
		if br.probing {
			return false
		}
		br.probing = true
		return true
	}
}

// Success records a call that worked.
func (b *Breakers) Success(provider string) Transition {
	b.mu.Lock()
	defer b.mu.Unlock()
	br := b.get(provider)
	br.failures = 0
	br.probing = false
	if br.state == stateClosed {
		return NoChange
	}
	br.state = stateClosed
	return Closed
}

// Failure records a call that failed on the provider's side or timed out.
func (b *Breakers) Failure(provider string) Transition {
	b.mu.Lock()
	defer b.mu.Unlock()
	br := b.get(provider)
	br.probing = false
	switch br.state {
	case stateOpen:
		return NoChange
	case stateHalfOpen:
		br.open(b.now())
		return Opened
	}
	br.failures++
	if br.failures < b.policy.FailureThreshold {
		return NoChange
	}
	br.open(b.now())
	return Opened
}

// Release gives back a probe slot taken by Allow when the call ended without telling us
// anything about the provider, such as a cancelled caller or a message-level rejection.
func (b *Breakers) Release(provider string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.get(provider).probing = false
}

func (br *breaker) open(now time.Time) {
	br.state = stateOpen
	br.openedAt = now
	br.failures = 0
}

func (b *Breakers) get(provider string) *breaker {
	br, ok := b.by[provider]
	if !ok {
		br = &breaker{}
		b.by[provider] = br
	}
	return br
}
