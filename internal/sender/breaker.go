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

// Ticket is what Allow hands a caller: which open period the call began in, and whether it is the
// half-open probe. The caller passes it back with the call's result.
type Ticket struct {
	// valid is false for the zero Ticket, so a result reported without one is ignored.
	valid bool
	epoch uint64
	probe bool
}

// Breakers holds one circuit breaker per provider, local to this sender. A breaker opens
// after FailureThreshold consecutive failures and refuses calls for OpenFor. It then lets
// exactly one probe through (half-open): the probe's success closes it, its failure opens it
// again. The result of a call that began before the breaker last opened is ignored, because it
// says nothing about the provider now.
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
	// epoch counts the times the breaker has opened.
	epoch uint64
	// probing is true while the half-open probe is in flight, and probeAt is when it began.
	probing bool
	probeAt time.Time
}

// NewBreakers returns breakers with the given policy. now is the clock, so tests can move it.
func NewBreakers(policy domain.BreakerPolicy, now func() time.Time) *Breakers {
	return &Breakers{policy: policy, now: now, by: map[string]*breaker{}}
}

// Allow reports whether a call to provider may go ahead. After the open period it admits
// one probe at a time. The caller must report the result with the ticket: Success, Failure, or
// Release when the call told us nothing.
func (b *Breakers) Allow(provider string) (Ticket, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	br := b.get(provider)
	switch br.state {
	case stateClosed:
		return Ticket{valid: true, epoch: br.epoch}, true
	case stateOpen:
		if b.now().Sub(br.openedAt) < b.policy.OpenFor {
			return Ticket{}, false
		}
		return b.probe(br), true
	default: // half-open: only the probe already in flight may proceed
		if !br.probing {
			return b.probe(br), true
		}
		if b.now().Sub(br.probeAt) < b.policy.OpenFor {
			return Ticket{}, false
		}
		// The probe has not answered for a whole open period: presume it lost, so the breaker
		// cannot stay half-open for good. Its late result, if any, is stale.
		br.epoch++
		return b.probe(br), true
	}
}

// probe makes the breaker half-open and hands out its one probe ticket.
func (b *Breakers) probe(br *breaker) Ticket {
	br.state = stateHalfOpen
	br.probing = true
	br.probeAt = b.now()
	return Ticket{valid: true, epoch: br.epoch, probe: true}
}

// Success records a call that worked.
func (b *Breakers) Success(provider string, t Ticket) Transition {
	b.mu.Lock()
	defer b.mu.Unlock()
	br := b.get(provider)
	if !t.valid || t.epoch != br.epoch {
		return NoChange
	}
	switch br.state {
	case stateHalfOpen:
		if !t.probe {
			return NoChange
		}
		br.probing = false
		br.failures = 0
		br.state = stateClosed
		return Closed
	case stateClosed:
		br.failures = 0
	}
	return NoChange
}

// Failure records a call that failed on the provider's side or timed out.
func (b *Breakers) Failure(provider string, t Ticket) Transition {
	b.mu.Lock()
	defer b.mu.Unlock()
	br := b.get(provider)
	if !t.valid || t.epoch != br.epoch {
		return NoChange
	}
	switch br.state {
	case stateHalfOpen:
		if !t.probe {
			return NoChange
		}
		br.open(b.now())
		return Opened
	case stateClosed:
		br.failures++
		if br.failures >= b.policy.FailureThreshold {
			br.open(b.now())
			return Opened
		}
	}
	return NoChange
}

// Release gives back the probe slot a ticket took when the call ended without telling us
// anything about the provider, such as a cancelled caller, a message-level rejection or a
// throttled send. It does nothing for a ticket that is not the current probe.
func (b *Breakers) Release(provider string, t Ticket) {
	b.mu.Lock()
	defer b.mu.Unlock()
	br := b.get(provider)
	if t.valid && t.probe && t.epoch == br.epoch && br.state == stateHalfOpen {
		br.probing = false
	}
}

func (br *breaker) open(now time.Time) {
	br.state = stateOpen
	br.openedAt = now
	br.failures = 0
	br.probing = false
	br.epoch++
}

func (b *Breakers) get(provider string) *breaker {
	br, ok := b.by[provider]
	if !ok {
		br = &breaker{}
		b.by[provider] = br
	}
	return br
}
