package sender

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync/atomic"
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

// Providers looks up a provider by name.
type Providers interface {
	Provider(name string) (ports.Provider, bool)
}

// Routing says which providers serve each channel and how each one is retried.
type Routing struct {
	// Order holds each channel's provider names in try order: the primary first.
	Order map[domain.Channel][]string
	// Policies holds each provider's retry policy by name.
	Policies map[string]domain.RetryPolicy
}

// levelCritical marks a log line an operator must see, above slog's error level.
const levelCritical = slog.LevelError + 4

// Attempt makes one provider call for a claimed delivery and settles the outcome in the
// inbox. A failure a retry could fix parks the row in WAITING_RETRY for the retry scan, and
// a provider that is out of retries, or unusable, moves the row to the channel's next
// provider. The last provider ends the delivery.
type Attempt struct {
	deliveries ports.DeliveryStore
	inbox      ports.InboxStore
	providers  Providers
	checker    ports.ProviderChecker
	breakers   *Breakers
	limits     *ProviderLimits
	routing    Routing
	log        *slog.Logger

	now    func() time.Time
	jitter func() float64
	// outage is set while every provider is unavailable, so one outage logs one critical line.
	outage atomic.Bool
}

// NewAttempt returns an Attempt. limits may be nil, which leaves every provider unlimited.
func NewAttempt(deliveries ports.DeliveryStore, inbox ports.InboxStore, providers Providers, checker ports.ProviderChecker,
	breakers *Breakers, limits *ProviderLimits, routing Routing, log *slog.Logger) *Attempt {
	return &Attempt{
		deliveries: deliveries, inbox: inbox, providers: providers, checker: checker,
		breakers: breakers, limits: limits, routing: routing, log: log,
		now:    time.Now,
		jitter: rand.Float64, //nolint:gosec // jitter spreads retries, it is not a secret
	}
}

// settleTimeout bounds the inbox write that records an attempt's outcome.
const settleTimeout = 5 * time.Second

// attemptRun is one attempt's context: the claim and where its provider sits in the channel's order.
type attemptRun struct {
	c        Claimed
	policy   domain.RetryPolicy
	provider ports.Provider
	// next is the provider after this one, or "" when this is the last.
	next string
	// ticket is the breaker's pass for this call; the result is reported with it.
	ticket Ticket
}

// Send implements Send. An attempt that gets an answer ends in one log line with the
// provider, the failure class and the outcome, and never with the contact. An error that
// leaves the row for the janitor is returned instead, and the caller logs it.
func (a *Attempt) Send(ctx context.Context, c Claimed) error {
	d, err := a.deliveries.Load(ctx, c.DeliveryID)
	if err != nil {
		return fmt.Errorf("load delivery %d: %w", c.DeliveryID, err)
	}
	// A webhook can finish a delivery before this attempt starts. Sending then would only
	// produce a duplicate, so the row is closed without a call.
	if d.State != domain.DeliveryPending {
		settleCtx, cancel := settleContext(ctx)
		defer cancel()
		applied, err := a.inbox.Complete(settleCtx, c.DeliveryID, c.ClaimCount)
		if err != nil {
			return err
		}
		a.logAttempt(c, "", outcomeSkipped, applied, "delivery_state", d.State)
		return nil
	}

	run, err := a.plan(d.Channel, c)
	if err != nil {
		return err
	}

	settleCtx, cancel := settleContext(ctx)
	defer cancel()

	if !d.ExpiresAt.IsZero() && !a.now().Before(d.ExpiresAt) {
		return a.fail(settleCtx, run, "", domain.ReasonExpired)
	}
	ticket, allowed := a.breakers.Allow(c.Provider)
	run.ticket = ticket
	if !allowed {
		if run.next == "" {
			return a.noProvider(settleCtx, run)
		}
		// An open breaker moves on without spending a retry.
		return a.switchTo(settleCtx, run, domain.FailureProvider, "breaker_open")
	}
	// The token comes after the breaker, so a call the breaker refuses wastes none. A refusal
	// here gives back the half-open probe slot the breaker just handed out.
	if ok, wait := a.limits.Take(c.Provider); !ok {
		a.breakers.Release(c.Provider, ticket)
		return a.throttle(settleCtx, run, wait, d.ExpiresAt)
	}

	err = run.provider.Send(ctx, ports.SendRequest{DeliveryID: d.ID, Channel: d.Channel, Contact: d.Contact, Content: d.Content})
	return a.settle(ctx, run, err)
}

// plan resolves the claim's provider, its retry policy and the provider after it.
func (a *Attempt) plan(ch domain.Channel, c Claimed) (attemptRun, error) {
	order := a.routing.Order[ch]
	i := slices.Index(order, c.Provider)
	if i < 0 {
		return attemptRun{}, fmt.Errorf("delivery %d: provider %q does not serve channel %s", c.DeliveryID, c.Provider, ch)
	}
	p, ok := a.providers.Provider(c.Provider)
	if !ok {
		return attemptRun{}, fmt.Errorf("delivery %d: unknown provider %q", c.DeliveryID, c.Provider)
	}
	// Checked before the call so a misconfigured provider cannot be hit without a retry delay.
	policy, ok := a.routing.Policies[c.Provider]
	if !ok {
		return attemptRun{}, fmt.Errorf("delivery %d: no retry policy configured for provider %q", c.DeliveryID, c.Provider)
	}
	run := attemptRun{c: c, policy: policy, provider: p}
	if i+1 < len(order) {
		run.next = order[i+1]
	}
	return run, nil
}

const (
	outcomeAccepted   = "accepted"
	outcomeFailed     = "failed"
	outcomeParked     = "parked"
	outcomeSkipped    = "skipped"
	outcomeThrottled  = "throttled"
	outcomeSwitched   = "switched"
	outcomeUnresolved = "unresolved"
)

// settleContext is the context for the inbox write that records an outcome. It survives the
// caller's cancellation: a shutdown right after the provider took the message must still
// record that, or the janitor would resend it.
func settleContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
}

// settle writes the outcome of the provider call to the inbox, fenced by the claim.
func (a *Attempt) settle(ctx context.Context, run attemptRun, sendErr error) error {
	settleCtx, cancel := settleContext(ctx)
	defer cancel()
	provider := run.c.Provider

	if sendErr == nil {
		if a.breakers.Success(provider, run.ticket) == Closed {
			a.outage.Store(false)
		}
		return a.complete(settleCtx, run.c, "")
	}

	class, classified := ports.ClassOf(sendErr)
	if !classified {
		// Not a statement about the provider, such as shutdown. The row stays PROCESSING
		// and the janitor decides what happened.
		a.breakers.Release(provider, run.ticket)
		return sendErr
	}

	if class == domain.FailureMessage {
		// The provider refused this message, not itself: the breaker learns nothing.
		a.breakers.Release(provider, run.ticket)
		return a.fail(settleCtx, run, class, domain.ReasonProviderRejected, "error", sendErr)
	}

	if a.breakers.Failure(provider, run.ticket) == Opened {
		a.log.Warn("circuit breaker opened", "provider", provider)
	}
	if class == domain.FailureTimeout {
		return a.settleTimeout(ctx, settleCtx, run, sendErr)
	}
	return a.retryOrMove(settleCtx, run, class, sendErr)
}

// settleTimeout resolves a call whose outcome is unknown by asking the provider whether it
// holds the message. checkCtx is the caller's context, so a shutdown stops the lookup and
// leaves the row for the janitor, which makes the same check.
func (a *Attempt) settleTimeout(checkCtx, settleCtx context.Context, run attemptRun, sendErr error) error {
	res, err := a.checker.Check(checkCtx, run.c.Provider, run.c.DeliveryID)
	if err != nil && checkCtx.Err() != nil {
		return checkCtx.Err()
	}
	switch {
	case err == nil && res == ports.CheckAccepted:
		return a.complete(settleCtx, run.c, domain.FailureTimeout, "lookup", "accepted")
	case err == nil && res == ports.CheckNotFound:
		// The provider never got it, so another try on the same provider is safe.
		return a.retryOrMove(settleCtx, run, domain.FailureTimeout, sendErr)
	}
	// No usable answer: the message may or may not be there. Another provider could
	// duplicate it, which is accepted; the last provider can only leave it unresolved.
	if run.next == "" {
		return a.unresolve(settleCtx, run, "lookup_error", err)
	}
	return a.switchTo(settleCtx, run, domain.FailureTimeout, "lookup_unusable")
}

// retryOrMove handles a failure the provider is known not to have taken: retry it while it
// has retries left, then move to the next provider, and after the last one end the delivery.
func (a *Attempt) retryOrMove(ctx context.Context, run attemptRun, class domain.FailureClass, sendErr error) error {
	c := run.c
	if c.Retries < run.policy.MaxRetries {
		// The delay grows with the retries already spent on this provider, not with claims,
		// which the janitor's reclaims also add to.
		next := a.now().Add(run.policy.Backoff.Delay(c.Retries+1, a.jitter()))
		applied, err := a.inbox.Park(ctx, c.DeliveryID, c.ClaimCount, class, next)
		if err != nil {
			return err
		}
		a.logAttempt(c, class, outcomeParked, applied, "error", sendErr, "next_attempt_at", next)
		return nil
	}
	if run.next != "" {
		return a.switchTo(ctx, run, class, "retries_exhausted", "error", sendErr)
	}
	return a.fail(ctx, run, class, domain.ReasonProviderFailed, "error", sendErr)
}

// throttleSpread spreads the deferred rows of a spike over this window, so they do not all come
// back at the instant the next token is free.
const throttleSpread = 250 * time.Millisecond

// throttle puts the row back on the same provider, due when its bucket will have a token, plus
// jitter, but no later than the delivery's deadline. It is not a failure: no retry is spent, the breaker hears nothing, and the claim does
// not count towards the cap. The deadline is checked again on the next attempt.
func (a *Attempt) throttle(ctx context.Context, run attemptRun, wait time.Duration, expiresAt time.Time) error {
	c := run.c
	next := a.now().Add(wait + time.Duration(a.jitter()*float64(throttleSpread)))
	// Never wait past the deadline: the row comes back at it and ends EXPIRED on time.
	if !expiresAt.IsZero() && next.After(expiresAt) {
		next = expiresAt
	}
	applied, err := a.inbox.Defer(ctx, c.DeliveryID, c.ClaimCount, next)
	if err != nil {
		return err
	}
	a.logAttempt(c, "", outcomeThrottled, applied, "next_attempt_at", next)
	return nil
}

// switchTo moves the row to the channel's next provider, due now, with retries reset.
func (a *Attempt) switchTo(ctx context.Context, run attemptRun, class domain.FailureClass, why string, extra ...any) error {
	c := run.c
	applied, err := a.inbox.Switch(ctx, c.DeliveryID, c.ClaimCount, run.next, class, a.now())
	if err != nil {
		return err
	}
	a.logAttempt(c, class, outcomeSwitched, applied, append([]any{"why", why, "next_provider", run.next}, extra...)...)
	return nil
}

func (a *Attempt) complete(ctx context.Context, c Claimed, class domain.FailureClass, extra ...any) error {
	applied, err := a.inbox.Complete(ctx, c.DeliveryID, c.ClaimCount)
	if err != nil {
		return err
	}
	a.logAttempt(c, class, outcomeAccepted, applied, extra...)
	return nil
}

func (a *Attempt) fail(ctx context.Context, run attemptRun, class domain.FailureClass, reason domain.FailureReason, extra ...any) error {
	c := run.c
	applied, err := a.inbox.Fail(ctx, c.DeliveryID, c.ClaimCount, class, reason)
	if err != nil {
		return err
	}
	a.logAttempt(c, class, outcomeFailed, applied, append([]any{"reason", string(reason)}, extra...)...)
	return nil
}

func (a *Attempt) unresolve(ctx context.Context, run attemptRun, why string, cause error) error {
	c := run.c
	applied, err := a.inbox.Unresolve(ctx, c.DeliveryID, c.ClaimCount, domain.FailureTimeout)
	if err != nil {
		return err
	}
	a.logAttempt(c, domain.FailureTimeout, outcomeUnresolved, applied, "why", why, "error", cause)
	return nil
}

// noProvider ends a delivery whose last provider is behind an open breaker. The critical
// line is logged once per outage, not once per delivery.
func (a *Attempt) noProvider(ctx context.Context, run attemptRun) error {
	if a.outage.CompareAndSwap(false, true) {
		a.log.Log(context.Background(), levelCritical, "no provider available: every circuit breaker is open",
			"provider", run.c.Provider)
	}
	return a.fail(ctx, run, domain.FailureProvider, domain.ReasonNoProviderAvailable)
}

// logAttempt writes the attempt's log line. applied false means the claim was stale: the
// row was reclaimed meanwhile, and this attempt's result was discarded.
func (a *Attempt) logAttempt(c Claimed, class domain.FailureClass, outcome string, applied bool, extra ...any) {
	attrs := append([]any{"delivery_id", c.DeliveryID, "provider", c.Provider, "claim", c.ClaimCount,
		"failure_class", string(class), "outcome", outcome}, extra...)
	level := slog.LevelInfo
	switch outcome {
	case outcomeAccepted, outcomeSkipped:
	case outcomeThrottled:
		// A spike defers thousands of rows; each is normal, so none is worth a warning.
		level = slog.LevelDebug
	default:
		level = slog.LevelWarn
	}
	if !applied {
		level = slog.LevelWarn
		attrs = append(attrs, "stale_claim", true)
	}
	a.log.Log(context.Background(), level, "send attempt", attrs...)
}
