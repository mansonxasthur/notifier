package sender

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

// Providers looks up a provider by name.
type Providers interface {
	Provider(name string) (ports.Provider, bool)
}

// Attempt makes one provider call for a claimed delivery and settles the outcome in the
// inbox. It does not retry or fail over: a failure that a retry could fix parks the row in
// WAITING_RETRY with its failure class, for the retry scan to pick up.
type Attempt struct {
	deliveries ports.DeliveryStore
	inbox      ports.InboxStore
	providers  Providers
	backoff    map[string]domain.Backoff
	log        *slog.Logger

	now    func() time.Time
	jitter func() float64
}

// NewAttempt returns an Attempt. backoff holds each provider's retry policy by name.
func NewAttempt(deliveries ports.DeliveryStore, inbox ports.InboxStore, providers Providers, backoff map[string]domain.Backoff, log *slog.Logger) *Attempt {
	return &Attempt{
		deliveries: deliveries, inbox: inbox, providers: providers, backoff: backoff, log: log,
		now:    time.Now,
		jitter: rand.Float64, //nolint:gosec // jitter spreads retries, it is not a secret
	}
}

// settleTimeout bounds the inbox write that records an attempt's outcome.
const settleTimeout = 5 * time.Second

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

	p, ok := a.providers.Provider(c.Provider)
	if !ok {
		return fmt.Errorf("delivery %d: unknown provider %q", c.DeliveryID, c.Provider)
	}
	// Checked before the call so a misconfigured provider cannot be hit without a retry delay.
	if _, ok := a.backoff[c.Provider]; !ok {
		return fmt.Errorf("delivery %d: no backoff configured for provider %q", c.DeliveryID, c.Provider)
	}
	err = p.Send(ctx, ports.SendRequest{DeliveryID: d.ID, Channel: d.Channel, Contact: d.Contact, Content: d.Content})
	return a.settle(ctx, c, err)
}

const (
	outcomeAccepted = "accepted"
	outcomeFailed   = "failed"
	outcomeParked   = "parked"
	outcomeSkipped  = "skipped"
)

// settleContext is the context for the inbox write that records an outcome. It survives the
// caller's cancellation: a shutdown right after the provider took the message must still
// record that, or the janitor would resend it.
func settleContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
}

// settle writes the outcome of the provider call to the inbox, fenced by the claim.
func (a *Attempt) settle(ctx context.Context, c Claimed, sendErr error) error {
	ctx, cancel := settleContext(ctx)
	defer cancel()

	if sendErr == nil {
		applied, err := a.inbox.Complete(ctx, c.DeliveryID, c.ClaimCount)
		if err != nil {
			return err
		}
		a.logAttempt(c, "", outcomeAccepted, applied)
		return nil
	}

	class, classified := ports.ClassOf(sendErr)
	if !classified {
		// Not a statement about the provider, such as shutdown. The row stays PROCESSING
		// and the janitor decides what happened.
		return sendErr
	}

	if class == domain.FailureMessage {
		applied, err := a.inbox.Fail(ctx, c.DeliveryID, c.ClaimCount, class, domain.ReasonProviderRejected)
		if err != nil {
			return err
		}
		a.logAttempt(c, class, outcomeFailed, applied, "error", sendErr)
		return nil
	}

	// The delay grows with the retries already spent on this provider, not with claims,
	// which the janitor's reclaims also add to.
	next := a.now().Add(a.backoff[c.Provider].Delay(c.Retries+1, a.jitter()))
	applied, err := a.inbox.Park(ctx, c.DeliveryID, c.ClaimCount, class, next)
	if err != nil {
		return err
	}
	a.logAttempt(c, class, outcomeParked, applied, "error", sendErr, "next_attempt_at", next)
	return nil
}

// logAttempt writes the attempt's log line. applied false means the claim was stale: the
// row was reclaimed meanwhile, and this attempt's result was discarded.
func (a *Attempt) logAttempt(c Claimed, class domain.FailureClass, outcome string, applied bool, extra ...any) {
	attrs := append([]any{"delivery_id", c.DeliveryID, "provider", c.Provider, "claim", c.ClaimCount,
		"failure_class", string(class), "outcome", outcome}, extra...)
	level := slog.LevelInfo
	if outcome == outcomeFailed || outcome == outcomeParked {
		level = slog.LevelWarn
	}
	if !applied {
		level = slog.LevelWarn
		attrs = append(attrs, "stale_claim", true)
	}
	a.log.Log(context.Background(), level, "send attempt", attrs...)
}
