package relay

import (
	"context"
	"log/slog"
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

// inboxSweepLimit bounds the stuck rows one sweep handles per provider.
const inboxSweepLimit = 500

// Provider is what the inbox sweep needs to know about a provider.
type Provider struct {
	Name    string
	Timeout time.Duration
	// Next is the provider after this one on its channel, or empty for the last.
	Next string
	// MaxRetries and Backoff are the provider's own retry policy, which a requeued row follows
	// exactly as it would after a failed send.
	MaxRetries int
	Backoff    domain.Backoff
}

// JanitorConfig tunes the sweeps.
type JanitorConfig struct {
	Interval         time.Duration
	OutboxStuckAfter time.Duration
	// InboxStuckMargin is added to a provider's timeout to get its inbox threshold.
	InboxStuckMargin time.Duration
	Backoff          domain.Backoff
	Providers        []Provider
}

// Janitor reclaims outbox and inbox rows left in PROCESSING by a crashed relay or sender.
type Janitor struct {
	*runner
	outbox  ports.OutboxStore
	inbox   ports.InboxStore
	checker ports.ProviderChecker
	cfg     JanitorConfig
	log     *slog.Logger
}

func NewJanitor(outbox ports.OutboxStore, inbox ports.InboxStore, checker ports.ProviderChecker, cfg JanitorConfig, log *slog.Logger) *Janitor {
	return &Janitor{runner: newRunner("janitor"), outbox: outbox, inbox: inbox, checker: checker, cfg: cfg, log: log}
}

// Start implements lifecycle.Component.
func (j *Janitor) Start(ctx context.Context) error {
	ctx = context.WithoutCancel(ctx)
	j.start(1, func(_ int, stop <-chan struct{}) {
		for wait(stop, j.cfg.Interval) {
			j.SweepOutbox(ctx)
			j.SweepInbox(ctx)
		}
	})
	return nil
}

// SweepOutbox returns outbox rows stuck in PROCESSING to NEW.
func (j *Janitor) SweepOutbox(ctx context.Context) {
	n, err := j.outbox.ReclaimStuck(ctx, j.cfg.OutboxStuckAfter, func(retries int) time.Duration {
		return j.cfg.Backoff.Delay(retries, jitter())
	})
	if err != nil {
		j.log.Error("sweep outbox", "error", err)
		return
	}
	if n > 0 {
		j.log.Warn("reclaimed stuck outbox rows", "count", n)
	}
}

// SweepInbox recovers inbox rows stuck in PROCESSING with the same check the sender makes
// after a timeout. A provider that has the message makes it ACCEPTED. One that does not
// know it gets the row again. One that cannot say moves the row to the next provider, or
// ends the delivery UNRESOLVED if there is none. A row past its deadline is not sent again.
func (j *Janitor) SweepInbox(ctx context.Context) {
	for _, p := range j.cfg.Providers {
		stuck, err := j.inbox.ListStuck(ctx, p.Name, p.Timeout+j.cfg.InboxStuckMargin, inboxSweepLimit)
		if err != nil {
			j.log.Error("list stuck inbox rows", "provider", p.Name, "error", err)
			continue
		}
		for _, s := range stuck {
			j.recover(ctx, p, s)
		}
	}
}

func (j *Janitor) recover(ctx context.Context, p Provider, s domain.StuckInbox) {
	// Judged before the provider check, which can take seconds, so a row inside its deadline
	// when it was picked is not failed for the time the check took.
	expired := !time.Now().Before(s.ExpiresAt)

	res, err := j.checker.Check(ctx, s.Provider, s.DeliveryID)
	if err != nil {
		j.log.Warn("provider check failed", "delivery_id", s.DeliveryID, "provider", s.Provider, "error", err)
		res = ports.CheckUnknown
	}

	// Past its deadline a row is never sent again, so "not found" ends it EXPIRED, as the
	// sender's own check would, and an unknown answer ends it UNRESOLVED, because the provider
	// may already hold the message. A provider that confirms it holds the message still wins.
	var ok bool
	var action string
	switch {
	case res == ports.CheckAccepted:
		action = "accepted"
		ok, err = j.inbox.Accept(ctx, s)
	case expired && res == ports.CheckNotFound:
		action = "expired"
		ok, err = j.inbox.Fail(ctx, s.DeliveryID, s.ClaimCount, domain.FailureNone, domain.ReasonExpired)
	case expired:
		action = "unresolved"
		ok, err = j.inbox.Unresolve(ctx, s.DeliveryID, s.ClaimCount, domain.FailureTimeout)
	case res == ports.CheckNotFound && s.Retries < p.MaxRetries:
		// The provider never got it, so the same provider is safe to try again. This spends
		// a retry, so a provider that keeps losing messages is eventually left behind.
		action = "requeued"
		next := time.Now().Add(p.Backoff.Delay(s.Retries+1, jitter()))
		ok, err = j.inbox.Requeue(ctx, s, domain.FailureTimeout, next)
	case res == ports.CheckNotFound && p.Next == "":
		// Out of retries on the last provider, and it says it never had the message.
		action = "failed"
		ok, err = j.inbox.Fail(ctx, s.DeliveryID, s.ClaimCount, domain.FailureTimeout, domain.ReasonProviderFailed)
	case p.Next != "":
		action = "switched"
		ok, err = j.inbox.Switch(ctx, s.DeliveryID, s.ClaimCount, p.Next, domain.FailureTimeout, time.Now())
	default:
		action = "unresolved"
		ok, err = j.inbox.Unresolve(ctx, s.DeliveryID, s.ClaimCount, domain.FailureTimeout)
	}
	j.logRecovery(s, action, ok, err)
}

func (j *Janitor) logRecovery(s domain.StuckInbox, action string, ok bool, err error) {
	switch {
	case err != nil:
		j.log.Error("recover inbox row", "delivery_id", s.DeliveryID, "action", action, "error", err)
	case !ok:
		j.log.Info("inbox row changed before recovery", "delivery_id", s.DeliveryID)
	default:
		j.log.Warn("recovered stuck inbox row", "delivery_id", s.DeliveryID, "provider", s.Provider, "action", action)
	}
}
