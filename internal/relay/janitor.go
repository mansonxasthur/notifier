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

// SweepInbox recovers inbox rows stuck in PROCESSING. A row is sent again only when the
// provider does not confirm having the message: a confirmed one is ACCEPTED instead.
func (j *Janitor) SweepInbox(ctx context.Context) {
	for _, p := range j.cfg.Providers {
		stuck, err := j.inbox.ListStuck(ctx, p.Name, p.Timeout+j.cfg.InboxStuckMargin, inboxSweepLimit)
		if err != nil {
			j.log.Error("list stuck inbox rows", "provider", p.Name, "error", err)
			continue
		}
		for _, s := range stuck {
			j.recover(ctx, s)
		}
	}
}

func (j *Janitor) recover(ctx context.Context, s domain.StuckInbox) {
	res, err := j.checker.Check(ctx, s.Provider, s.DeliveryID)
	if err != nil {
		// No answer is handled like an unknown one: the row is retried.
		j.log.Warn("provider check failed", "delivery_id", s.DeliveryID, "provider", s.Provider, "error", err)
		res = ports.CheckUnknown
	}

	if res == ports.CheckAccepted {
		ok, err := j.inbox.Accept(ctx, s)
		j.logRecovery(s, "accepted", ok, err)
		return
	}
	next := time.Now().Add(j.cfg.Backoff.Delay(s.ClaimCount, jitter()))
	ok, err := j.inbox.Requeue(ctx, s, next)
	j.logRecovery(s, "requeued", ok, err)
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
