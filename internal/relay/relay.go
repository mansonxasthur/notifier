package relay

import (
	"context"
	"log/slog"
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

// Config tunes the relay loop.
type Config struct {
	PollInterval time.Duration
	BatchSize    int
	MaxAttempts  int
	Backoff      domain.Backoff
}

// Relay claims due outbox rows, publishes them and records the outcome. Delivery to the
// queue is at least once: a crash after a confirm but before MarkPublished republishes.
type Relay struct {
	*runner
	store ports.OutboxStore
	pub   ports.Publisher
	cfg   Config
	log   *slog.Logger
}

func New(store ports.OutboxStore, pub ports.Publisher, cfg Config, log *slog.Logger) *Relay {
	return &Relay{runner: newRunner("relay"), store: store, pub: pub, cfg: cfg, log: log}
}

// Start implements lifecycle.Component.
func (r *Relay) Start(ctx context.Context) error {
	// The loop outlives the start context's cancellation on purpose: Stop ends it between
	// batches, so a claimed batch is never abandoned half-published at shutdown.
	ctx = context.WithoutCancel(ctx)
	r.start(func(stop <-chan struct{}) {
		for {
			n := r.runOnce(ctx)
			if n >= r.cfg.BatchSize {
				select {
				case <-stop:
					return
				default:
					continue // a full batch means more is probably waiting
				}
			}
			if !wait(stop, r.cfg.PollInterval) {
				return
			}
		}
	})
	return nil
}

// runOnce handles one batch and returns how many rows it claimed.
func (r *Relay) runOnce(ctx context.Context) int {
	claims, err := r.store.Claim(ctx, r.cfg.BatchSize, r.cfg.MaxAttempts)
	if err != nil {
		r.log.Error("claim outbox", "error", err)
		return 0
	}
	if len(claims) == 0 {
		return 0
	}

	byChannel := map[domain.Channel][]domain.OutboxClaim{}
	for _, c := range claims {
		byChannel[c.Channel] = append(byChannel[c.Channel], c)
	}
	for ch, group := range byChannel {
		r.publish(ctx, ch, group)
	}
	return len(claims)
}

func (r *Relay) publish(ctx context.Context, ch domain.Channel, group []domain.OutboxClaim) {
	ids := make([]int64, len(group))
	for i, c := range group {
		ids[i] = c.DeliveryID
	}
	confirmed, err := r.pub.Publish(ctx, ch, ids)
	if err != nil {
		r.log.Warn("publish incomplete", "channel", ch, "batch", len(ids), "error", err)
	}

	var published int
	for i, c := range group {
		if confirmed[i] {
			if r.mark(ctx, c) {
				published++
			}
			continue
		}
		r.release(ctx, c)
	}
	r.log.Debug("published", "channel", ch, "published", published, "released", len(group)-published)
}

// mark records a confirmed publish. A fenced-out claim is logged, not retried: another
// relay owns the row now and its republish is harmless.
func (r *Relay) mark(ctx context.Context, c domain.OutboxClaim) bool {
	ok, err := r.store.MarkPublished(ctx, c)
	switch {
	case err != nil:
		// The row stays PROCESSING and the janitor reclaims it, which republishes.
		r.log.Error("mark published", "delivery_id", c.DeliveryID, "error", err)
	case !ok:
		r.log.Warn("claim lost before mark published", "delivery_id", c.DeliveryID)
	}
	return ok && err == nil
}

// release returns an unconfirmed row to NEW, due again after the backoff for its claims so far.
func (r *Relay) release(ctx context.Context, c domain.OutboxClaim) {
	next := time.Now().Add(r.cfg.Backoff.Delay(c.Retries, jitter()))
	ok, err := r.store.Release(ctx, c, next, domain.ReasonPublishFailed)
	switch {
	case err != nil:
		r.log.Error("release outbox", "delivery_id", c.DeliveryID, "error", err)
	case !ok:
		r.log.Warn("claim lost before release", "delivery_id", c.DeliveryID)
	}
}
