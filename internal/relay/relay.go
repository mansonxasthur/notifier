package relay

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

// Config tunes the relay loops.
type Config struct {
	PollInterval time.Duration
	BatchSize    int
	// Workers is how many claim-and-publish loops run. SKIP LOCKED keeps them off each
	// other's rows.
	Workers     int
	MaxAttempts int
	Backoff     domain.Backoff
}

// Relay claims due outbox rows, publishes them and records the outcome. Delivery to the
// queue is at least once: a crash after a confirm but before MarkPublished republishes.
type Relay struct {
	*runner
	store        ports.OutboxStore
	newPublisher func() ports.Publisher
	cfg          Config
	log          *slog.Logger

	mu         sync.Mutex
	publishers []ports.Publisher
}

// New returns a relay whose workers each publish through their own publisher from
// newPublisher, so one slow confirm does not hold up the others.
func New(store ports.OutboxStore, newPublisher func() ports.Publisher, cfg Config, log *slog.Logger) *Relay {
	return &Relay{runner: newRunner("relay"), store: store, newPublisher: newPublisher, cfg: cfg, log: log}
}

// Start implements lifecycle.Component.
func (r *Relay) Start(ctx context.Context) error {
	// The loops outlive the start context's cancellation on purpose: Stop ends them between
	// batches, so a claimed batch is never abandoned half-published at shutdown.
	ctx = context.WithoutCancel(ctx)
	pubs := make([]ports.Publisher, r.cfg.Workers)
	for i := range pubs {
		pubs[i] = r.newPublisher()
	}
	r.mu.Lock()
	r.publishers = pubs
	r.mu.Unlock()

	r.start(r.cfg.Workers, func(worker int, stop <-chan struct{}) {
		pub := pubs[worker]
		for {
			n := r.runOnce(ctx, pub)
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

// Stop implements lifecycle.Component. It waits for the workers, then closes the
// publishers that have something to close.
func (r *Relay) Stop(ctx context.Context) error {
	if err := r.runner.Stop(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range r.publishers {
		if s, ok := p.(interface{ Stop() }); ok {
			s.Stop()
		}
	}
	return nil
}

// runOnce handles one batch and returns how many rows it claimed. Every confirmed row of
// the batch is marked PUBLISHED in a single update.
func (r *Relay) runOnce(ctx context.Context, pub ports.Publisher) int {
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
	confirmed := make([]domain.OutboxClaim, 0, len(claims))
	for ch, group := range byChannel {
		confirmed = append(confirmed, r.publish(ctx, pub, ch, group)...)
	}
	r.markPublished(ctx, confirmed)
	return len(claims)
}

// publish sends one channel's rows, releases the unconfirmed ones and returns the confirmed.
func (r *Relay) publish(ctx context.Context, pub ports.Publisher, ch domain.Channel, group []domain.OutboxClaim) []domain.OutboxClaim {
	ids := make([]int64, len(group))
	for i, c := range group {
		ids[i] = c.DeliveryID
	}
	ok, err := pub.Publish(ctx, ch, ids)
	if err != nil {
		r.log.Warn("publish incomplete", "channel", ch, "batch", len(ids), "error", err)
	}

	confirmed := make([]domain.OutboxClaim, 0, len(group))
	for i, c := range group {
		if ok[i] {
			confirmed = append(confirmed, c)
			continue
		}
		r.release(ctx, c)
	}
	r.log.Debug("published", "channel", ch, "published", len(confirmed), "released", len(group)-len(confirmed))
	return confirmed
}

// markPublished records the confirmed rows. Fenced-out claims are logged, not retried:
// another relay owns those rows now and its republish is harmless.
func (r *Relay) markPublished(ctx context.Context, confirmed []domain.OutboxClaim) {
	n, err := r.store.MarkPublished(ctx, confirmed)
	switch {
	case err != nil:
		// The rows stay PROCESSING and the janitor reclaims them, which republishes.
		r.log.Error("mark published", "count", len(confirmed), "error", err)
	case n < len(confirmed):
		r.log.Warn("claims lost before mark published", "count", len(confirmed)-n)
	}
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
