package relay

import (
	"context"
	"log/slog"
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

// DepthSampler reports each channel's queue depth at a fixed interval.
type DepthSampler struct {
	*runner
	depth    func(ctx context.Context, ch domain.Channel) (int, error)
	rec      ports.Recorder
	interval time.Duration
	log      *slog.Logger
}

func NewDepthSampler(depth func(ctx context.Context, ch domain.Channel) (int, error), rec ports.Recorder, interval time.Duration, log *slog.Logger) *DepthSampler {
	return &DepthSampler{runner: newRunner("queue-depth"), depth: depth, rec: rec, interval: interval, log: log}
}

// depthTimeout bounds one reading, so a stuck broker cannot hold up shutdown.
const depthTimeout = 3 * time.Second

// Start implements lifecycle.Component.
func (d *DepthSampler) Start(ctx context.Context) error {
	ctx = context.WithoutCancel(ctx)
	d.start(1, func(_ int, stop <-chan struct{}) {
		for ok := true; ok; ok = wait(stop, d.interval) {
			d.sample(ctx)
		}
	})
	return nil
}

// sample reads every channel once. A channel whose reading fails keeps its last value.
func (d *DepthSampler) sample(ctx context.Context) {
	for _, ch := range domain.Channels() {
		rctx, cancel := context.WithTimeout(ctx, depthTimeout)
		n, err := d.depth(rctx, ch)
		cancel()
		if err != nil {
			d.log.Warn("read queue depth", "channel", ch, "error", err)
			continue
		}
		d.rec.QueueDepth(ch, n)
	}
}
