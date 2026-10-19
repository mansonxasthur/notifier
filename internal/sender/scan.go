package sender

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

// Scan sends the retries that have become due. A parked or switched inbox row waits in
// WAITING_RETRY with a next_attempt_at; nothing goes back through the outbox or the broker,
// so the scan is the only thing that picks a retry up.
//
// Each channel has its own scan loop, so a backlog on one channel, or its provider's rate
// limit, cannot fill the batches and starve the others.
type Scan struct {
	inbox    ports.InboxStore
	send     Send
	interval time.Duration
	batch    int
	limits   domain.ClaimLimits
	log      *slog.Logger
}

// NewScan returns a Scan that looks for due retries of each channel every interval and claims up
// to batch of a channel's at a time, within the claim limits.
func NewScan(inbox ports.InboxStore, send Send, interval time.Duration, batch int, limits domain.ClaimLimits, log *slog.Logger) *Scan {
	return &Scan{inbox: inbox, send: send, interval: interval, batch: batch, limits: limits, log: log}
}

// Run scans every channel until ctx is done, then waits for the sends already started.
func (s *Scan) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, ch := range domain.Channels() {
		wg.Go(func() { s.runChannel(ctx, ch) })
	}
	wg.Wait()
}

// runChannel is one channel's loop. A scan that found anything means more may be due behind it,
// so the next scan follows at once; one that found nothing waits for the interval. (A full batch
// is not the test: rows the claim cap ended are consumed from the batch but not returned.)
func (s *Scan) runChannel(ctx context.Context, ch domain.Channel) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		n, err := s.Once(ctx, ch)
		if err != nil && ctx.Err() == nil {
			s.log.Error("retry scan failed", "channel", ch, "error", err)
		}
		wait := s.interval
		if err == nil && n > 0 {
			wait = 0
		}
		timer.Reset(wait)
	}
}

// Once claims the channel's due retries, sends them concurrently, and returns how many it
// claimed. It returns after every send has finished. A send that fails leaves its row in
// PROCESSING, where the janitor recovers it.
func (s *Scan) Once(ctx context.Context, ch domain.Channel) (int, error) {
	due, err := s.inbox.ClaimDue(ctx, ch, s.batch, s.limits)
	if err != nil {
		return 0, err
	}
	var wg sync.WaitGroup
	for _, d := range due {
		wg.Go(func() {
			c := Claimed{DeliveryID: d.DeliveryID, Provider: d.Provider, ClaimCount: d.ClaimCount, Retries: d.Retries}
			if err := s.send(ctx, c); err != nil {
				s.log.Error("retry failed", "delivery_id", d.DeliveryID, "provider", d.Provider, "error", err)
			}
		})
	}
	wg.Wait()
	return len(due), nil
}
