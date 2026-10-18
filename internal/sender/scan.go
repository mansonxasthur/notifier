package sender

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/mansonxasthur/notifier/internal/ports"
)

// Scan sends the retries that have become due. A parked or switched inbox row waits in
// WAITING_RETRY with a next_attempt_at; nothing goes back through the outbox or the broker,
// so the scan is the only thing that picks a retry up.
type Scan struct {
	inbox     ports.InboxStore
	send      Send
	interval  time.Duration
	batch     int
	maxClaims int
	log       *slog.Logger
}

// NewScan returns a Scan that looks for due retries every interval and claims up to batch
// of them at a time, with at most maxClaims claims per delivery.
func NewScan(inbox ports.InboxStore, send Send, interval time.Duration, batch, maxClaims int, log *slog.Logger) *Scan {
	return &Scan{inbox: inbox, send: send, interval: interval, batch: batch, maxClaims: maxClaims, log: log}
}

// Run scans until ctx is done, then waits for the sends already started. A full batch
// means more may be due, so the next scan follows at once instead of after the interval.
func (s *Scan) Run(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		n, err := s.Once(ctx)
		if err != nil && ctx.Err() == nil {
			s.log.Error("retry scan failed", "error", err)
		}
		wait := s.interval
		if err == nil && n >= s.batch {
			wait = 0
		}
		timer.Reset(wait)
	}
}

// Once claims the due retries, sends them concurrently, and returns how many it claimed.
// It returns after every send has finished. A send that fails leaves its row in
// PROCESSING, where the janitor recovers it.
func (s *Scan) Once(ctx context.Context) (int, error) {
	due, err := s.inbox.ClaimDue(ctx, s.batch, s.maxClaims)
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
