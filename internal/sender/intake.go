// Package sender consumes the channel queues and owns the inbox side of a delivery.
package sender

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

// Claimed is a delivery this sender won the claim for. ClaimCount is the fencing token:
// the sender's later updates to the inbox row apply only while the row still carries it.
type Claimed struct {
	DeliveryID int64
	Provider   string
	ClaimCount int
	// Retries is how many retries the row has already used on this provider.
	Retries int
}

// Send performs the work for a claimed delivery. A returned error leaves the inbox row in
// PROCESSING, where the janitor recovers it.
type Send func(ctx context.Context, c Claimed) error

// Intake turns queue messages into inbox claims.
type Intake struct {
	inbox   ports.InboxStore
	primary map[domain.Channel]string
	limits  domain.ClaimLimits
	send    Send
	log     *slog.Logger

	batchSize int
	linger    time.Duration
	// claimers are the per-channel claim batches, made by Handler.
	mu       sync.Mutex
	claimers []*Coalescer[int64, domain.InboxClaim]
}

// NewIntake claims a new delivery on its channel's primary provider, within the claim limits.
func NewIntake(inbox ports.InboxStore, primary map[domain.Channel]string, limits domain.ClaimLimits, send Send, log *slog.Logger) *Intake {
	return &Intake{inbox: inbox, primary: primary, limits: limits, send: send, log: log, batchSize: 1}
}

// WithBatching makes the claims of one channel's concurrent messages share a transaction:
// up to size at a time, flushed after linger at the latest. Call it before Handler.
func (i *Intake) WithBatching(size int, linger time.Duration) *Intake {
	i.batchSize, i.linger = size, linger
	return i
}

// Close flushes the claim batches still waiting. Call it after the last handler has returned.
func (i *Intake) Close() {
	i.mu.Lock()
	claimers := i.claimers
	i.mu.Unlock()
	for _, c := range claimers {
		c.Close()
	}
}

// Handler returns the queue handler for a channel. The message is acknowledged (a nil
// return) only after the inbox write, so a crash before it redelivers the message and a
// redelivery after it is a duplicate that is dropped.
func (i *Intake) Handler(ch domain.Channel) (ports.Handler, error) {
	provider, ok := i.primary[ch]
	if !ok {
		return nil, fmt.Errorf("no primary provider for channel %s", ch)
	}
	claimer := NewCoalescer(i.batchSize, i.linger, func(ctx context.Context, ids []int64) ([]domain.InboxClaim, error) {
		return i.inbox.ClaimMany(ctx, ids, provider, i.limits)
	})
	i.mu.Lock()
	i.claimers = append(i.claimers, claimer)
	i.mu.Unlock()
	return func(ctx context.Context, deliveryID int64) error {
		// The claim survives the handler's cancellation: once submitted it is written, and a message
		// requeued after that would find its own claim and be dropped as a duplicate.
		claim, err := claimer.Do(context.WithoutCancel(ctx), deliveryID)
		if err != nil {
			return fmt.Errorf("claim delivery %d: %w", deliveryID, err)
		}
		switch claim.Result {
		case domain.InboxClaimMissing:
			// Like any claim that cannot be written, this requeues the message.
			return fmt.Errorf("claim delivery %d: %w", deliveryID, ports.ErrDeliveryNotFound)
		case domain.InboxClaimWon:
			// A failed send is not returned: requeueing the message would only produce a
			// duplicate that is dropped. The row stays PROCESSING and the janitor recovers it.
			// The row's own provider, not the primary: a failover moves it.
			c := Claimed{DeliveryID: deliveryID, Provider: claim.Provider, ClaimCount: claim.ClaimCount, Retries: claim.Retries}
			if err := i.send(ctx, c); err != nil {
				i.log.Error("send failed", "delivery_id", deliveryID, "provider", claim.Provider, "error", err)
			}
		case domain.InboxClaimDead:
			i.log.Error("delivery exceeded its claim cap", "delivery_id", deliveryID, "reason", domain.ReasonMaxClaimsExceeded)
		case domain.InboxClaimDuplicate:
			i.log.Debug("dropping duplicate message", "delivery_id", deliveryID)
		}
		return nil
	}, nil
}
