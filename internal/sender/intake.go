// Package sender consumes the channel queues and owns the inbox side of a delivery.
package sender

import (
	"context"
	"fmt"
	"log/slog"

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
	inbox     ports.InboxStore
	primary   map[domain.Channel]string
	maxClaims int
	send      Send
	log       *slog.Logger
}

// NewIntake claims a new delivery on its channel's primary provider and at most maxClaims claims per delivery.
func NewIntake(inbox ports.InboxStore, primary map[domain.Channel]string, maxClaims int, send Send, log *slog.Logger) *Intake {
	return &Intake{inbox: inbox, primary: primary, maxClaims: maxClaims, send: send, log: log}
}

// Handler returns the queue handler for a channel. The message is acknowledged (a nil
// return) only after the inbox write, so a crash before it redelivers the message and a
// redelivery after it is a duplicate that is dropped.
func (i *Intake) Handler(ch domain.Channel) (ports.Handler, error) {
	provider, ok := i.primary[ch]
	if !ok {
		return nil, fmt.Errorf("no primary provider for channel %s", ch)
	}
	return func(ctx context.Context, deliveryID int64) error {
		claim, err := i.inbox.Claim(ctx, deliveryID, provider, i.maxClaims)
		if err != nil {
			return fmt.Errorf("claim delivery %d: %w", deliveryID, err)
		}
		switch claim.Result {
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
