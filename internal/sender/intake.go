// Package sender consumes the channel queues and owns the inbox side of a delivery.
package sender

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

// Send performs the work for a delivery this sender won the claim for.
type Send func(ctx context.Context, deliveryID int64, provider string) error

// LogOnly is the Send used until the provider call exists: it records the claim and leaves
// the inbox row in PROCESSING, so the janitor's recovery path stays exercised.
func LogOnly(log *slog.Logger) Send {
	return func(_ context.Context, deliveryID int64, provider string) error {
		log.Info("claimed delivery", "delivery_id", deliveryID, "provider", provider)
		return nil
	}
}

// Intake turns queue messages into inbox claims.
type Intake struct {
	inbox     ports.InboxStore
	primary   map[domain.Channel]string
	maxClaims int
	send      Send
	log       *slog.Logger
}

// NewIntake claims with each channel's primary provider and at most maxClaims claims per delivery.
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
		res, err := i.inbox.Claim(ctx, deliveryID, provider, i.maxClaims)
		if err != nil {
			return fmt.Errorf("claim delivery %d: %w", deliveryID, err)
		}
		switch res {
		case domain.InboxClaimWon:
			// A failed send is not returned: requeueing the message would only produce a
			// duplicate that is dropped. The row stays PROCESSING and the janitor recovers it.
			if err := i.send(ctx, deliveryID, provider); err != nil {
				i.log.Error("send failed", "delivery_id", deliveryID, "provider", provider, "error", err)
			}
		case domain.InboxClaimDead:
			i.log.Error("delivery exceeded its claim cap", "delivery_id", deliveryID, "reason", domain.ReasonMaxClaimsExceeded)
		case domain.InboxClaimDuplicate:
			i.log.Debug("dropping duplicate message", "delivery_id", deliveryID)
		}
		return nil
	}, nil
}
