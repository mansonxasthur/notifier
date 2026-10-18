package ports

import (
	"context"
	"errors"

	"github.com/mansonxasthur/notifier/internal/domain"
)

// ErrDeliveryNotFound means no delivery has the given id.
var ErrDeliveryNotFound = errors.New("delivery not found")

// DeliveryStore reads what a send attempt needs.
type DeliveryStore interface {
	// Load returns the delivery with its channel's content and contact. It returns
	// ErrDeliveryNotFound when there is no such delivery.
	Load(ctx context.Context, id int64) (domain.DeliveryDetail, error)
}

// WebhookEvent is an authenticated provider webhook, with the delivery state it reports.
type WebhookEvent struct {
	DeliveryID int64
	Provider   string
	// EventType is the provider's event name as received, kept for the record.
	EventType string
	// Payload is the raw JSON body.
	Payload []byte
	// Outcome is the state the event reports: DELIVERED or FAILED.
	Outcome domain.DeliveryState
	// Reason is set when Outcome is FAILED.
	Reason domain.FailureReason
}

// WebhookStore records provider webhooks and applies them to deliveries.
type WebhookStore interface {
	// Apply stores the event and moves its delivery to the reported outcome, in one
	// transaction. The move is conditional on the delivery's current state, so a duplicate,
	// reordered or late event is stored but changes nothing. applied says whether the
	// delivery moved. It returns ErrDeliveryNotFound for an unknown delivery.
	Apply(ctx context.Context, e WebhookEvent) (applied bool, err error)
}
