package ports

import (
	"context"
	"errors"
	"net/http"

	"github.com/mansonxasthur/notifier/internal/domain"
)

// ErrDeliveryNotFound means no delivery has the given id.
var ErrDeliveryNotFound = errors.New("delivery not found")

// DeliveryStore reads what a send attempt needs.
type DeliveryStore interface {
	// Load returns the delivery with its channel's content and contact. It returns
	// ErrDeliveryNotFound when there is no such delivery.
	Load(ctx context.Context, id int64) (domain.DeliveryDetail, error)
	// LoadMany loads many deliveries in one query. An id with no delivery is left out of the map.
	LoadMany(ctx context.Context, ids []int64) (map[int64]domain.DeliveryDetail, error)
}

// WebhookEvent is an authenticated provider webhook, with the delivery state it reports.
type WebhookEvent struct {
	DeliveryID int64
	Provider   string
	// Channel is the channel the provider serves. The event applies only to a delivery on
	// that channel, so one provider's secret cannot settle another channel's deliveries.
	Channel domain.Channel
	// EventType is the provider's event name as received, kept for the record.
	EventType string
	// Payload is the raw JSON body.
	Payload []byte
	// Outcome is the state the event reports: DELIVERED or FAILED.
	Outcome domain.DeliveryState
	// Reason is set when Outcome is FAILED.
	Reason domain.FailureReason
}

// WebhookAuthenticator checks that a webhook really comes from one provider. Providers
// authenticate in different ways, so each gets its own, built from its configuration.
type WebhookAuthenticator interface {
	// Authenticate returns nil when the request is genuine. body is the raw request body,
	// already read, because a signature covers its exact bytes.
	Authenticate(r *http.Request, body []byte) error
}

// WebhookStore records provider webhooks and applies them to deliveries.
type WebhookStore interface {
	// Apply stores the event and moves its delivery to the reported outcome, in one
	// transaction. The move is conditional on the delivery's current state, so a duplicate,
	// reordered or late event is stored but changes nothing. applied says whether the
	// delivery moved. It returns ErrDeliveryNotFound for an unknown delivery, and for one on
	// another channel than the event's.
	Apply(ctx context.Context, e WebhookEvent) (applied bool, err error)
}
