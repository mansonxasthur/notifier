package ports

import (
	"context"

	"github.com/mansonxasthur/notifier/internal/domain"
)

// Publisher puts delivery IDs on a channel's queue.
type Publisher interface {
	// Publish sends the IDs and waits for the broker to confirm them. confirmed[i] is true
	// only if ids[i] was confirmed, even when err is set: a failure part-way leaves the
	// earlier confirmations valid.
	Publish(ctx context.Context, ch domain.Channel, ids []int64) (confirmed []bool, err error)
}

// Handler processes one delivery ID. Returning nil acknowledges the message; an error
// returns it to the queue.
type Handler func(ctx context.Context, deliveryID int64) error

// Consumer reads delivery IDs from a channel's queue.
type Consumer interface {
	// Consume calls h for each message until ctx is done, reconnecting if the broker drops.
	Consume(ctx context.Context, ch domain.Channel, h Handler) error
}
