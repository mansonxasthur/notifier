package ports

import (
	"context"
	"fmt"

	"github.com/mansonxasthur/notifier/internal/domain"
)

// Publisher puts delivery IDs on a channel's queue, many to a message.
type Publisher interface {
	// Publish sends the IDs and waits for the broker to confirm them. confirmed[i] is true
	// only if the message carrying ids[i] was confirmed, even when err is set: a failure part-way leaves the
	// earlier confirmations valid.
	Publish(ctx context.Context, ch domain.Channel, ids []int64) (confirmed []bool, err error)
}

// Handler processes the delivery IDs of one message. Returning nil acknowledges the message,
// so it means every ID was handled. A *BatchError names the IDs that were not, so the
// others are not handled again; any other error returns the whole message to the queue.
type Handler func(ctx context.Context, ids []int64) error

// BatchError is a Handler's report that only some IDs of a message failed.
type BatchError struct {
	// Failed holds the IDs to try again; the rest of the message was handled.
	Failed []int64
	Err    error
}

func (e *BatchError) Error() string {
	return fmt.Sprintf("%d ids failed: %v", len(e.Failed), e.Err)
}

func (e *BatchError) Unwrap() error { return e.Err }

// Consumer reads messages of delivery IDs from a channel's queue.
type Consumer interface {
	// Consume calls h for each message until ctx is done, reconnecting if the broker drops.
	Consume(ctx context.Context, ch domain.Channel, h Handler) error
}
