package ports

import (
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
)

// Recorder receives the measurements the load test reads. Implementations must be safe for
// concurrent use and cheap: the callers sit on the hot path.
type Recorder interface {
	// QueueWait is how long a message sat in a channel's queue before a handler took it.
	QueueWait(ch domain.Channel, d time.Duration)
	// AcceptLatency is the time from a delivery's creation to its ACCEPTED write. first says
	// whether it was accepted by its channel's primary provider with no retry spent.
	AcceptLatency(ch domain.Channel, first bool, d time.Duration)
	// QueueDepth is the number of messages waiting in a channel's queue.
	QueueDepth(ch domain.Channel, n int)
}

// NopRecorder discards every measurement.
type NopRecorder struct{}

func (NopRecorder) QueueWait(domain.Channel, time.Duration)           {}
func (NopRecorder) AcceptLatency(domain.Channel, bool, time.Duration) {}
func (NopRecorder) QueueDepth(domain.Channel, int)                    {}
