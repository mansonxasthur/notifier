// Package memory is an in-memory queue for tests. It is not used by any entry point.
package memory

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

var (
	_ ports.Publisher = (*Queue)(nil)
	_ ports.Consumer  = (*Queue)(nil)
)

// Queue holds messages of delivery IDs per channel. A handler error puts the message back.
type Queue struct {
	mu   sync.Mutex
	size int
	msgs map[domain.Channel][][]int64
}

// New returns a queue whose messages hold at most size IDs.
func New(size int) *Queue {
	return &Queue{size: max(size, 1), msgs: map[domain.Channel][][]int64{}}
}

// Publish implements ports.Publisher and confirms everything.
func (q *Queue) Publish(_ context.Context, ch domain.Channel, ids []int64) ([]bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for first := 0; first < len(ids); first += q.size {
		q.msgs[ch] = append(q.msgs[ch], slices.Clone(ids[first:min(first+q.size, len(ids))]))
	}
	confirmed := make([]bool, len(ids))
	for i := range confirmed {
		confirmed[i] = true
	}
	return confirmed, nil
}

// Len is the number of messages waiting on the channel's queue.
func (q *Queue) Len(ch domain.Channel) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.msgs[ch])
}

func (q *Queue) pop(ch domain.Channel) ([]int64, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.msgs[ch]) == 0 {
		return nil, false
	}
	m := q.msgs[ch][0]
	q.msgs[ch] = q.msgs[ch][1:]
	return m, true
}

// Consume implements ports.Consumer.
func (q *Queue) Consume(ctx context.Context, ch domain.Channel, h ports.Handler) error {
	for ctx.Err() == nil {
		m, ok := q.pop(ch)
		if !ok {
			select {
			case <-ctx.Done():
			case <-time.After(time.Millisecond):
			}
			continue
		}
		if err := h(ctx, m); err != nil {
			// Like the broker consumer, only the ids a handler reports as failed come back.
			if be := new(ports.BatchError); errors.As(err, &be) && len(be.Failed) > 0 {
				m = be.Failed
			}
			q.mu.Lock()
			q.msgs[ch] = append(q.msgs[ch], m)
			q.mu.Unlock()
		}
	}
	return nil
}
