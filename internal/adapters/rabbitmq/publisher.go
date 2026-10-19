package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

var _ ports.Publisher = (*Publisher)(nil)

// Publisher publishes persistent messages with publisher confirms.
type Publisher struct {
	conn           *Conn
	confirmTimeout time.Duration
	messageSize    int

	mu sync.Mutex
	ch *amqp.Channel
}

// NewPublisher waits at most confirmTimeout for the broker to confirm a batch, and puts at
// most messageSize IDs in a message.
func NewPublisher(conn *Conn, confirmTimeout time.Duration, messageSize int) *Publisher {
	return &Publisher{conn: conn, confirmTimeout: confirmTimeout, messageSize: max(messageSize, 1)}
}

// channel returns the confirm-mode channel, opening one if there is none. The caller holds p.mu.
func (p *Publisher) channel(ctx context.Context) (*amqp.Channel, error) {
	if p.ch != nil && !p.ch.IsClosed() {
		return p.ch, nil
	}
	ch, err := p.conn.Channel(ctx)
	if err != nil {
		return nil, err
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("enable confirms: %w", err)
	}
	p.ch = ch
	return ch, nil
}

// drop discards a channel that errored so the next call opens a fresh one. The caller holds p.mu.
func (p *Publisher) drop() {
	if p.ch != nil {
		_ = p.ch.Close()
		p.ch = nil
	}
}

// Publish implements ports.Publisher.
func (p *Publisher) Publish(ctx context.Context, c domain.Channel, ids []int64) ([]bool, error) {
	confirmed := make([]bool, len(ids))
	if len(ids) == 0 {
		return confirmed, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	ch, err := p.channel(ctx)
	if err != nil {
		return confirmed, err
	}

	// Each message is confirmed as a whole, so its IDs share one outcome.
	type sent struct {
		first, end int
		dc         *amqp.DeferredConfirmation
	}
	var pending []sent
	var errs []error
	for first := 0; first < len(ids); first += p.messageSize {
		end := min(first+p.messageSize, len(ids))
		dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", QueueName(c), false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         encodeIDs(ids[first:end]),
			Headers:      amqp.Table{EnqueuedHeader: time.Now().UnixNano()},
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("publish %d ids: %w", end-first, err))
			break
		}
		pending = append(pending, sent{first, end, dc})
	}

	waitCtx, cancel := context.WithTimeout(ctx, p.confirmTimeout)
	defer cancel()
	for _, m := range pending {
		acked, err := m.dc.WaitContext(waitCtx)
		if err != nil {
			errs = append(errs, fmt.Errorf("confirm %d ids: %w", m.end-m.first, err))
			break
		}
		if !acked {
			errs = append(errs, fmt.Errorf("broker nacked a message of %d ids", m.end-m.first))
			continue
		}
		for i := m.first; i < m.end; i++ {
			confirmed[i] = true
		}
	}

	if len(errs) > 0 {
		p.drop()
	}
	return confirmed, errors.Join(errs...)
}

// Stop closes the publisher's channel.
func (p *Publisher) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.drop()
}
