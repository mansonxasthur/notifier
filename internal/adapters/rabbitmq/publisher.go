package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"strconv"
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

	mu sync.Mutex
	ch *amqp.Channel
}

// NewPublisher waits at most confirmTimeout for the broker to confirm a batch.
func NewPublisher(conn *Conn, confirmTimeout time.Duration) *Publisher {
	return &Publisher{conn: conn, confirmTimeout: confirmTimeout}
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

	pending := make([]*amqp.DeferredConfirmation, len(ids))
	var errs []error
	for i, id := range ids {
		dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", QueueName(c), false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(strconv.FormatInt(id, 10)),
			Headers:      amqp.Table{EnqueuedHeader: time.Now().UnixNano()},
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("publish %d: %w", id, err))
			break
		}
		pending[i] = dc
	}

	waitCtx, cancel := context.WithTimeout(ctx, p.confirmTimeout)
	defer cancel()
	for i, dc := range pending {
		if dc == nil {
			continue
		}
		acked, err := dc.WaitContext(waitCtx)
		if err != nil {
			errs = append(errs, fmt.Errorf("confirm %d: %w", ids[i], err))
			break
		}
		confirmed[i] = acked
		if !acked {
			errs = append(errs, fmt.Errorf("broker nacked %d", ids[i]))
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
