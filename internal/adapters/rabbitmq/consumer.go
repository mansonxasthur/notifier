package rabbitmq

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

var _ ports.Consumer = (*Consumer)(nil)

// reconnectDelay is the pause before consuming again after the broker dropped us.
const reconnectDelay = time.Second

// Consumer reads delivery IDs with manual acknowledgements.
type Consumer struct {
	conn     *Conn
	prefetch int
	log      *slog.Logger
}

// NewConsumer holds at most prefetch unacknowledged messages per queue.
func NewConsumer(conn *Conn, prefetch int, log *slog.Logger) *Consumer {
	return &Consumer{conn: conn, prefetch: prefetch, log: log}
}

// Consume implements ports.Consumer.
func (c *Consumer) Consume(ctx context.Context, ch domain.Channel, h ports.Handler) error {
	for ctx.Err() == nil {
		if err := c.consumeOnce(ctx, ch, h); err != nil {
			c.log.Warn("consumer stopped, reconnecting", "channel", ch, "error", err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(reconnectDelay):
		}
	}
	return nil
}

// consumeOnce consumes until ctx is done or the broker drops the channel. Handlers run in
// parallel up to the prefetch count; on return none is still running.
func (c *Consumer) consumeOnce(ctx context.Context, ch domain.Channel, h ports.Handler) error {
	amqpCh, err := c.conn.Channel(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = amqpCh.Close() }()

	if err := amqpCh.Qos(c.prefetch, 0, false); err != nil {
		return err
	}
	msgs, err := amqpCh.Consume(QueueName(ch), "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	defer wg.Wait()
	slots := make(chan struct{}, c.prefetch)

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-msgs:
			if !ok {
				return amqp.ErrClosed
			}
			slots <- struct{}{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-slots }()
				c.handle(ctx, ch, h, msg)
			}()
		}
	}
}

func (c *Consumer) handle(ctx context.Context, ch domain.Channel, h ports.Handler, msg amqp.Delivery) {
	id, err := strconv.ParseInt(string(msg.Body), 10, 64)
	if err != nil {
		// A body that is not a delivery ID can never succeed; requeueing would loop forever.
		c.log.Error("dropping malformed message", "channel", ch, "error", err)
		_ = msg.Reject(false)
		return
	}
	if err := h(ctx, id); err != nil {
		c.log.Warn("handler failed, requeueing", "channel", ch, "delivery_id", id, "error", err)
		_ = msg.Nack(false, true)
		return
	}
	_ = msg.Ack(false)
}
