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

const (
	// requeueDelay slows redelivery of a failing message, so an outage is not a hot loop.
	requeueDelay = time.Second
	// handlerTimeout bounds one handler call, since it no longer sees the consumer's cancellation.
	handlerTimeout = 30 * time.Second
)

// Consumer reads delivery IDs with manual acknowledgements.
type Consumer struct {
	conn     *Conn
	prefetch int
	log      *slog.Logger
	rec      ports.Recorder
}

// NewConsumer holds at most prefetch unacknowledged messages per queue.
func NewConsumer(conn *Conn, prefetch int, log *slog.Logger) *Consumer {
	return &Consumer{conn: conn, prefetch: prefetch, log: log, rec: ports.NopRecorder{}}
}

// WithRecorder makes the consumer report how long each message waited in its queue.
func (c *Consumer) WithRecorder(r ports.Recorder) *Consumer {
	c.rec = r
	return c
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
	c.recordWait(ch, msg)
	// In-flight messages finish when the consumer stops, so the handler is not cancelled with it.
	hctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), handlerTimeout)
	defer cancel()
	if err := h(hctx, id); err != nil {
		c.log.Warn("handler failed, requeueing", "channel", ch, "delivery_id", id, "error", err)
		select {
		case <-ctx.Done():
		case <-time.After(requeueDelay):
		}
		_ = msg.Nack(false, true)
		return
	}
	_ = msg.Ack(false)
}

// recordWait reports the time since the publisher sent msg. A redelivered message is skipped:
// its wait includes the failure that sent it back, which is not queueing.
func (c *Consumer) recordWait(ch domain.Channel, msg amqp.Delivery) {
	if msg.Redelivered {
		return
	}
	ns, ok := msg.Headers[EnqueuedHeader].(int64)
	if !ok {
		return
	}
	c.rec.QueueWait(ch, time.Since(time.Unix(0, ns)))
}
