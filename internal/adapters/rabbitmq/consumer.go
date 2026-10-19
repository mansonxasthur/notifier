package rabbitmq

import (
	"context"
	"errors"
	"log/slog"
	"slices"
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
	// republish carries the failed IDs of a partly handled message to a new message.
	republish ports.Publisher
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

// WithRepublisher makes a message the handler only partly finished give its unfinished IDs
// to a new message instead of going back whole. Without it the whole message is requeued.
func (c *Consumer) WithRepublisher(p ports.Publisher) *Consumer {
	c.republish = p
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
	ids, err := decodeIDs(msg.Body)
	if err != nil {
		// A body that is not delivery IDs can never succeed; requeueing would loop forever.
		c.log.Error("dropping malformed message", "channel", ch, "error", err)
		_ = msg.Reject(false)
		return
	}
	c.recordWait(ch, msg, len(ids))
	// In-flight messages finish when the consumer stops, so the handler is not cancelled with it.
	hctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), handlerTimeout)
	defer cancel()
	err = h(hctx, ids)
	if err == nil {
		_ = msg.Ack(false)
		return
	}
	if c.splitFailed(ctx, ch, ids, err) {
		_ = msg.Ack(false)
		return
	}
	c.log.Warn("handler failed, requeueing", "channel", ch, "ids", len(ids), "error", err)
	select {
	case <-ctx.Done():
	case <-time.After(requeueDelay):
	}
	_ = msg.Nack(false, true)
}

// splitFailed moves the IDs a handler reported as failed to a new message and returns true
// once the broker has confirmed it, so the original can be acknowledged without the IDs that
// were handled coming back. It returns false when the whole message should be requeued.
func (c *Consumer) splitFailed(ctx context.Context, ch domain.Channel, ids []int64, err error) bool {
	var be *ports.BatchError
	if c.republish == nil || !errors.As(err, &be) || len(be.Failed) == 0 || len(be.Failed) >= len(ids) {
		return false
	}
	c.log.Warn("handler failed for part of a message, republishing those ids",
		"channel", ch, "failed", len(be.Failed), "ids", len(ids), "error", be.Err)
	// The failed ids wait out the same delay as a requeued message, so a persistent failure is not
	// a hot loop. The handler's own context may have expired, so the republish gets a fresh one.
	select {
	case <-ctx.Done():
	case <-time.After(requeueDelay):
	}
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), handlerTimeout)
	defer cancel()
	ok, perr := c.republish.Publish(pctx, ch, be.Failed)
	if perr != nil {
		c.log.Warn("republish failed ids", "channel", ch, "error", perr)
		return false
	}
	return !slices.Contains(ok, false)
}

// recordWait reports the time since the publisher sent msg, once for each of its n IDs. A redelivered message is skipped:
// its wait includes the failure that sent it back, which is not queueing.
func (c *Consumer) recordWait(ch domain.Channel, msg amqp.Delivery, n int) {
	if msg.Redelivered {
		return
	}
	ns, ok := msg.Headers[EnqueuedHeader].(int64)
	if !ok {
		return
	}
	d := time.Since(time.Unix(0, ns))
	for range n {
		c.rec.QueueWait(ch, d)
	}
}
