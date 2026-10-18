// Package rabbitmq is the RabbitMQ adapter.
package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/mansonxasthur/notifier/internal/lifecycle"
)

// Conn is a lifecycle component around an AMQP connection. It redials on demand, so a
// broker restart needs no process restart: the next Channel call after the connection
// dropped dials again.
type Conn struct {
	url string

	mu   sync.Mutex
	conn *amqp.Connection
}

func New(url string) *Conn { return &Conn{url: url} }

func (c *Conn) Name() string { return "rabbitmq" }

func (c *Conn) Start(ctx context.Context) error {
	return lifecycle.Retry(ctx, 10, time.Second, func(ctx context.Context) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.dial(ctx)
	})
}

// dial replaces the connection. The caller holds c.mu.
func (c *Conn) dial(ctx context.Context) error {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	conn, err := amqp.DialConfig(c.url, amqp.Config{
		Heartbeat: 10 * time.Second,
		Dial: func(network, addr string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, addr)
		},
	})
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	c.conn = conn
	return nil
}

func (c *Conn) Stop(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	// A connection the broker already dropped is not a shutdown failure.
	if err := c.conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
		return err
	}
	return nil
}

// Ping reports whether the connection is still open.
func (c *Conn) Ping(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil || c.conn.IsClosed() {
		return errors.New("connection closed")
	}
	return nil
}

// Channel opens a channel with the queues declared, redialing first if the connection is
// closed. Callers drop a channel that errors and ask for a new one.
func (c *Conn) Channel(ctx context.Context) (*amqp.Channel, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil || c.conn.IsClosed() {
		if err := c.dial(ctx); err != nil {
			return nil, err
		}
	}
	ch, err := c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open channel: %w", err)
	}
	if err := declareQueues(ch); err != nil {
		_ = ch.Close()
		return nil, err
	}
	return ch, nil
}
