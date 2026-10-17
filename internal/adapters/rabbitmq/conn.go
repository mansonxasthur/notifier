// Package rabbitmq is the RabbitMQ adapter.
package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/mansonxasthur/notifier/internal/lifecycle"
)

// Conn is a lifecycle component around an AMQP connection.
type Conn struct {
	url  string
	conn *amqp.Connection
}

func New(url string) *Conn { return &Conn{url: url} }

func (c *Conn) Name() string { return "rabbitmq" }

func (c *Conn) Start(ctx context.Context) error {
	return lifecycle.Retry(ctx, 10, time.Second, func(ctx context.Context) error {
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
	})
}

func (c *Conn) Stop(context.Context) error {
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
	if c.conn == nil || c.conn.IsClosed() {
		return errors.New("connection closed")
	}
	return nil
}

// Connection exposes the underlying connection to the publisher and consumer added in Phase 3.
func (c *Conn) Connection() *amqp.Connection { return c.conn }
