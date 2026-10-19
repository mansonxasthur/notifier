// Package redis is the Redis adapter.
package redis

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/mansonxasthur/notifier/internal/lifecycle"
)

// Client is a lifecycle component around a Redis client.
type Client struct {
	url string
	// client is set by Start and read by request goroutines, so it is atomic.
	client atomic.Pointer[goredis.Client]
}

func New(url string) *Client { return &Client{url: url} }

func (c *Client) Name() string { return "redis" }

func (c *Client) Start(ctx context.Context) error {
	opts, err := goredis.ParseURL(c.url)
	if err != nil {
		return fmt.Errorf("parse url: %w", err)
	}
	client := goredis.NewClient(opts)
	err = lifecycle.Retry(ctx, 10, time.Second, func(ctx context.Context) error {
		return client.Ping(ctx).Err()
	})
	if err != nil {
		_ = client.Close()
		return err
	}
	c.client.Store(client)
	return nil
}

func (c *Client) Stop(context.Context) error {
	client := c.client.Load()
	if client == nil {
		return nil
	}
	return client.Close()
}

func (c *Client) Ping(ctx context.Context) error {
	client := c.client.Load()
	if client == nil {
		return errors.New("not connected")
	}
	return client.Ping(ctx).Err()
}

// Client exposes the underlying client, which is nil until Start has run. Code that is built
// before the lifecycle starts uses Scripter instead.
func (c *Client) Client() *goredis.Client { return c.client.Load() }
