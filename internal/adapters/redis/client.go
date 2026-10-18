// Package redis is the Redis adapter.
package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/mansonxasthur/notifier/internal/lifecycle"
)

// Client is a lifecycle component around a Redis client.
type Client struct {
	url    string
	client *goredis.Client
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
	c.client = client
	return nil
}

func (c *Client) Stop(context.Context) error {
	if c.client == nil {
		return nil
	}
	return c.client.Close()
}

func (c *Client) Ping(ctx context.Context) error {
	if c.client == nil {
		return errors.New("not connected")
	}
	return c.client.Ping(ctx).Err()
}

// Client exposes the underlying client to the rate limiter added in Phase 6.
func (c *Client) Client() *goredis.Client { return c.client }
