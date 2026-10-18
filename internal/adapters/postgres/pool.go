// Package postgres is the Postgres adapter.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mansonxasthur/notifier/internal/lifecycle"
)

// Pool is a lifecycle component around a pgx connection pool.
type Pool struct {
	url  string
	pool *pgxpool.Pool
}

func New(url string) *Pool { return &Pool{url: url} }

func (p *Pool) Name() string { return "postgres" }

// Start connects, retrying while the database boots, and verifies with a ping.
func (p *Pool) Start(ctx context.Context) error {
	return lifecycle.Retry(ctx, 10, time.Second, func(ctx context.Context) error {
		pool, err := pgxpool.New(ctx, p.url)
		if err != nil {
			return fmt.Errorf("open pool: %w", err)
		}
		if err := pool.Ping(ctx); err != nil {
			pool.Close()
			return fmt.Errorf("ping: %w", err)
		}
		p.pool = pool
		return nil
	})
}

func (p *Pool) Stop(context.Context) error {
	if p.pool != nil {
		p.pool.Close()
	}
	return nil
}

func (p *Pool) Ping(ctx context.Context) error {
	if p.pool == nil {
		return errors.New("not connected")
	}
	return p.pool.Ping(ctx)
}

// Pool exposes the underlying pool to repositories added in later phases.
func (p *Pool) Pool() *pgxpool.Pool { return p.pool }
