// Package ratelimit holds the in-process limiter and the wrapper that falls back to it.
package ratelimit

import (
	"context"
	"sync"
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

// Local is a token bucket held in this process's memory.
type Local struct {
	mu     sync.Mutex
	bucket *domain.Bucket
	now    func() time.Time
}

var _ ports.Limiter = (*Local)(nil)

// NewLocal returns a full local bucket. The policy must be valid.
func NewLocal(policy domain.BucketPolicy, now func() time.Time) *Local {
	return &Local{bucket: domain.NewBucket(policy, now()), now: now}
}

// Allow charges n tokens, all or nothing. It never fails.
func (l *Local) Allow(_ context.Context, n int) (ports.Decision, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	ok, wait := l.bucket.Take(n, l.now())
	return ports.Decision{Allowed: ok, RetryAfter: wait}, nil
}
