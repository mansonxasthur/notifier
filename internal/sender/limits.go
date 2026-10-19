package sender

import (
	"sync"
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
)

// ProviderLimits holds one token bucket per provider, local to this sender, so a spike cannot
// flood a provider. The limit is per sender process: with N senders a provider can see up to
// N times its configured rate. A provider with no bucket is not limited.
type ProviderLimits struct {
	now func() time.Time

	mu      sync.Mutex
	buckets map[string]*domain.Bucket
}

// NewProviderLimits returns full buckets for the given policies. now is the clock, so tests can
// move it. The policies must be valid.
func NewProviderLimits(policies map[string]domain.BucketPolicy, now func() time.Time) *ProviderLimits {
	l := &ProviderLimits{now: now, buckets: make(map[string]*domain.Bucket, len(policies))}
	for name, p := range policies {
		l.buckets[name] = domain.NewBucket(p, now())
	}
	return l
}

// Take charges one call to provider. When the bucket is empty it takes nothing and returns how
// long until a token is free. A nil ProviderLimits, or a provider without a bucket, never refuses.
func (l *ProviderLimits) Take(provider string) (ok bool, wait time.Duration) {
	if l == nil {
		return true, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	b, limited := l.buckets[provider]
	if !limited {
		return true, 0
	}
	return b.Take(1, l.now())
}
