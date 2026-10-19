package redis_test

import (
	"context"
	"testing"

	"github.com/mansonxasthur/notifier/internal/adapters/redis"
	"github.com/mansonxasthur/notifier/internal/adapters/redis/redistest"
	"github.com/mansonxasthur/notifier/internal/domain"
)

// BenchmarkLimiterAllow is one Allow call: a single round trip to a Redis in a container.
func BenchmarkLimiterAllow(b *testing.B) {
	rdb := client(b, redistest.Start(b))
	l := redis.NewLimiter(rdb, "rl:bench", domain.BucketPolicy{Rate: 1e9, Burst: 1e9})
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if d, err := l.Allow(ctx, 1); err != nil || !d.Allowed {
			b.Fatalf("Allow = %+v, %v", d, err)
		}
	}
}
