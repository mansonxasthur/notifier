package redis_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/mansonxasthur/notifier/internal/adapters/redis"
	"github.com/mansonxasthur/notifier/internal/adapters/redis/redistest"
	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

func client(t *testing.T, url string) *goredis.Client {
	t.Helper()
	opts, err := goredis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	c := goredis.NewClient(opts)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func take(t *testing.T, l *redis.Limiter, n int) ports.Decision {
	t.Helper()
	d, err := l.Allow(context.Background(), n)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestLimiter(t *testing.T) {
	url := redistest.Start(t)
	rdb := client(t, url)
	ctx := context.Background()

	t.Run("takes the burst then refuses", func(t *testing.T) {
		l := redis.NewLimiter(rdb, "rl:burst", domain.BucketPolicy{Rate: 0.5, Burst: 10})
		if d, err := l.Allow(ctx, 10); err != nil || !d.Allowed {
			t.Fatalf("whole burst = %+v, %v", d, err)
		}
		d, err := l.Allow(ctx, 1)
		if err != nil || d.Allowed {
			t.Fatalf("empty bucket = %+v, %v; want refused", d, err)
		}
		// One token at 0.5/s is about 2s away.
		if d.RetryAfter < time.Second || d.RetryAfter > 3*time.Second {
			t.Fatalf("RetryAfter = %v, want about 2s", d.RetryAfter)
		}
	})

	t.Run("a refused charge takes nothing", func(t *testing.T) {
		l := redis.NewLimiter(rdb, "rl:allornothing", domain.BucketPolicy{Rate: 0.001, Burst: 10})
		take(t, l, 8)
		if d, _ := l.Allow(ctx, 5); d.Allowed {
			t.Fatal("took 5 with 2 left")
		}
		if d, _ := l.Allow(ctx, 2); !d.Allowed {
			t.Fatal("a refused charge consumed tokens")
		}
	})

	t.Run("refills over time", func(t *testing.T) {
		l := redis.NewLimiter(rdb, "rl:refill", domain.BucketPolicy{Rate: 100, Burst: 5})
		take(t, l, 5)
		d, _ := l.Allow(ctx, 5)
		if d.Allowed {
			t.Fatal("bucket was not empty")
		}
		time.Sleep(d.RetryAfter + 20*time.Millisecond)
		if d, err := l.Allow(ctx, 5); err != nil || !d.Allowed {
			t.Fatalf("after the returned wait = %+v, %v", d, err)
		}
	})

	t.Run("free and oversized charges", func(t *testing.T) {
		l := redis.NewLimiter(rdb, "rl:edge", domain.BucketPolicy{Rate: 1, Burst: 5})
		if d, err := l.Allow(ctx, 0); err != nil || !d.Allowed {
			t.Fatalf("zero charge = %+v, %v", d, err)
		}
		if d, err := l.Allow(ctx, 6); err != nil || d.Allowed || d.RetryAfter != domain.MaxWait {
			t.Fatalf("charge above the burst = %+v, %v", d, err)
		}
	})

	t.Run("a refused charge writes nothing", func(t *testing.T) {
		l := redis.NewLimiter(rdb, "rl:nowrite", domain.BucketPolicy{Rate: 0.001, Burst: 5})
		take(t, l, 5)
		before := rdb.HGetAll(ctx, "rl:nowrite").Val()
		if d := take(t, l, 1); d.Allowed {
			t.Fatal("took from an empty bucket")
		}
		after := rdb.HGetAll(ctx, "rl:nowrite").Val()
		if before["tokens"] != after["tokens"] || before["ts"] != after["ts"] {
			t.Fatalf("a refusal changed the stored bucket: %v -> %v", before, after)
		}
	})

	t.Run("a huge wait is capped, not overflowed", func(t *testing.T) {
		l := redis.NewLimiter(rdb, "rl:huge", domain.BucketPolicy{Rate: 1e-15, Burst: 2})
		take(t, l, 2)
		if d := take(t, l, 1); d.Allowed || d.RetryAfter != domain.MaxWait {
			t.Fatalf("Decision = %+v; want refused with MaxWait", d)
		}
	})

	t.Run("instances share one budget", func(t *testing.T) {
		policy := domain.BucketPolicy{Rate: 0.001, Burst: 100}
		var allowed atomic.Int64
		var wg sync.WaitGroup
		for i := range 4 {
			l := redis.NewLimiter(client(t, url), "rl:shared", policy)
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 50 {
					if d, err := l.Allow(ctx, 1); err != nil {
						t.Errorf("instance %d: %v", i, err)
						return
					} else if d.Allowed {
						allowed.Add(1)
					}
				}
			}()
		}
		wg.Wait()
		if got := allowed.Load(); got != 100 {
			t.Fatalf("4 instances took %d tokens from a burst of 100", got)
		}
	})

	t.Run("an idle bucket expires", func(t *testing.T) {
		l := redis.NewLimiter(rdb, "rl:expire", domain.BucketPolicy{Rate: 1000, Burst: 5})
		take(t, l, 1)
		ttl, err := rdb.PTTL(ctx, "rl:expire").Result()
		if err != nil || ttl <= 0 {
			t.Fatalf("PTTL = %v, %v; want a positive expiry", ttl, err)
		}
	})
}

func TestLimiterReportsUnreachableRedis(t *testing.T) {
	rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: 200 * time.Millisecond})
	t.Cleanup(func() { _ = rdb.Close() })
	l := redis.NewLimiter(rdb, "rl:down", domain.BucketPolicy{Rate: 1, Burst: 1})
	if d, err := l.Allow(context.Background(), 1); err == nil {
		t.Fatalf("Allow = %+v, nil; want an error so the caller can fall back", d)
	}
}
