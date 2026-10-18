package ratelimit

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/mansonxasthur/notifier/internal/adapters/redis"
	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newClock() *clock { return &clock{t: time.Unix(1_000_000, 0)} }

func TestLocalLimits(t *testing.T) {
	c := newClock()
	l := NewLocal(domain.BucketPolicy{Rate: 10, Burst: 20}, c.now)
	ctx := context.Background()
	if d, err := l.Allow(ctx, 20); err != nil || !d.Allowed {
		t.Fatalf("whole burst = %+v, %v", d, err)
	}
	d, _ := l.Allow(ctx, 5)
	if d.Allowed || d.RetryAfter < 500*time.Millisecond {
		t.Fatalf("empty bucket = %+v; want refused with a wait of about 500ms", d)
	}
	c.advance(d.RetryAfter)
	if d, _ := l.Allow(ctx, 5); !d.Allowed {
		t.Fatal("refused after the returned wait")
	}
}

func TestLocalIsSafeForConcurrentUse(t *testing.T) {
	l := NewLocal(domain.BucketPolicy{Rate: 0.001, Burst: 100}, newClock().now)
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				if d, _ := l.Allow(context.Background(), 1); d.Allowed {
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if got := allowed.Load(); got != 100 {
		t.Fatalf("allowed %d of a burst of 100", got)
	}
}

type fakeShared struct {
	calls    int
	err      error
	decision ports.Decision
	sawDL    bool
}

func (f *fakeShared) Allow(ctx context.Context, _ int) (ports.Decision, error) {
	f.calls++
	_, f.sawDL = ctx.Deadline()
	return f.decision, f.err
}

type logs struct{ bytes.Buffer }

func newFallback(shared ports.Limiter, local ports.Limiter, c *clock) (*Fallback, *logs) {
	var buf logs
	log := slog.New(slog.NewTextHandler(&buf, nil))
	return NewFallback(shared, local, 50*time.Millisecond, time.Second, log, c.now), &buf
}

func TestFallbackUsesSharedWhileItWorks(t *testing.T) {
	c := newClock()
	shared := &fakeShared{decision: ports.Decision{Allowed: false, RetryAfter: 3 * time.Second}}
	f, _ := newFallback(shared, NewLocal(domain.BucketPolicy{Rate: 1, Burst: 1}, c.now), c)
	d, err := f.Allow(context.Background(), 1)
	if err != nil || d != shared.decision {
		t.Fatalf("Allow = %+v, %v; want the shared answer", d, err)
	}
	if !shared.sawDL {
		t.Fatal("the shared call had no deadline")
	}
}

func TestFallbackLimitsLocallyWhenSharedFails(t *testing.T) {
	c := newClock()
	shared := &fakeShared{err: errors.New("connection refused")}
	f, buf := newFallback(shared, NewLocal(domain.BucketPolicy{Rate: 0.001, Burst: 3}, c.now), c)
	ctx := context.Background()
	for i := range 3 {
		if d, err := f.Allow(ctx, 1); err != nil || !d.Allowed {
			t.Fatalf("request %d = %+v, %v; want allowed by the local bucket", i, d, err)
		}
	}
	if d, err := f.Allow(ctx, 1); err != nil || d.Allowed {
		t.Fatalf("request over the local burst = %+v, %v; want refused", d, err)
	}
	if n := strings.Count(buf.String(), "unavailable"); n != 1 {
		t.Fatalf("outage logged %d times, want once:\n%s", n, buf.String())
	}
}

func TestFallbackSkipsSharedDuringCooldownThenRetries(t *testing.T) {
	c := newClock()
	shared := &fakeShared{err: errors.New("down")}
	f, _ := newFallback(shared, NewLocal(domain.BucketPolicy{Rate: 1000, Burst: 1000}, c.now), c)
	ctx := context.Background()
	for range 5 {
		f.Allow(ctx, 1) //nolint:errcheck,gosec // Fallback never errors; asserted elsewhere
	}
	if shared.calls != 1 {
		t.Fatalf("shared called %d times during the cooldown, want 1", shared.calls)
	}
	c.advance(time.Second)
	f.Allow(ctx, 1) //nolint:errcheck,gosec // as above
	if shared.calls != 2 {
		t.Fatalf("shared called %d times after the cooldown, want 2", shared.calls)
	}
}

func TestFallbackReturnsToSharedAndLogsRecoveryOnce(t *testing.T) {
	c := newClock()
	shared := &fakeShared{err: errors.New("down")}
	f, buf := newFallback(shared, NewLocal(domain.BucketPolicy{Rate: 1000, Burst: 1000}, c.now), c)
	ctx := context.Background()
	f.Allow(ctx, 1) //nolint:errcheck,gosec // as above

	shared.err = nil
	shared.decision = ports.Decision{Allowed: false, RetryAfter: time.Second}
	c.advance(time.Second)
	for range 3 {
		if d, _ := f.Allow(ctx, 1); d != shared.decision {
			t.Fatalf("after recovery got %+v, want the shared answer", d)
		}
	}
	if n := strings.Count(buf.String(), "recovered"); n != 1 {
		t.Fatalf("recovery logged %d times, want once:\n%s", n, buf.String())
	}

	// A second outage is logged again.
	shared.err = errors.New("down again")
	c.advance(time.Second)
	f.Allow(ctx, 1) //nolint:errcheck,gosec // as above
	if n := strings.Count(buf.String(), "unavailable"); n != 2 {
		t.Fatalf("outages logged %d times, want 2:\n%s", n, buf.String())
	}
}

func TestFallbackOverUnreachableRedis(t *testing.T) {
	rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: 100 * time.Millisecond})
	t.Cleanup(func() { _ = rdb.Close() })
	policy := domain.BucketPolicy{Rate: 0.001, Burst: 2}
	c := newClock()
	f, _ := newFallback(redis.NewLimiter(rdb, "rl:down", policy), NewLocal(policy, c.now), c)
	ctx := context.Background()
	for i := range 2 {
		if d, err := f.Allow(ctx, 1); err != nil || !d.Allowed {
			t.Fatalf("request %d = %+v, %v; want allowed locally", i, d, err)
		}
	}
	if d, err := f.Allow(ctx, 1); err != nil || d.Allowed {
		t.Fatalf("request over the burst = %+v, %v; want refused locally", d, err)
	}
}

func TestFallbackIgnoresACallerThatGaveUp(t *testing.T) {
	c := newClock()
	shared := &fakeShared{err: context.Canceled}
	f, buf := newFallback(shared, NewLocal(domain.BucketPolicy{Rate: 1, Burst: 1}, c.now), c)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.Allow(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want the caller's cancellation", err)
	}
	if strings.Contains(buf.String(), "unavailable") {
		t.Fatal("a cancelled caller was logged as a shared limiter outage")
	}
	// The shared limiter is still tried on the next request: no cooldown started.
	shared.err = nil
	shared.decision = ports.Decision{Allowed: true}
	if d, err := f.Allow(context.Background(), 1); err != nil || !d.Allowed || shared.calls != 2 {
		t.Fatalf("next request = %+v, %v after %d shared calls; want the shared limiter used", d, err, shared.calls)
	}
}

func TestFallbackAdmitsOneProbeAfterTheCooldown(t *testing.T) {
	c := newClock()
	shared := &fakeShared{err: errors.New("down")}
	f, _ := newFallback(shared, NewLocal(domain.BucketPolicy{Rate: 1000, Burst: 1000}, c.now), c)
	ctx := context.Background()
	f.Allow(ctx, 1) //nolint:errcheck,gosec // opens the cooldown
	c.advance(time.Second)
	if !f.claimShared() {
		t.Fatal("the first caller after the cooldown was not admitted")
	}
	if f.claimShared() {
		t.Fatal("a second caller was admitted while the probe was in flight")
	}
}
