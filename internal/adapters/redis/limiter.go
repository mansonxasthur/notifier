package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

// bucketScript refills and takes in one step, so no instance sees a half-updated bucket. It
// reads the Redis server clock, so instances with skewed clocks agree. The key expires once the
// bucket would have refilled, so an idle system leaves no state. A refused charge writes nothing:
// the next call recomputes the same refill from the stored state, so refusals under overload cost
// Redis no writes and add no rounding drift. It returns {allowed, wait in µs}.
var bucketScript = goredis.NewScript(`
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local n = tonumber(ARGV[3])
local h = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(h[1])
local ts = tonumber(h[2])
if tokens == nil or ts == nil then
  tokens = burst
  ts = now
end
if now > ts then
  tokens = math.min(burst, tokens + (now - ts) * rate / 1000000)
  ts = now
end
if tokens < n then
  -- Capped at a day in µs here too: Redis turns a number past int64 into garbage.
  return {0, math.min(math.ceil((n - tokens) / rate * 1000000), 86400000000)}
end
tokens = tokens - n
redis.call('HSET', KEYS[1], 'tokens', string.format('%.6f', tokens), 'ts', string.format('%d', ts))
redis.call('PEXPIRE', KEYS[1], math.min(math.ceil(burst / rate * 1000) + 1000, 86400000))
return {1, 0}
`)

// Limiter is a token bucket shared by every process that uses the same key.
type Limiter struct {
	rdb    goredis.Scripter
	key    string
	policy domain.BucketPolicy
}

var _ ports.Limiter = (*Limiter)(nil)

// NewLimiter returns a limiter over the bucket stored at key. The policy must be valid.
func NewLimiter(rdb goredis.Scripter, key string, policy domain.BucketPolicy) *Limiter {
	return &Limiter{rdb: rdb, key: key, policy: policy}
}

// Allow charges n tokens. A charge of zero or less is free, and one above the burst is refused
// without a round trip, because no wait can satisfy it.
func (l *Limiter) Allow(ctx context.Context, n int) (ports.Decision, error) {
	if n <= 0 {
		return ports.Decision{Allowed: true}, nil
	}
	if !l.policy.Fits(n) {
		return ports.Decision{RetryAfter: domain.MaxWait}, nil
	}
	res, err := bucketScript.Run(ctx, l.rdb, []string{l.key}, l.policy.Rate, l.policy.Burst, n).Int64Slice()
	if err != nil {
		return ports.Decision{}, fmt.Errorf("take from bucket %s: %w", l.key, err)
	}
	if len(res) != 2 {
		return ports.Decision{}, fmt.Errorf("take from bucket %s: script returned %d values, want 2", l.key, len(res))
	}
	if res[0] == 1 {
		return ports.Decision{Allowed: true}, nil
	}
	// Cap before converting: a huge wait in microseconds would overflow a Duration.
	if res[1] >= int64(domain.MaxWait/time.Microsecond) {
		return ports.Decision{RetryAfter: domain.MaxWait}, nil
	}
	return ports.Decision{RetryAfter: time.Duration(res[1]) * time.Microsecond}, nil
}
