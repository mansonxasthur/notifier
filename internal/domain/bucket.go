package domain

import (
	"errors"
	"math"
	"time"
)

// BucketPolicy tunes a token bucket.
type BucketPolicy struct {
	// Rate is how many tokens are added per second.
	Rate float64
	// Burst is the most tokens the bucket holds, and the largest charge that can ever succeed.
	Burst int
}

// Validate reports a policy a bucket cannot run on.
func (p BucketPolicy) Validate() error {
	if !(p.Rate > 0) || math.IsInf(p.Rate, 0) {
		return errors.New("rate must be a positive number")
	}
	if p.Rate < 1/MaxWait.Seconds() {
		return errors.New("rate is too low: one token must refill within MaxWait")
	}
	if p.Burst < 1 {
		return errors.New("burst must be at least 1")
	}
	if float64(p.Burst)/p.Rate > MaxWait.Seconds() {
		return errors.New("burst is too high for the rate: an empty bucket must refill within MaxWait")
	}
	return nil
}

// Fits reports whether a charge of n can ever succeed, however long the caller waits.
func (p BucketPolicy) Fits(n int) bool { return n <= p.Burst }

// MaxWait caps the wait Take reports. It is also the wait for a charge that exceeds the burst,
// which no wait can satisfy, so a caller that skips Fits defers for a long time, not for a
// short one it would retry in a loop.
const MaxWait = 24 * time.Hour

// Bucket is a token bucket that starts full. It is not safe for concurrent use: callers lock.
type Bucket struct {
	policy BucketPolicy
	tokens float64
	last   time.Time
}

// NewBucket returns a full bucket whose clock starts at now. The policy must be valid.
func NewBucket(p BucketPolicy, now time.Time) *Bucket {
	return &Bucket{policy: p, tokens: float64(p.Burst), last: now}
}

// Take charges n tokens, all or nothing. When the bucket cannot cover n it takes nothing and
// returns how long until it could, never more than MaxWait. A charge of zero or less is free. A
// charge above the burst is refused with MaxWait; callers check Fits first.
func (b *Bucket) Take(n int, now time.Time) (ok bool, wait time.Duration) {
	if n <= 0 {
		return true, 0
	}
	if !b.policy.Fits(n) {
		return false, MaxWait
	}
	b.refill(now)
	need := float64(n)
	if b.tokens >= need {
		b.tokens -= need
		return true, 0
	}
	missing := need - b.tokens
	ns := math.Ceil(missing/b.policy.Rate*float64(time.Second)) + 1 // +1ns: rounding must not leave the retry a hair short
	if ns >= float64(MaxWait) {
		return false, MaxWait
	}
	return false, time.Duration(ns)
}

// refill adds the tokens earned since the last call. A clock that goes backwards earns nothing.
func (b *Bucket) refill(now time.Time) {
	elapsed := now.Sub(b.last)
	if elapsed <= 0 {
		return
	}
	b.last = now
	b.tokens = math.Min(float64(b.policy.Burst), b.tokens+elapsed.Seconds()*b.policy.Rate)
}
