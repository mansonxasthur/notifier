package domain

import (
	"math"
	"time"
)

// Backoff is an exponential retry delay with jitter.
type Backoff struct {
	Initial    time.Duration
	Max        time.Duration
	Multiplier float64
	// Jitter is the fraction by which a delay may vary in either direction.
	Jitter float64
}

// Delay returns the wait before the next attempt after the given number of attempts so far
// (at least 1). unit is a random value in [0, 1) that picks the point inside the jitter
// band; passing it in keeps the function pure.
func (b Backoff) Delay(attempts int, unit float64) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	d := float64(b.Initial) * math.Pow(b.Multiplier, float64(attempts-1))
	d = math.Min(d, float64(b.Max))
	d *= 1 + b.Jitter*(2*unit-1)
	return time.Duration(d)
}

// RetryPolicy is how a provider is retried before the sender moves on to the next one.
type RetryPolicy struct {
	// MaxRetries is how many retries follow the first attempt on the provider.
	MaxRetries int
	Backoff    Backoff
}
