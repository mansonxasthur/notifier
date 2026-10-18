package domain

import "time"

// BreakerPolicy tunes a circuit breaker.
type BreakerPolicy struct {
	// FailureThreshold is how many consecutive failures open the breaker.
	FailureThreshold int
	// OpenFor is how long an open breaker refuses calls before it lets one probe through.
	OpenFor time.Duration
}
