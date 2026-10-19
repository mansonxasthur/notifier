package ports

import (
	"context"
	"time"
)

// Decision is a limiter's answer to one charge.
type Decision struct {
	// Allowed is true when the charge was taken.
	Allowed bool
	// RetryAfter is, when the charge was refused, how long until it would fit.
	RetryAfter time.Duration
}

// Limiter admits or refuses a charge of n deliveries, all or nothing. A refused charge takes
// nothing. An error means the limiter could not decide, not that the charge was refused.
type Limiter interface {
	Allow(ctx context.Context, n int) (Decision, error)
}
