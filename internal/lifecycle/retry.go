package lifecycle

import (
	"context"
	"fmt"
	"time"
)

// Retry calls fn until it succeeds, attempts run out, or ctx ends. Dependencies in
// compose may still be booting when a process starts, so a connect gets a few tries.
func Retry(ctx context.Context, attempts int, delay time.Duration, fn func(ctx context.Context) error) error {
	attempts = max(attempts, 1)
	var err error
	for i := 1; i <= attempts; i++ {
		if err = fn(ctx); err == nil {
			return nil
		}
		if i == attempts {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w (last error: %w)", ctx.Err(), err)
		case <-time.After(delay):
		}
	}
	return fmt.Errorf("after %d attempts: %w", attempts, err)
}
