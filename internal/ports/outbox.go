package ports

import (
	"context"
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
)

// OutboxStore is the relay's view of the outbox. Every write after Claim is fenced by the
// claim's Retries, so a relay whose row was reclaimed changes nothing and gets false.
type OutboxStore interface {
	// Claim takes up to limit due NEW rows for this relay. Rows already at maxAttempts
	// claims are ended DEAD (and their delivery FAILED) instead of being returned.
	Claim(ctx context.Context, limit, maxAttempts int) ([]domain.OutboxClaim, error)
	// MarkPublished records, in one statement, that the broker confirmed the messages of
	// these claims. It returns how many claims were still current; the rest were reclaimed
	// by someone else and are left alone.
	MarkPublished(ctx context.Context, claims []domain.OutboxClaim) (int, error)
	// Release returns the row to NEW, due again at next.
	Release(ctx context.Context, c domain.OutboxClaim, next time.Time, reason domain.FailureReason) (bool, error)
	// ReclaimStuck returns rows left in PROCESSING longer than olderThan to NEW. delay gives
	// the wait before each is due again, from its retries so far. It returns the count.
	ReclaimStuck(ctx context.Context, olderThan time.Duration, delay func(retries int) time.Duration) (int, error)
}
