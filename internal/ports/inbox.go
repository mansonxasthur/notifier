package ports

import (
	"context"
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
)

// InboxStore is the sender's view of the inbox, and the janitor's for stuck rows.
type InboxStore interface {
	// Claim inserts or finds the inbox row for the delivery and tries to take it for this
	// sender. The first claim inserts the row as PROCESSING with the given provider; a row
	// in WAITING_RETRY can be claimed again; anything else is a duplicate. A claim that
	// would pass maxClaims ends the row DEAD and the delivery FAILED.
	Claim(ctx context.Context, deliveryID int64, provider string, maxClaims int) (domain.InboxClaimResult, error)
	// ListStuck returns up to limit rows of the provider left in PROCESSING longer than olderThan.
	ListStuck(ctx context.Context, provider string, olderThan time.Duration, limit int) ([]domain.StuckInbox, error)
	// Requeue moves a stuck row to WAITING_RETRY, due at next. False means the row changed since it was listed.
	Requeue(ctx context.Context, s domain.StuckInbox, next time.Time) (bool, error)
	// Accept ends a stuck row DONE and marks its delivery ACCEPTED, because the provider confirmed it.
	// False means the row changed since it was listed.
	Accept(ctx context.Context, s domain.StuckInbox) (bool, error)
}
