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
	Claim(ctx context.Context, deliveryID int64, provider string, maxClaims int) (domain.InboxClaim, error)
	// ListStuck returns up to limit rows of the provider left in PROCESSING longer than olderThan.
	ListStuck(ctx context.Context, provider string, olderThan time.Duration, limit int) ([]domain.StuckInbox, error)
	// Requeue moves a stuck row to WAITING_RETRY, due at next. False means the row changed since it was listed.
	Requeue(ctx context.Context, s domain.StuckInbox, next time.Time) (bool, error)
	// Complete ends a row this sender holds DONE and moves its delivery to ACCEPTED, because
	// the provider took the message. claimCount is the claim's fencing token. False means the
	// claim is stale: the row was reclaimed since. A delivery that already left PENDING (an
	// early webhook) keeps its state.
	Complete(ctx context.Context, deliveryID int64, claimCount int) (bool, error)
	// Fail ends a row this sender holds DEAD and its delivery FAILED with the reason, for a
	// failure that no retry can fix. False means the claim is stale.
	Fail(ctx context.Context, deliveryID int64, claimCount int, class domain.FailureClass, reason domain.FailureReason) (bool, error)
	// Park moves a row this sender holds to WAITING_RETRY after a failed attempt: it records
	// the failure class, adds one to retries, and sets next as the due time. False means the claim is stale.
	Park(ctx context.Context, deliveryID int64, claimCount int, class domain.FailureClass, next time.Time) (bool, error)
	// Accept ends a stuck row DONE and marks its delivery ACCEPTED, because the provider confirmed it.
	// False means the row changed since it was listed.
	Accept(ctx context.Context, s domain.StuckInbox) (bool, error)
}
