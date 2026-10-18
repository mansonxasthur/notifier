package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

var _ ports.InboxStore = (*InboxStore)(nil)

// InboxStore implements the sender's side of the inbox and the janitor's stuck-row sweep.
type InboxStore struct {
	db *Pool
}

func NewInboxStore(db *Pool) *InboxStore { return &InboxStore{db: db} }

const insertInbox = `
INSERT INTO inbox (delivery_id, state, provider, claim_count)
VALUES ($1, $2, $3, 1)
ON CONFLICT (delivery_id) DO NOTHING`

const lockInbox = `SELECT state, claim_count FROM inbox WHERE delivery_id = $1 FOR UPDATE`

const reclaimInbox = `
UPDATE inbox SET state = $2, claim_count = claim_count + 1, last_try = now()
WHERE delivery_id = $1`

const deadInbox = `
UPDATE inbox SET state = $2, failure_reason = $3, last_try = now()
WHERE delivery_id = $1`

const failDelivery = `UPDATE deliveries SET state = $2, updated_at = now() WHERE id = $1 AND state = $3`

// Claim implements ports.InboxStore.
func (s *InboxStore) Claim(ctx context.Context, deliveryID int64, provider string, maxClaims int) (domain.InboxClaimResult, error) {
	tx, err := s.db.Pool().Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("claim inbox: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, insertInbox, deliveryID, domain.InboxProcessing, provider)
	if err != nil {
		return 0, fmt.Errorf("claim inbox: %w", err)
	}
	result := domain.InboxClaimWon
	if tag.RowsAffected() == 0 {
		result, err = claimExisting(ctx, tx, deliveryID, maxClaims)
		if err != nil {
			return 0, fmt.Errorf("claim inbox: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("claim inbox: %w", err)
	}
	return result, nil
}

// claimExisting handles a message whose inbox row already exists. Only a row waiting for a
// retry can be claimed again; the row lock makes concurrent senders take turns.
func claimExisting(ctx context.Context, tx pgx.Tx, deliveryID int64, maxClaims int) (domain.InboxClaimResult, error) {
	var state domain.InboxState
	var claims int
	if err := tx.QueryRow(ctx, lockInbox, deliveryID).Scan(&state, &claims); err != nil {
		return 0, err
	}
	if state != domain.InboxWaitingRetry {
		return domain.InboxClaimDuplicate, nil
	}
	if claims >= maxClaims {
		if _, err := tx.Exec(ctx, deadInbox, deliveryID, domain.InboxDead, domain.ReasonMaxClaimsExceeded); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(ctx, failDelivery, deliveryID, domain.DeliveryFailed, domain.DeliveryPending); err != nil {
			return 0, err
		}
		return domain.InboxClaimDead, nil
	}
	if _, err := tx.Exec(ctx, reclaimInbox, deliveryID, domain.InboxProcessing); err != nil {
		return 0, err
	}
	return domain.InboxClaimWon, nil
}

const listStuckInbox = `
SELECT delivery_id, provider, claim_count FROM inbox
WHERE provider = $1 AND state = $2 AND last_try < now() - make_interval(secs => $3)
ORDER BY last_try
LIMIT $4`

// ListStuck implements ports.InboxStore.
func (s *InboxStore) ListStuck(ctx context.Context, provider string, olderThan time.Duration, limit int) ([]domain.StuckInbox, error) {
	rows, err := s.db.Pool().Query(ctx, listStuckInbox, provider, domain.InboxProcessing, olderThan.Seconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("list stuck inbox: %w", err)
	}
	stuck, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (domain.StuckInbox, error) {
		var s domain.StuckInbox
		err := r.Scan(&s.DeliveryID, &s.Provider, &s.ClaimCount)
		return s, err
	})
	if err != nil {
		return nil, fmt.Errorf("list stuck inbox: %w", err)
	}
	return stuck, nil
}

const requeueInbox = `
UPDATE inbox SET state = $4, next_attempt_at = $3, last_try = now()
WHERE delivery_id = $1 AND state = $5 AND claim_count = $2`

// Requeue implements ports.InboxStore.
func (s *InboxStore) Requeue(ctx context.Context, st domain.StuckInbox, next time.Time) (bool, error) {
	tag, err := s.db.Pool().Exec(ctx, requeueInbox, st.DeliveryID, st.ClaimCount, next,
		domain.InboxWaitingRetry, domain.InboxProcessing)
	if err != nil {
		return false, fmt.Errorf("requeue inbox: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

const acceptInbox = `
WITH done AS (
  UPDATE inbox SET state = $3, last_try = now()
  WHERE delivery_id = $1 AND state = $4 AND claim_count = $2
  RETURNING delivery_id
)
UPDATE deliveries d SET state = $5, updated_at = now()
FROM done WHERE d.id = done.delivery_id AND d.state = $6`

// Accept implements ports.InboxStore.
func (s *InboxStore) Accept(ctx context.Context, st domain.StuckInbox) (bool, error) {
	tag, err := s.db.Pool().Exec(ctx, acceptInbox, st.DeliveryID, st.ClaimCount,
		domain.InboxDone, domain.InboxProcessing, domain.DeliveryAccepted, domain.DeliveryPending)
	if err != nil {
		return false, fmt.Errorf("accept inbox: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
