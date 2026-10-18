package postgres

import (
	"context"
	"errors"
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

const lockInbox = `
SELECT state, claim_count, retries, provider, coalesce(next_attempt_at <= now(), true)
FROM inbox WHERE delivery_id = $1 FOR UPDATE`

const reclaimInbox = `
UPDATE inbox SET state = $2, claim_count = claim_count + 1, last_try = now()
WHERE delivery_id = $1`

const deadInbox = `
UPDATE inbox SET state = $2, failure_reason = $3, last_try = now()
WHERE delivery_id = $1`

const failDelivery = `
UPDATE deliveries SET state = $2, failure_reason = $4, updated_at = now()
WHERE id = $1 AND state = $3`

// Claim implements ports.InboxStore.
func (s *InboxStore) Claim(ctx context.Context, deliveryID int64, provider string, maxClaims int) (domain.InboxClaim, error) {
	tx, err := s.db.Pool().Begin(ctx)
	if err != nil {
		return domain.InboxClaim{}, fmt.Errorf("claim inbox: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, insertInbox, deliveryID, domain.InboxProcessing, provider)
	if err != nil {
		return domain.InboxClaim{}, fmt.Errorf("claim inbox: %w", err)
	}
	claim := domain.InboxClaim{Result: domain.InboxClaimWon, ClaimCount: 1, Provider: provider}
	if tag.RowsAffected() == 0 {
		claim, err = claimExisting(ctx, tx, deliveryID, maxClaims)
		if err != nil {
			return domain.InboxClaim{}, fmt.Errorf("claim inbox: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.InboxClaim{}, fmt.Errorf("claim inbox: %w", err)
	}
	return claim, nil
}

// claimExisting handles a message whose inbox row already exists. Only a row waiting for a
// retry that is due can be claimed again; the row lock makes concurrent senders take turns.
func claimExisting(ctx context.Context, tx pgx.Tx, deliveryID int64, maxClaims int) (domain.InboxClaim, error) {
	var state domain.InboxState
	var claims, retries int
	var provider string
	var due bool
	if err := tx.QueryRow(ctx, lockInbox, deliveryID).Scan(&state, &claims, &retries, &provider, &due); err != nil {
		return domain.InboxClaim{}, err
	}
	if state != domain.InboxWaitingRetry || !due {
		return domain.InboxClaim{Result: domain.InboxClaimDuplicate}, nil
	}
	won, err := reclaim(ctx, tx, deliveryID, claims, maxClaims)
	if err != nil {
		return domain.InboxClaim{}, err
	}
	if !won {
		return domain.InboxClaim{Result: domain.InboxClaimDead}, nil
	}
	return domain.InboxClaim{Result: domain.InboxClaimWon, ClaimCount: claims + 1, Retries: retries, Provider: provider}, nil
}

// reclaim takes a locked WAITING_RETRY row for a sender, or, if it has used its claims,
// ends it DEAD and fails its delivery. It reports whether the row was taken.
func reclaim(ctx context.Context, tx pgx.Tx, deliveryID int64, claims, maxClaims int) (bool, error) {
	if claims >= maxClaims {
		if _, err := tx.Exec(ctx, deadInbox, deliveryID, domain.InboxDead, domain.ReasonMaxClaimsExceeded); err != nil {
			return false, err
		}
		_, err := tx.Exec(ctx, failDelivery, deliveryID, domain.DeliveryFailed, domain.DeliveryPending,
			domain.ReasonMaxClaimsExceeded)
		return false, err
	}
	_, err := tx.Exec(ctx, reclaimInbox, deliveryID, domain.InboxProcessing)
	return err == nil, err
}

const dueInbox = `
SELECT delivery_id, provider, claim_count, retries FROM inbox
WHERE state = $1 AND next_attempt_at <= now()
ORDER BY next_attempt_at
LIMIT $2
FOR UPDATE SKIP LOCKED`

// ClaimDue implements ports.InboxStore.
func (s *InboxStore) ClaimDue(ctx context.Context, limit int, maxClaims int) ([]domain.DueInbox, error) {
	tx, err := s.db.Pool().Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("claim due inbox: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, dueInbox, domain.InboxWaitingRetry, limit)
	if err != nil {
		return nil, fmt.Errorf("claim due inbox: %w", err)
	}
	locked, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (domain.DueInbox, error) {
		var d domain.DueInbox
		err := r.Scan(&d.DeliveryID, &d.Provider, &d.ClaimCount, &d.Retries)
		return d, err
	})
	if err != nil {
		return nil, fmt.Errorf("claim due inbox: %w", err)
	}

	claimed := make([]domain.DueInbox, 0, len(locked))
	for _, d := range locked {
		won, err := reclaim(ctx, tx, d.DeliveryID, d.ClaimCount, maxClaims)
		if err != nil {
			return nil, fmt.Errorf("claim due inbox: %w", err)
		}
		if won {
			d.ClaimCount++
			claimed = append(claimed, d)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("claim due inbox: %w", err)
	}
	return claimed, nil
}

const listStuckInbox = `
SELECT delivery_id, provider, claim_count, retries FROM inbox
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
		err := r.Scan(&s.DeliveryID, &s.Provider, &s.ClaimCount, &s.Retries)
		return s, err
	})
	if err != nil {
		return nil, fmt.Errorf("list stuck inbox: %w", err)
	}
	return stuck, nil
}

const requeueInbox = `
UPDATE inbox SET state = $4, next_attempt_at = $3, retries = retries + 1, last_try = now()
WHERE delivery_id = $1 AND state = $5 AND claim_count = $2 AND retries = $6`

// Requeue implements ports.InboxStore.
func (s *InboxStore) Requeue(ctx context.Context, st domain.StuckInbox, next time.Time) (bool, error) {
	tag, err := s.db.Pool().Exec(ctx, requeueInbox, st.DeliveryID, st.ClaimCount, next,
		domain.InboxWaitingRetry, domain.InboxProcessing, st.Retries)
	if err != nil {
		return false, fmt.Errorf("requeue inbox: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

const completeInbox = `
WITH done AS (
  UPDATE inbox SET state = $3, last_try = now()
  WHERE delivery_id = $1 AND state = $4 AND claim_count = $2
  RETURNING delivery_id
), accepted AS (
  UPDATE deliveries d SET state = $5, updated_at = now()
  FROM done WHERE d.id = done.delivery_id AND d.state = $6
)
SELECT true FROM done`

// Accept implements ports.InboxStore. True means the inbox row was completed; the delivery
// moves to ACCEPTED only if it was still PENDING.
func (s *InboxStore) Accept(ctx context.Context, st domain.StuckInbox) (bool, error) {
	return s.Complete(ctx, st.DeliveryID, st.ClaimCount)
}

// Complete implements ports.InboxStore. The inbox row is fenced; the delivery only moves
// out of PENDING, so a webhook that already finished it is not undone. A stale claim
// updates nothing, and a fresh claim whose delivery had already moved still reports true,
// which is why the result comes from the inbox row.
func (s *InboxStore) Complete(ctx context.Context, deliveryID int64, claimCount int) (bool, error) {
	var done bool
	err := s.db.Pool().QueryRow(ctx, completeInbox, deliveryID, claimCount,
		domain.InboxDone, domain.InboxProcessing, domain.DeliveryAccepted, domain.DeliveryPending).Scan(&done)
	return scanApplied(err, done, "complete inbox")
}

// scanApplied turns the result of a fenced update that returns one row when it applied.
func scanApplied(err error, applied bool, what string) (bool, error) {
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("%s: %w", what, err)
	}
	return applied, nil
}

const failInbox = `
WITH dead AS (
  UPDATE inbox SET state = $3, failure_class = $5, failure_reason = $6, last_try = now()
  WHERE delivery_id = $1 AND state = $4 AND claim_count = $2
  RETURNING delivery_id
), failed AS (
  UPDATE deliveries d SET state = $7, failure_reason = $6, updated_at = now()
  FROM dead WHERE d.id = dead.delivery_id AND d.state = $8
)
SELECT true FROM dead`

// Fail implements ports.InboxStore.
func (s *InboxStore) Fail(ctx context.Context, deliveryID int64, claimCount int, class domain.FailureClass, reason domain.FailureReason) (bool, error) {
	var done bool
	err := s.db.Pool().QueryRow(ctx, failInbox, deliveryID, claimCount,
		domain.InboxDead, domain.InboxProcessing, class, reason, domain.DeliveryFailed, domain.DeliveryPending).Scan(&done)
	return scanApplied(err, done, "fail inbox")
}

const parkInbox = `
UPDATE inbox SET state = $3, failure_class = $5, retries = retries + 1, next_attempt_at = $6, last_try = now()
WHERE delivery_id = $1 AND state = $4 AND claim_count = $2`

// Park implements ports.InboxStore.
func (s *InboxStore) Park(ctx context.Context, deliveryID int64, claimCount int, class domain.FailureClass, next time.Time) (bool, error) {
	tag, err := s.db.Pool().Exec(ctx, parkInbox, deliveryID, claimCount,
		domain.InboxWaitingRetry, domain.InboxProcessing, class, next)
	if err != nil {
		return false, fmt.Errorf("park inbox: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

const switchInbox = `
UPDATE inbox SET state = $3, provider = $5, retries = 0, failure_class = $6, next_attempt_at = $7, last_try = now()
WHERE delivery_id = $1 AND state = $4 AND claim_count = $2`

// Switch implements ports.InboxStore.
func (s *InboxStore) Switch(ctx context.Context, deliveryID int64, claimCount int, provider string, class domain.FailureClass, next time.Time) (bool, error) {
	tag, err := s.db.Pool().Exec(ctx, switchInbox, deliveryID, claimCount,
		domain.InboxWaitingRetry, domain.InboxProcessing, provider, class, next)
	if err != nil {
		return false, fmt.Errorf("switch inbox provider: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

const unresolveInbox = `
WITH dead AS (
  UPDATE inbox SET state = $3, failure_class = $5, last_try = now()
  WHERE delivery_id = $1 AND state = $4 AND claim_count = $2
  RETURNING delivery_id
), unresolved AS (
  UPDATE deliveries d SET state = $6, updated_at = now()
  FROM dead WHERE d.id = dead.delivery_id AND d.state = $7
)
SELECT true FROM dead`

// Unresolve implements ports.InboxStore. Like Fail, the delivery only moves out of PENDING.
func (s *InboxStore) Unresolve(ctx context.Context, deliveryID int64, claimCount int, class domain.FailureClass) (bool, error) {
	var done bool
	err := s.db.Pool().QueryRow(ctx, unresolveInbox, deliveryID, claimCount,
		domain.InboxDead, domain.InboxProcessing, class, domain.DeliveryUnresolved, domain.DeliveryPending).Scan(&done)
	return scanApplied(err, done, "unresolve inbox")
}
