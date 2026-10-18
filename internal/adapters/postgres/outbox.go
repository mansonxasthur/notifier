package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

var _ ports.OutboxStore = (*OutboxStore)(nil)

// OutboxStore implements the relay's side of the outbox.
type OutboxStore struct {
	db *Pool
}

func NewOutboxStore(db *Pool) *OutboxStore { return &OutboxStore{db: db} }

// claimOutbox locks due rows with SKIP LOCKED so relays never take the same row. Rows that
// already used all their claims end DEAD, and their delivery FAILED, instead of cycling.
// The data-modifying CTEs all run; only the claimed rows are returned.
const claimOutbox = `
WITH due AS (
  SELECT delivery_id, retries FROM outbox
  WHERE state = $4 AND (next_attempt_at IS NULL OR next_attempt_at <= now())
  ORDER BY next_attempt_at NULLS FIRST, delivery_id
  LIMIT $1
  FOR UPDATE SKIP LOCKED
), dead AS (
  UPDATE outbox o SET state = $5, failure_reason = $6, last_try = now()
  FROM due WHERE o.delivery_id = due.delivery_id AND due.retries >= $2
  RETURNING o.delivery_id
), failed AS (
  UPDATE deliveries d SET state = $7, updated_at = now()
  FROM dead WHERE d.id = dead.delivery_id AND d.state = $8
), claimed AS (
  UPDATE outbox o SET state = $3, retries = o.retries + 1, last_try = now()
  FROM due WHERE o.delivery_id = due.delivery_id AND due.retries < $2
  RETURNING o.delivery_id, o.retries
)
SELECT c.delivery_id, c.retries, d.channel
FROM claimed c JOIN deliveries d ON d.id = c.delivery_id
ORDER BY c.delivery_id`

// Claim implements ports.OutboxStore.
func (s *OutboxStore) Claim(ctx context.Context, limit, maxAttempts int) ([]domain.OutboxClaim, error) {
	rows, err := s.db.Pool().Query(ctx, claimOutbox,
		limit, maxAttempts, domain.OutboxProcessing, domain.OutboxNew,
		domain.OutboxDead, domain.ReasonMaxAttemptsExceeded, domain.DeliveryFailed, domain.DeliveryPending)
	if err != nil {
		return nil, fmt.Errorf("claim outbox: %w", err)
	}
	claims, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (domain.OutboxClaim, error) {
		var c domain.OutboxClaim
		err := r.Scan(&c.DeliveryID, &c.Retries, &c.Channel)
		return c, err
	})
	if err != nil {
		return nil, fmt.Errorf("claim outbox: %w", err)
	}
	return claims, nil
}

const markPublished = `
UPDATE outbox o SET state = $3, last_try = now()
FROM unnest($1::bigint[], $2::int[]) AS c(delivery_id, retries)
WHERE o.delivery_id = c.delivery_id AND o.state = $4 AND o.retries = c.retries`

// MarkPublished implements ports.OutboxStore.
func (s *OutboxStore) MarkPublished(ctx context.Context, claims []domain.OutboxClaim) (int, error) {
	if len(claims) == 0 {
		return 0, nil
	}
	ids := make([]int64, len(claims))
	retries := make([]int32, len(claims))
	for i, c := range claims {
		ids[i], retries[i] = c.DeliveryID, int32(c.Retries) //nolint:gosec // retries is a small attempt counter
	}
	tag, err := s.db.Pool().Exec(ctx, markPublished, ids, retries, domain.OutboxPublished, domain.OutboxProcessing)
	if err != nil {
		return 0, fmt.Errorf("mark published: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

const releaseOutbox = `
UPDATE outbox SET state = $4, next_attempt_at = $3, failure_reason = $5
WHERE delivery_id = $1 AND state = $6 AND retries = $2`

// Release implements ports.OutboxStore.
func (s *OutboxStore) Release(ctx context.Context, c domain.OutboxClaim, next time.Time, reason domain.FailureReason) (bool, error) {
	tag, err := s.db.Pool().Exec(ctx, releaseOutbox, c.DeliveryID, c.Retries, next,
		domain.OutboxNew, reason, domain.OutboxProcessing)
	if err != nil {
		return false, fmt.Errorf("release outbox: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

const selectStuckOutbox = `
SELECT delivery_id, retries FROM outbox
WHERE state = $1 AND last_try < now() - make_interval(secs => $2)
ORDER BY last_try
FOR UPDATE SKIP LOCKED`

const reclaimOutbox = `
UPDATE outbox o SET state = $2, next_attempt_at = r.next_attempt_at
FROM unnest($3::bigint[], $4::timestamptz[]) AS r(delivery_id, next_attempt_at)
WHERE o.delivery_id = r.delivery_id AND o.state = $1`

// ReclaimStuck implements ports.OutboxStore.
func (s *OutboxStore) ReclaimStuck(ctx context.Context, olderThan time.Duration, delay func(retries int) time.Duration) (int, error) {
	tx, err := s.db.Pool().Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("reclaim outbox: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, selectStuckOutbox, domain.OutboxProcessing, olderThan.Seconds())
	if err != nil {
		return 0, fmt.Errorf("reclaim outbox: %w", err)
	}
	var ids []int64
	var due []time.Time
	now := time.Now()
	for rows.Next() {
		var id int64
		var retries int
		if err := rows.Scan(&id, &retries); err != nil {
			rows.Close()
			return 0, fmt.Errorf("reclaim outbox: %w", err)
		}
		ids = append(ids, id)
		due = append(due, now.Add(delay(retries)))
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("reclaim outbox: %w", err)
	}
	if len(ids) == 0 {
		return 0, nil
	}

	tag, err := tx.Exec(ctx, reclaimOutbox, domain.OutboxProcessing, domain.OutboxNew, ids, due)
	if err != nil {
		return 0, fmt.Errorf("reclaim outbox: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("reclaim outbox: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
