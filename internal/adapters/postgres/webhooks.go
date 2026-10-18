package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

var _ ports.WebhookStore = (*WebhookStore)(nil)

// WebhookStore records provider webhooks and applies them to deliveries.
type WebhookStore struct {
	db *Pool
}

func NewWebhookStore(db *Pool) *WebhookStore { return &WebhookStore{db: db} }

// foreignKeyViolation is the Postgres error code for a missing referenced row.
const foreignKeyViolation = "23503"

// The guard: the delivery moves only while its state is one of the allowed sources, so
// terminal states stay terminal and the first terminal webhook wins.
const applyWebhook = `
UPDATE deliveries SET state = $2, failure_reason = $3, updated_at = now()
WHERE id = $1 AND state = ANY($4::text[])`

const recordWebhook = `
INSERT INTO webhook_events (delivery_id, provider, event_type, payload, applied)
VALUES ($1, $2, $3, $4::jsonb, $5)`

// Apply implements ports.WebhookStore.
func (s *WebhookStore) Apply(ctx context.Context, e ports.WebhookEvent) (bool, error) {
	from := domain.MovesInto(e.Outcome)
	if len(from) == 0 {
		return false, fmt.Errorf("apply webhook: %s is not a state a delivery can move into", e.Outcome)
	}
	sources := make([]string, len(from))
	for i, f := range from {
		sources[i] = string(f)
	}
	var reason *string
	if e.Outcome == domain.DeliveryFailed && e.Reason != "" { // only a failure has a reason
		r := string(e.Reason)
		reason = &r
	}

	tx, err := s.db.Pool().Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("apply webhook: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, applyWebhook, e.DeliveryID, string(e.Outcome), reason, sources)
	if err != nil {
		return false, fmt.Errorf("apply webhook: %w", err)
	}
	applied := tag.RowsAffected() == 1

	if _, err := tx.Exec(ctx, recordWebhook, e.DeliveryID, e.Provider, e.EventType, string(e.Payload), applied); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == foreignKeyViolation {
			return false, ports.ErrDeliveryNotFound
		}
		return false, fmt.Errorf("record webhook: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("apply webhook: %w", err)
	}
	return applied, nil
}
