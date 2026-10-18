package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

var _ ports.NotificationStore = (*NotificationStore)(nil)

// NotificationStore writes notifications, deliveries and outbox rows.
type NotificationStore struct {
	db        *Pool
	chunkSize int
}

// NewNotificationStore returns a store that inserts at most chunkSize deliveries per statement.
func NewNotificationStore(db *Pool, chunkSize int) *NotificationStore {
	return &NotificationStore{db: db, chunkSize: chunkSize}
}

const insertNotification = `
INSERT INTO notifications (idempotency_key, type, content)
VALUES ($1, $2, $3::jsonb)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING id`

const selectNotification = `SELECT id FROM notifications WHERE idempotency_key = $1`

// insertDeliveries adds a chunk of deliveries and an outbox row for each one that was not
// already there. The arrays keep the parameter count fixed however many rows a chunk has,
// and the outer INSERT's row count is the number of new deliveries.
const insertDeliveries = `
WITH inserted AS (
  INSERT INTO deliveries (notification_id, recipient_id, channel, contact, expires_at, state)
  SELECT $1::bigint, r.recipient_id, r.channel, r.contact::jsonb, r.expires_at, $2::text
  FROM unnest($3::text[], $4::text[], $5::text[], $6::timestamptz[])
    AS r(recipient_id, channel, contact, expires_at)
  ORDER BY r.recipient_id COLLATE "C", r.channel COLLATE "C"
  ON CONFLICT (notification_id, recipient_id, channel) DO NOTHING
  RETURNING id
)
INSERT INTO outbox (delivery_id, state, retries, last_try)
SELECT id, $7::text, 0, now() FROM inserted`

// storedContent is the JSON shape of a channel's content in notifications.content.
type storedContent struct {
	Text       string            `json:"text,omitempty"`
	Subject    string            `json:"subject,omitempty"`
	Body       string            `json:"body,omitempty"`
	TemplateID string            `json:"template_id,omitempty"`
	Variables  map[string]string `json:"variables,omitempty"`
}

func storedContents(content map[domain.Channel]domain.Content) map[domain.Channel]storedContent {
	out := make(map[domain.Channel]storedContent, len(content))
	for ch, c := range content {
		out[ch] = storedContent(c)
	}
	return out
}

// Create implements ports.NotificationStore.
func (s *NotificationStore) Create(ctx context.Context, n *domain.Notification) (ports.CreateResult, error) {
	if s.chunkSize < 1 {
		return ports.CreateResult{}, fmt.Errorf("create notification: chunk size %d must be positive", s.chunkSize)
	}
	if len(n.Deliveries) == 0 {
		return ports.CreateResult{}, errors.New("create notification: no deliveries")
	}
	content, err := json.Marshal(storedContents(n.Content))
	if err != nil {
		return ports.CreateResult{}, fmt.Errorf("encode content: %w", err)
	}

	// Sorting the whole request first makes every transaction lock rows in the same
	// order, so two overlapping requests cannot deadlock. Go compares bytes, so the SQL
	// ORDER BY uses the C collation to agree with it.
	rows := slices.Clone(n.Deliveries)
	slices.SortFunc(rows, func(a, b domain.Delivery) int {
		return strings.Compare(a.RecipientID+"\x00"+string(a.Channel), b.RecipientID+"\x00"+string(b.Channel))
	})

	tx, err := s.db.Pool().Begin(ctx)
	if err != nil {
		return ports.CreateResult{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // a no-op once committed

	id, err := notificationID(ctx, tx, n, content)
	if err != nil {
		return ports.CreateResult{}, err
	}

	var res ports.CreateResult
	for chunk := range slices.Chunk(rows, s.chunkSize) {
		created, err := insertChunk(ctx, tx, id, chunk)
		if err != nil {
			return ports.CreateResult{}, err
		}
		res.Created += created
		res.Duplicates += len(chunk) - created
	}
	if err := tx.Commit(ctx); err != nil {
		return ports.CreateResult{}, fmt.Errorf("commit: %w", err)
	}
	n.ID = id // only once the transaction is durable
	return res, nil
}

// notificationID inserts the notification, or finds it when the idempotency key exists.
// The lookup is a second statement because a single statement could miss a row that a
// concurrent request committed a moment earlier.
func notificationID(ctx context.Context, tx pgx.Tx, n *domain.Notification, content []byte) (int64, error) {
	var id int64
	err := tx.QueryRow(ctx, insertNotification, n.IdempotencyKey, n.Type, string(content)).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		err = tx.QueryRow(ctx, selectNotification, n.IdempotencyKey).Scan(&id)
	}
	if err != nil {
		return 0, fmt.Errorf("store notification: %w", err)
	}
	return id, nil
}

func insertChunk(ctx context.Context, tx pgx.Tx, notificationID int64, chunk []domain.Delivery) (int, error) {
	recipients := make([]string, len(chunk))
	channels := make([]string, len(chunk))
	contacts := make([]string, len(chunk))
	expiries := make([]time.Time, len(chunk))
	for i, d := range chunk {
		contact, err := json.Marshal(map[string]string{"address": d.Contact})
		if err != nil {
			return 0, fmt.Errorf("encode contact: %w", err)
		}
		recipients[i], channels[i], contacts[i], expiries[i] = d.RecipientID, string(d.Channel), string(contact), d.ExpiresAt
	}
	tag, err := tx.Exec(ctx, insertDeliveries, notificationID, string(domain.DeliveryPending),
		recipients, channels, contacts, expiries, string(domain.OutboxNew))
	if err != nil {
		return 0, fmt.Errorf("store deliveries: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
