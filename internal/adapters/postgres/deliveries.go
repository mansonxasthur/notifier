package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

var _ ports.DeliveryStore = (*DeliveryStore)(nil)

// DeliveryStore reads deliveries for the sender.
type DeliveryStore struct {
	db *Pool
}

func NewDeliveryStore(db *Pool) *DeliveryStore { return &DeliveryStore{db: db} }

// The content is stored once per notification, keyed by channel; the contact as {"address": ...}.
const loadDelivery = `
SELECT d.channel, d.contact ->> 'address', d.expires_at, d.state, d.created_at, n.content -> d.channel
FROM deliveries d JOIN notifications n ON n.id = d.notification_id
WHERE d.id = $1`

const loadDeliveries = `
SELECT d.id, d.channel, d.contact ->> 'address', d.expires_at, d.state, d.created_at, n.content -> d.channel
FROM deliveries d JOIN notifications n ON n.id = d.notification_id
WHERE d.id = ANY($1)`

// LoadMany implements ports.DeliveryStore.
func (s *DeliveryStore) LoadMany(ctx context.Context, ids []int64) (map[int64]domain.DeliveryDetail, error) {
	rows, err := s.db.Pool().Query(ctx, loadDeliveries, ids)
	if err != nil {
		return nil, fmt.Errorf("load deliveries: %w", err)
	}
	defer rows.Close()
	out := make(map[int64]domain.DeliveryDetail, len(ids))
	for rows.Next() {
		var (
			d       domain.DeliveryDetail
			content []byte
			expires time.Time
		)
		if err := rows.Scan(&d.ID, &d.Channel, &d.Contact, &expires, &d.State, &d.CreatedAt, &content); err != nil {
			return nil, fmt.Errorf("load deliveries: %w", err)
		}
		var sc storedContent
		if err := json.Unmarshal(content, &sc); err != nil {
			return nil, fmt.Errorf("decode content of delivery %d: %w", d.ID, err)
		}
		d.Content, d.ExpiresAt = domain.Content(sc), expires
		out[d.ID] = d
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load deliveries: %w", err)
	}
	return out, nil
}

// Load implements ports.DeliveryStore.
func (s *DeliveryStore) Load(ctx context.Context, id int64) (domain.DeliveryDetail, error) {
	var (
		d       = domain.DeliveryDetail{ID: id}
		content []byte
		expires time.Time
	)
	err := s.db.Pool().QueryRow(ctx, loadDelivery, id).Scan(&d.Channel, &d.Contact, &expires, &d.State, &d.CreatedAt, &content)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.DeliveryDetail{}, ports.ErrDeliveryNotFound
	}
	if err != nil {
		return domain.DeliveryDetail{}, fmt.Errorf("load delivery %d: %w", id, err)
	}
	var sc storedContent
	if err := json.Unmarshal(content, &sc); err != nil {
		return domain.DeliveryDetail{}, fmt.Errorf("decode content of delivery %d: %w", id, err)
	}
	d.Content, d.ExpiresAt = domain.Content(sc), expires
	return d, nil
}
