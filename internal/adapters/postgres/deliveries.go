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
SELECT d.channel, d.contact ->> 'address', d.expires_at, d.state, n.content -> d.channel
FROM deliveries d JOIN notifications n ON n.id = d.notification_id
WHERE d.id = $1`

// Load implements ports.DeliveryStore.
func (s *DeliveryStore) Load(ctx context.Context, id int64) (domain.DeliveryDetail, error) {
	var (
		d       = domain.DeliveryDetail{ID: id}
		content []byte
		expires time.Time
	)
	err := s.db.Pool().QueryRow(ctx, loadDelivery, id).Scan(&d.Channel, &d.Contact, &expires, &d.State, &content)
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
