package ports

import (
	"context"

	"github.com/mansonxasthur/notifier/internal/domain"
)

// CreateResult counts the deliveries of one Create call.
type CreateResult struct {
	// Created is the number of new deliveries, each with an outbox row.
	Created int
	// Duplicates is the number of deliveries that already existed and were skipped.
	Duplicates int
}

// NotificationStore persists notifications and their deliveries.
type NotificationStore interface {
	// Create stores the notification and its deliveries, with one outbox row per new
	// delivery, all or nothing. It sets n.ID: the new id, or the existing one when the
	// idempotency key was already used, in which case deliveries that exist are skipped.
	Create(ctx context.Context, n *domain.Notification) (CreateResult, error)
}
