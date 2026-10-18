package domain

import "time"

// Delivery is the unit of work: one message to one recipient on one channel.
type Delivery struct {
	RecipientID string
	Channel     Channel
	Contact     string
	ExpiresAt   time.Time
}

// DeliveryDetail is what a sender needs to make one send attempt.
type DeliveryDetail struct {
	ID        int64
	Channel   Channel
	Contact   string
	Content   Content
	ExpiresAt time.Time
	State     DeliveryState
}

// Notification is a validated request, ready to store. The store sets ID.
type Notification struct {
	ID             int64
	IdempotencyKey string
	Type           string
	Content        map[Channel]Content
	Deliveries     []Delivery
}
