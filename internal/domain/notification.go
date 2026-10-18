package domain

import "time"

// Content is what the producer wants sent on one channel. The service forwards it as is.
// SMS uses Text. Email and WhatsApp use either inline content (Subject and Body for email,
// Body for WhatsApp) or TemplateID with Variables.
type Content struct {
	Text       string            `json:"text,omitempty"`
	Subject    string            `json:"subject,omitempty"`
	Body       string            `json:"body,omitempty"`
	TemplateID string            `json:"template_id,omitempty"`
	Variables  map[string]string `json:"variables,omitempty"`
}

// Recipient is one person and the contact to use on each channel they accept.
type Recipient struct {
	ID       string             `json:"recipient_id"`
	Channels map[Channel]string `json:"channels"`
}

// Request is a notification as the producer sent it, before validation.
type Request struct {
	IdempotencyKey string              `json:"idempotency_key"`
	Type           string              `json:"type"`
	ExpiresAt      *time.Time          `json:"expires_at,omitempty"`
	Content        map[Channel]Content `json:"content"`
	Recipients     []Recipient         `json:"recipients"`
}

// Delivery is the unit of work: one message to one recipient on one channel.
type Delivery struct {
	RecipientID string
	Channel     Channel
	Contact     string
	ExpiresAt   time.Time
}

// Notification is a validated request, ready to store. The store sets ID.
type Notification struct {
	ID             int64
	IdempotencyKey string
	Type           string
	Content        map[Channel]Content
	Deliveries     []Delivery
}
