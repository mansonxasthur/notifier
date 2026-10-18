// Package requests holds the request bodies of the API and the validation that turns them
// into domain values.
package requests

import (
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
)

// CreateNotificationRequest is the body of POST /v1/notifications.
type CreateNotificationRequest struct {
	IdempotencyKey string                     `json:"idempotency_key"`
	Type           string                     `json:"type"`
	ExpiresAt      *time.Time                 `json:"expires_at,omitempty"`
	Content        map[domain.Channel]Content `json:"content"`
	Recipients     []Recipient                `json:"recipients"`
}

// Content is what to send on one channel: text for SMS; a subject and body, or a template
// ID with variables, for email; a body, or a template ID with variables, for WhatsApp.
type Content struct {
	Text       string            `json:"text,omitempty"`
	Subject    string            `json:"subject,omitempty"`
	Body       string            `json:"body,omitempty"`
	TemplateID string            `json:"template_id,omitempty"`
	Variables  map[string]string `json:"variables,omitempty"`
}

// Recipient is one person and the contact to use on each channel they accept.
type Recipient struct {
	ID       string                    `json:"recipient_id"`
	Channels map[domain.Channel]string `json:"channels"`
}
