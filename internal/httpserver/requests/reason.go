package requests

import "errors"

// RejectReason says why one recipient was not accepted. The values are part of the API.
type RejectReason string

const (
	ReasonInvalidRecipient RejectReason = "INVALID_RECIPIENT"
	ReasonInvalidContact   RejectReason = "INVALID_CONTACT"
	ReasonMissingContent   RejectReason = "MISSING_CONTENT"
	ReasonInvalidContent   RejectReason = "INVALID_CONTENT"
	ReasonUnknownChannel   RejectReason = "UNKNOWN_CHANNEL"
)

// Rejection is a recipient that failed validation. Rejections are not retryable as sent.
type Rejection struct {
	RecipientID string       `json:"recipient_id"`
	Reason      RejectReason `json:"reason"`
}

// Errors that reject the whole request before any recipient is looked at.
var (
	ErrInvalidIdempotencyKey = errors.New("idempotency_key must be a UUID")
	ErrNoRecipients          = errors.New("recipients must not be empty")
	ErrTooManyRecipients     = errors.New("too many recipients")
	ErrUnknownType           = errors.New("unknown notification type")
	ErrDeadlinePast          = errors.New("expires_at is in the past")
)
