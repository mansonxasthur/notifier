package requests

import (
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"

	"github.com/mansonxasthur/notifier/internal/domain"
)

// MaxRecipientIDLen is the longest recipient_id accepted, in bytes.
const MaxRecipientIDLen = 255

// Policy holds the configured limits that validation applies.
type Policy struct {
	// BatchLimit is the most recipients one request may carry.
	BatchLimit int
	// Expiry maps each known notification type to its default expiry.
	Expiry map[string]time.Duration
}

// Validated is the outcome of validating a request that was not rejected whole.
type Validated struct {
	// Notification holds a delivery for every valid recipient and channel. It has none
	// when every recipient was rejected.
	Notification *domain.Notification
	Rejected     []Rejection
	// Duplicates counts deliveries dropped because the payload repeated a recipient and channel.
	Duplicates int
}

// unit is the identity of a delivery inside one payload.
type unit struct {
	recipient string
	channel   domain.Channel
}

// Validate checks the request and splits it into deliveries and per-recipient rejections.
// It returns an error when the request as a whole cannot be accepted.
func (r CreateNotificationRequest) Validate(p Policy, now time.Time) (Validated, error) {
	key, err := uuid.Parse(r.IdempotencyKey)
	if err != nil || len(r.IdempotencyKey) != len(key.String()) {
		return Validated{}, ErrInvalidIdempotencyKey
	}
	if len(r.Recipients) == 0 {
		return Validated{}, ErrNoRecipients
	}
	if len(r.Recipients) > p.BatchLimit {
		return Validated{}, ErrTooManyRecipients
	}
	defaultExpiry, ok := p.Expiry[r.Type]
	if !ok {
		return Validated{}, ErrUnknownType
	}
	expiresAt := now.Add(defaultExpiry)
	if r.ExpiresAt != nil {
		if !r.ExpiresAt.After(now) {
			return Validated{}, ErrDeadlinePast
		}
		expiresAt = r.ExpiresAt.UTC()
	}

	n := &domain.Notification{IdempotencyKey: key.String(), Type: r.Type, Content: map[domain.Channel]domain.Content{}}
	out := Validated{Notification: n}
	seen := make(map[unit]bool)

	for _, rec := range r.Recipients {
		rows, reason := r.validateRecipient(rec, expiresAt)
		if reason != "" {
			// Every invalid entry is reported, so a bad repeat is never hidden and a
			// valid repeat of an invalid entry is never dropped.
			out.Rejected = append(out.Rejected, Rejection{RecipientID: rec.ID, Reason: reason})
			continue
		}
		for _, d := range rows {
			u := unit{d.RecipientID, d.Channel}
			if seen[u] {
				out.Duplicates++
				continue
			}
			seen[u] = true
			n.Deliveries = append(n.Deliveries, d)
			n.Content[d.Channel] = domain.Content(r.Content[d.Channel])
		}
	}
	return out, nil
}

// validateRecipient returns the recipient's deliveries, or the reason to reject all of them.
// The first problem found decides the reason, in this order: the recipient itself, an unknown
// channel, then for each channel in a stable order its content (missing, then shape) and
// finally its contact. So a recipient with a bad contact and no content is MISSING_CONTENT.
func (r CreateNotificationRequest) validateRecipient(rec Recipient, expiresAt time.Time) ([]domain.Delivery, RejectReason) {
	// Control characters are refused because Postgres rejects NUL in text, which would fail
	// the whole batch's insert instead of this one recipient.
	if rec.ID == "" || len(rec.ID) > MaxRecipientIDLen || strings.ContainsFunc(rec.ID, unicode.IsControl) || len(rec.Channels) == 0 {
		return nil, ReasonInvalidRecipient
	}
	for ch := range rec.Channels {
		if !ch.Valid() {
			return nil, ReasonUnknownChannel
		}
	}
	var rows []domain.Delivery
	for _, ch := range domain.Channels() {
		contact, listed := rec.Channels[ch]
		if !listed {
			continue
		}
		c, hasContent := r.Content[ch]
		if !hasContent {
			return nil, ReasonMissingContent
		}
		if !domain.Content(c).ValidFor(ch) {
			return nil, ReasonInvalidContent
		}
		if !ch.ValidContact(contact) {
			return nil, ReasonInvalidContact
		}
		rows = append(rows, domain.Delivery{RecipientID: rec.ID, Channel: ch, Contact: contact, ExpiresAt: expiresAt})
	}
	return rows, ""
}
