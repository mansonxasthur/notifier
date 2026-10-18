package domain

import (
	"net/mail"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
)

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
	Notification *Notification
	Rejected     []Rejection
	// Duplicates counts deliveries dropped because the payload repeated a recipient and channel.
	Duplicates int
}

// unit is the identity of a delivery inside one payload.
type unit struct {
	recipient string
	channel   Channel
}

func hasAccepted(seen map[unit]bool, recipient string) bool {
	for _, ch := range Channels() {
		if seen[unit{recipient, ch}] {
			return true
		}
	}
	return false
}

// e164 is a plus sign, a country code that does not start with zero, and up to 15 digits in all.
var e164 = regexp.MustCompile(`^\+[1-9][0-9]{6,14}$`)

// Validate checks the request and splits it into deliveries and per-recipient rejections.
// It returns an error when the request as a whole cannot be accepted.
func (r Request) Validate(p Policy, now time.Time) (Validated, error) {
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

	n := &Notification{IdempotencyKey: key.String(), Type: r.Type, Content: map[Channel]Content{}}
	out := Validated{Notification: n}
	seen := make(map[unit]bool)
	rejected := make(map[string]bool)

	for _, rec := range r.Recipients {
		if rejected[rec.ID] {
			// Reported once already; a repeat is not a second rejection.
			out.Duplicates += len(rec.Channels)
			continue
		}
		rows, reason := validateRecipient(rec, r.Content, expiresAt)
		if reason != "" {
			if rec.ID != "" && hasAccepted(seen, rec.ID) {
				// The first occurrence won; this copy is a repeat, not a rejection.
				out.Duplicates += len(rec.Channels)
				continue
			}
			rejected[rec.ID] = true
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
			n.Content[d.Channel] = r.Content[d.Channel]
		}
	}
	return out, nil
}

// validateRecipient returns the recipient's deliveries, or the reason to reject all of them.
// Channels are checked in a stable order so the reason does not depend on map iteration.
func validateRecipient(rec Recipient, content map[Channel]Content, expiresAt time.Time) ([]Delivery, RejectReason) {
	if rec.ID == "" || len(rec.Channels) == 0 {
		return nil, ReasonInvalidRecipient
	}
	for ch := range rec.Channels {
		if !ch.Valid() {
			return nil, ReasonUnknownChannel
		}
	}
	var rows []Delivery
	for _, ch := range Channels() {
		contact, listed := rec.Channels[ch]
		if !listed {
			continue
		}
		c, hasContent := content[ch]
		if !hasContent {
			return nil, ReasonMissingContent
		}
		if !c.validFor(ch) {
			return nil, ReasonInvalidContent
		}
		if !validContact(ch, contact) {
			return nil, ReasonInvalidContact
		}
		rows = append(rows, Delivery{RecipientID: rec.ID, Channel: ch, Contact: contact, ExpiresAt: expiresAt})
	}
	return rows, ""
}

// validFor reports whether the content has an accepted shape for the channel.
func (c Content) validFor(ch Channel) bool {
	switch ch {
	case ChannelSMS:
		return c.Text != "" && c.Subject == "" && c.Body == "" && c.TemplateID == "" && len(c.Variables) == 0
	case ChannelEmail, ChannelWhatsApp:
		if c.Text != "" {
			return false
		}
		if c.TemplateID != "" {
			return c.Subject == "" && c.Body == ""
		}
		if len(c.Variables) > 0 || c.Body == "" {
			return false
		}
		return ch == ChannelWhatsApp && c.Subject == "" || ch == ChannelEmail && c.Subject != ""
	}
	return false
}

func validContact(ch Channel, contact string) bool {
	if ch == ChannelEmail {
		addr, err := mail.ParseAddress(contact)
		if err != nil || addr.Address != contact || strings.Contains(contact, `"`) {
			return false
		}
		_, domain, _ := strings.Cut(contact, "@")
		return strings.Contains(domain, ".")
	}
	return e164.MatchString(contact)
}
