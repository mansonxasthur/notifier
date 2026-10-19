// Package loadtest is the load driver: it sends requests at a fixed rate, scrapes the
// services' metrics while it does, and reads the result back from Postgres.
package loadtest

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"github.com/mansonxasthur/notifier/internal/domain"
)

// Phase tags a recipient id with the part of the run that sent it, so the results can
// leave the warm-up out by id rather than by timestamp.
type Phase string

const (
	Warmup  Phase = "w"
	Measure Phase = "m"
)

// RecipientPrefix is what every recipient id of one phase of one run starts with.
func RecipientPrefix(runID string, p Phase) string { return fmt.Sprintf("%s-%s-", runID, p) }

// contact is a made-up address that is valid for the channel and unique to the recipient.
func contact(ch domain.Channel, n int) string {
	switch ch {
	case domain.ChannelEmail:
		return fmt.Sprintf("load%09d@example.com", n)
	case domain.ChannelWhatsApp:
		return fmt.Sprintf("+2011%08d", n%100_000_000)
	default:
		return fmt.Sprintf("+2010%08d", n%100_000_000)
	}
}

type recipient struct {
	ID       string                    `json:"recipient_id"`
	Channels map[domain.Channel]string `json:"channels"`
}

// Body builds the JSON body of one request with batch recipients on the given channels.
// first numbers the recipients, so no two in a run share an id or a contact.
func Body(runID string, p Phase, first, batch int, channels []domain.Channel) ([]byte, error) {
	rs := make([]recipient, batch)
	for i := range rs {
		n := first + i
		r := recipient{ID: fmt.Sprintf("%s%d", RecipientPrefix(runID, p), n), Channels: map[domain.Channel]string{}}
		for _, ch := range channels {
			r.Channels[ch] = contact(ch, n)
		}
		rs[i] = r
	}
	content := map[domain.Channel]map[string]any{
		domain.ChannelSMS:      {"text": "مباراة الهلال والنصر تبدأ الساعة 8 مساءً"},
		domain.ChannelEmail:    {"template_id": "match_started", "variables": map[string]string{"team": "الهلال"}},
		domain.ChannelWhatsApp: {"body": "مباراة الهلال والنصر تبدأ الساعة 8 مساءً"},
	}
	used := map[domain.Channel]map[string]any{}
	for _, ch := range channels {
		used[ch] = content[ch]
	}
	return json.Marshal(map[string]any{
		"idempotency_key": uuid.NewString(),
		"type":            "match_alert",
		"content":         used,
		"recipients":      rs,
	})
}
