package requests

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
)

const example = `{
  "idempotency_key": "0b6f6b3e-6d4e-4d6e-9d57-2f1d3f0f6f10",
  "type": "match_alert",
  "expires_at": "2026-10-20T18:00:00Z",
  "content": {
    "email": { "template_id": "match_started_v2", "variables": { "team_a": "الهلال", "team_b": "النصر" } },
    "sms": { "text": "مباراة الهلال والنصر تبدأ الساعة 8 مساءً" },
    "push": { "text": "unknown channel is kept for validation to reject" }
  },
  "recipients": [
    { "recipient_id": "u_1", "channels": { "email": "a@x.com", "sms": "+201000000000" } },
    { "recipient_id": "u_2", "channels": { "email": "b@x.com" } }
  ]
}`

func TestDecode(t *testing.T) {
	var r CreateNotificationRequest
	if err := json.Unmarshal([]byte(example), &r); err != nil {
		t.Fatal(err)
	}
	if r.Type != "match_alert" || r.IdempotencyKey == "" {
		t.Errorf("key/type = %q/%q", r.IdempotencyKey, r.Type)
	}
	if want := time.Date(2026, 10, 20, 18, 0, 0, 0, time.UTC); r.ExpiresAt == nil || !r.ExpiresAt.Equal(want) {
		t.Errorf("expires_at = %v, want %v", r.ExpiresAt, want)
	}
	if got := r.Content[domain.ChannelEmail]; got.TemplateID != "match_started_v2" || got.Variables["team_a"] != "الهلال" {
		t.Errorf("email content = %+v", got)
	}
	if got := r.Content[domain.ChannelSMS].Text; got != "مباراة الهلال والنصر تبدأ الساعة 8 مساءً" {
		t.Errorf("sms text = %q", got)
	}
	if _, ok := r.Content["push"]; !ok {
		t.Error("an unknown channel must decode so validation can reject it")
	}
	if len(r.Recipients) != 2 || r.Recipients[0].ID != "u_1" || r.Recipients[0].Channels[domain.ChannelSMS] != "+201000000000" {
		t.Errorf("recipients = %+v", r.Recipients)
	}
}

func TestExampleValidates(t *testing.T) {
	var r CreateNotificationRequest
	if err := json.Unmarshal([]byte(example), &r); err != nil {
		t.Fatal(err)
	}
	policy := Policy{BatchLimit: 10, Expiry: map[string]time.Duration{"match_alert": time.Hour}}
	v, err := r.Validate(policy, time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Notification.Deliveries) != 3 || len(v.Rejected) != 0 {
		t.Fatalf("got %d deliveries, %d rejected; want 3, 0", len(v.Notification.Deliveries), len(v.Rejected))
	}
	if _, kept := v.Notification.Content["push"]; kept {
		t.Error("content of a channel no delivery uses must not be stored")
	}
}
