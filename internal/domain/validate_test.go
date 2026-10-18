package domain

import (
	"errors"
	"strings"
	"testing"
	"time"
)

const testKey = "0b6f6b3e-6d4e-4d6e-9d57-2f1d3f0f6f10"

var (
	now    = time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	policy = Policy{BatchLimit: 3, Expiry: map[string]time.Duration{"match_alert": 2 * time.Hour}}
)

func goodContent() map[Channel]Content {
	return map[Channel]Content{
		ChannelEmail:    {TemplateID: "match_started_v2", Variables: map[string]string{"team_a": "الهلال"}},
		ChannelSMS:      {Text: "مباراة الهلال والنصر تبدأ الساعة 8 مساءً"},
		ChannelWhatsApp: {Body: "hello"},
	}
}

func request(recipients ...Recipient) Request {
	return Request{IdempotencyKey: testKey, Type: "match_alert", Content: goodContent(), Recipients: recipients}
}

func rec(id string, channels map[Channel]string) Recipient {
	return Recipient{ID: id, Channels: channels}
}

func TestValidateRequestErrors(t *testing.T) {
	past := now.Add(-time.Minute)
	good := rec("u_1", map[Channel]string{ChannelSMS: "+201000000000"})
	tests := []struct {
		name   string
		mutate func(r *Request)
		want   error
	}{
		{"bad key", func(r *Request) { r.IdempotencyKey = "abc" }, ErrInvalidIdempotencyKey},
		{"key in braces", func(r *Request) { r.IdempotencyKey = "{" + testKey + "}" }, ErrInvalidIdempotencyKey},
		{"key without dashes", func(r *Request) { r.IdempotencyKey = "0b6f6b3e6d4e4d6e9d572f1d3f0f6f10" }, ErrInvalidIdempotencyKey},
		{"no recipients", func(r *Request) { r.Recipients = nil }, ErrNoRecipients},
		{"over the limit", func(r *Request) { r.Recipients = []Recipient{good, good, good, good} }, ErrTooManyRecipients},
		{"unknown type", func(r *Request) { r.Type = "nope" }, ErrUnknownType},
		{"deadline in the past", func(r *Request) { r.ExpiresAt = &past }, ErrDeadlinePast},
		{"deadline now", func(r *Request) { n := now; r.ExpiresAt = &n }, ErrDeadlinePast},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := request(good)
			tc.mutate(&r)
			if _, err := r.Validate(policy, now); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestValidateLimitIsInclusive(t *testing.T) {
	good := rec("u", map[Channel]string{ChannelSMS: "+201000000000"})
	r := request(good, rec("v", good.Channels), rec("w", good.Channels))
	if _, err := r.Validate(policy, now); err != nil {
		t.Fatalf("exactly the limit should pass: %v", err)
	}
}

func TestValidateRecipientReasons(t *testing.T) {
	tests := []struct {
		name    string
		content map[Channel]Content
		rec     Recipient
		want    RejectReason
	}{
		{"empty id", nil, rec("", map[Channel]string{ChannelSMS: "+201000000000"}), ReasonInvalidRecipient},
		{"id over the limit", nil, rec(strings.Repeat("a", MaxRecipientIDLen+1), map[Channel]string{ChannelSMS: "+201000000000"}), ReasonInvalidRecipient},
		{"id with NUL", nil, rec("u\x00", map[Channel]string{ChannelSMS: "+201000000000"}), ReasonInvalidRecipient},
		{"sms text with NUL", map[Channel]Content{ChannelSMS: {Text: "a\x00b"}},
			rec("u", map[Channel]string{ChannelSMS: "+201000000000"}), ReasonInvalidContent},
		{"no channels", nil, rec("u", nil), ReasonInvalidRecipient},
		{"unknown channel", nil, rec("u", map[Channel]string{"push": "x"}), ReasonUnknownChannel},
		{"no content for channel", map[Channel]Content{ChannelEmail: goodContent()[ChannelEmail]},
			rec("u", map[Channel]string{ChannelSMS: "+201000000000"}), ReasonMissingContent},
		{"sms without text", map[Channel]Content{ChannelSMS: {TemplateID: "t"}},
			rec("u", map[Channel]string{ChannelSMS: "+201000000000"}), ReasonInvalidContent},
		{"email body without subject", map[Channel]Content{ChannelEmail: {Body: "b"}},
			rec("u", map[Channel]string{ChannelEmail: "a@x.com"}), ReasonInvalidContent},
		{"template and body together", map[Channel]Content{ChannelWhatsApp: {TemplateID: "t", Body: "b"}},
			rec("u", map[Channel]string{ChannelWhatsApp: "+201000000000"}), ReasonInvalidContent},
		{"variables without template", map[Channel]Content{ChannelWhatsApp: {Body: "b", Variables: map[string]string{"a": "b"}}},
			rec("u", map[Channel]string{ChannelWhatsApp: "+201000000000"}), ReasonInvalidContent},
		{"email without a dotted domain", nil, rec("u", map[Channel]string{ChannelEmail: "a@localhost"}), ReasonInvalidContact},
		{"email with quoted local part", nil, rec("u", map[Channel]string{ChannelEmail: `"a b"@x.com`}), ReasonInvalidContact},
		{"bad email", nil, rec("u", map[Channel]string{ChannelEmail: "not-an-email"}), ReasonInvalidContact},
		{"email with display name", nil, rec("u", map[Channel]string{ChannelEmail: "A <a@x.com>"}), ReasonInvalidContact},
		{"phone without plus", nil, rec("u", map[Channel]string{ChannelSMS: "201000000000"}), ReasonInvalidContact},
		{"phone too short", nil, rec("u", map[Channel]string{ChannelWhatsApp: "+123"}), ReasonInvalidContact},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := request(tc.rec)
			if tc.content != nil {
				r.Content = tc.content
			}
			v, err := r.Validate(policy, now)
			if err != nil {
				t.Fatal(err)
			}
			if len(v.Rejected) != 1 || v.Rejected[0].Reason != tc.want || v.Rejected[0].RecipientID != tc.rec.ID {
				t.Fatalf("rejected = %+v, want one %s", v.Rejected, tc.want)
			}
			if len(v.Notification.Deliveries) != 0 {
				t.Fatalf("a rejected recipient must produce no deliveries, got %+v", v.Notification.Deliveries)
			}
		})
	}
}

func TestValidateRejectsWholeRecipientOnOneBadChannel(t *testing.T) {
	r := request(rec("u", map[Channel]string{ChannelEmail: "a@x.com", ChannelSMS: "bad"}))
	v, err := r.Validate(policy, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Rejected) != 1 || len(v.Notification.Deliveries) != 0 {
		t.Fatalf("want the whole recipient rejected, got %+v", v)
	}
}

func TestValidateAcceptsAndStampsExpiry(t *testing.T) {
	r := request(rec("u_1", map[Channel]string{ChannelEmail: "a@x.com", ChannelSMS: "+201000000000"}))
	v, err := r.Validate(policy, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Rejected) != 0 || v.Duplicates != 0 {
		t.Fatalf("unexpected result: %+v", v)
	}
	got := v.Notification.Deliveries
	if len(got) != 2 || got[0].Channel != ChannelEmail || got[1].Channel != ChannelSMS {
		t.Fatalf("deliveries = %+v, want email then sms", got)
	}
	for _, d := range got {
		if !d.ExpiresAt.Equal(now.Add(2 * time.Hour)) {
			t.Errorf("%s expires at %s, want the type default", d.Channel, d.ExpiresAt)
		}
	}
	if v.Notification.IdempotencyKey != testKey {
		t.Errorf("key = %q", v.Notification.IdempotencyKey)
	}
}

func TestValidateRequestDeadlineOverridesDefault(t *testing.T) {
	far := now.Add(90 * 24 * time.Hour)
	r := request(rec("u", map[Channel]string{ChannelSMS: "+201000000000"}))
	r.ExpiresAt = &far
	v, err := r.Validate(policy, now)
	if err != nil {
		t.Fatal(err)
	}
	if !v.Notification.Deliveries[0].ExpiresAt.Equal(far) {
		t.Errorf("expiry = %s, want %s", v.Notification.Deliveries[0].ExpiresAt, far)
	}
}

func TestValidateCountsRepeatedRecipientAsDuplicate(t *testing.T) {
	a := rec("u_1", map[Channel]string{ChannelEmail: "a@x.com", ChannelSMS: "+201000000000"})
	b := rec("u_1", map[Channel]string{ChannelEmail: "other@x.com", ChannelWhatsApp: "+201000000001"})
	v, err := request(a, b).Validate(policy, now)
	if err != nil {
		t.Fatal(err)
	}
	if v.Duplicates != 1 {
		t.Errorf("duplicates = %d, want 1 (the repeated email row)", v.Duplicates)
	}
	if len(v.Notification.Deliveries) != 3 {
		t.Fatalf("deliveries = %+v, want 3", v.Notification.Deliveries)
	}
	if v.Notification.Deliveries[0].Contact != "a@x.com" {
		t.Error("the first occurrence must win")
	}
}

func TestValidateMixedBatch(t *testing.T) {
	r := request(
		rec("ok", map[Channel]string{ChannelSMS: "+201000000000"}),
		rec("bad", map[Channel]string{ChannelSMS: "nope"}),
		rec("ok", map[Channel]string{ChannelSMS: "+201000000000"}),
	)
	v, err := r.Validate(policy, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Notification.Deliveries) != 1 || len(v.Rejected) != 1 || v.Duplicates != 1 {
		t.Fatalf("got %d accepted, %d rejected, %d duplicates; want 1, 1, 1",
			len(v.Notification.Deliveries), len(v.Rejected), v.Duplicates)
	}
}

func TestValidateBadRepeatOfAcceptedRecipientIsDuplicate(t *testing.T) {
	good := rec("u_1", map[Channel]string{ChannelSMS: "+201000000000"})
	bad := rec("u_1", map[Channel]string{ChannelSMS: "nope"})
	v, err := request(good, bad).Validate(policy, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Rejected) != 0 || v.Duplicates != 1 || len(v.Notification.Deliveries) != 1 {
		t.Fatalf("got %+v, want u_1 accepted once and the repeat counted as a duplicate", v)
	}
}

func TestValidateRepeatOfRejectedRecipientIsReportedOnce(t *testing.T) {
	bad := rec("u_1", map[Channel]string{ChannelSMS: "nope"})
	good := rec("u_1", map[Channel]string{ChannelSMS: "+201000000000"})
	v, err := request(bad, good).Validate(policy, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Rejected) != 1 || len(v.Notification.Deliveries) != 0 || v.Duplicates != 1 {
		t.Fatalf("got %+v, want u_1 rejected once and never accepted", v)
	}
}

func TestValidateStoresOnlyContentOfUsedChannels(t *testing.T) {
	r := request(rec("u", map[Channel]string{ChannelSMS: "+201000000000"}))
	r.Content["push"] = Content{Text: "never validated"}
	v, err := r.Validate(policy, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Notification.Content) != 1 || v.Notification.Content[ChannelSMS].Text == "" {
		t.Fatalf("content = %+v, want only sms", v.Notification.Content)
	}
}

func TestValidateRecipientIDAtLimitIsAccepted(t *testing.T) {
	id := strings.Repeat("a", MaxRecipientIDLen)
	v, err := request(rec(id, map[Channel]string{ChannelSMS: "+201000000000"})).Validate(policy, now)
	if err != nil || len(v.Rejected) != 0 || len(v.Notification.Deliveries) != 1 {
		t.Fatalf("got %+v, %v; want accepted", v, err)
	}
}

func TestValidateAllowsLineBreaksInText(t *testing.T) {
	r := request(rec("u", map[Channel]string{ChannelSMS: "+201000000000"}))
	r.Content[ChannelSMS] = Content{Text: "line one\nline two\ttabbed"}
	v, err := r.Validate(policy, now)
	if err != nil || len(v.Rejected) != 0 {
		t.Fatalf("got %+v, %v; want accepted", v, err)
	}
}
