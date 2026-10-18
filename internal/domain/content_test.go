package domain

import "testing"

func TestContentValidFor(t *testing.T) {
	vars := map[string]string{"team_a": "الهلال"}
	tests := []struct {
		name string
		ch   Channel
		c    Content
		want bool
	}{
		{"sms text", ChannelSMS, Content{Text: "hi"}, true},
		{"sms text with line break and tab", ChannelSMS, Content{Text: "a\nb\tc"}, true},
		{"sms without text", ChannelSMS, Content{TemplateID: "t"}, false},
		{"sms with a template", ChannelSMS, Content{Text: "hi", TemplateID: "t"}, false},
		{"email template", ChannelEmail, Content{TemplateID: "t", Variables: vars}, true},
		{"email inline", ChannelEmail, Content{Subject: "s", Body: "b"}, true},
		{"email body without subject", ChannelEmail, Content{Body: "b"}, false},
		{"email with text", ChannelEmail, Content{Text: "t", Body: "b", Subject: "s"}, false},
		{"whatsapp inline", ChannelWhatsApp, Content{Body: "b"}, true},
		{"whatsapp template", ChannelWhatsApp, Content{TemplateID: "t"}, true},
		{"whatsapp with a subject", ChannelWhatsApp, Content{Body: "b", Subject: "s"}, false},
		{"template and body together", ChannelWhatsApp, Content{TemplateID: "t", Body: "b"}, false},
		{"variables without a template", ChannelWhatsApp, Content{Body: "b", Variables: vars}, false},
		{"empty", ChannelEmail, Content{}, false},
		{"NUL in text", ChannelSMS, Content{Text: "a\x00b"}, false},
		{"NUL in a variable", ChannelEmail, Content{TemplateID: "t", Variables: map[string]string{"k": "v\x00"}}, false},
		{"unknown channel", "push", Content{Text: "hi"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.c.ValidFor(tc.ch); got != tc.want {
				t.Errorf("ValidFor(%s) = %v, want %v", tc.ch, got, tc.want)
			}
		})
	}
}

func TestChannelValidContact(t *testing.T) {
	tests := []struct {
		ch      Channel
		contact string
		want    bool
	}{
		{ChannelEmail, "a@x.com", true},
		{ChannelEmail, "not-an-email", false},
		{ChannelEmail, "A <a@x.com>", false},
		{ChannelEmail, "a@localhost", false},
		{ChannelEmail, `"a b"@x.com`, false},
		{ChannelSMS, "+201000000000", true},
		{ChannelSMS, "201000000000", false},
		{ChannelSMS, "+0123456789", false},
		{ChannelWhatsApp, "+123", false},
		{ChannelWhatsApp, "+201000000001", true},
	}
	for _, tc := range tests {
		if got := tc.ch.ValidContact(tc.contact); got != tc.want {
			t.Errorf("%s.ValidContact(%q) = %v, want %v", tc.ch, tc.contact, got, tc.want)
		}
	}
}
