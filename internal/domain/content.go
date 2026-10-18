package domain

import (
	"strings"
	"unicode"
)

// Content is what is sent on one channel. The service forwards it as is. SMS uses Text.
// Email and WhatsApp use either inline content (Subject and Body for email, Body for
// WhatsApp) or TemplateID with Variables.
type Content struct {
	Text       string
	Subject    string
	Body       string
	TemplateID string
	Variables  map[string]string
}

// ValidFor reports whether the content has an accepted shape for the channel.
func (c Content) ValidFor(ch Channel) bool {
	if c.hasControl() {
		return false
	}
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

// hasControl reports whether any text in the content has a control character. Postgres
// refuses NUL in text and jsonb. Line breaks and tabs are fine in message text, so only
// the rest of the control characters are refused.
func (c Content) hasControl() bool {
	bad := func(s string) bool {
		return strings.ContainsFunc(s, func(r rune) bool {
			return unicode.IsControl(r) && r != '\n' && r != '\r' && r != '\t'
		})
	}
	if bad(c.Text) || bad(c.Subject) || bad(c.Body) || bad(c.TemplateID) {
		return true
	}
	for k, v := range c.Variables {
		if bad(k) || bad(v) {
			return true
		}
	}
	return false
}
