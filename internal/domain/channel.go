package domain

import (
	"net/mail"
	"regexp"
	"strings"
)

// Channel is a delivery channel. Each channel has its own queue and providers.
type Channel string

const (
	ChannelEmail    Channel = "email"
	ChannelSMS      Channel = "sms"
	ChannelWhatsApp Channel = "whatsapp"
)

// Channels lists every channel in a stable order.
func Channels() []Channel {
	return []Channel{ChannelEmail, ChannelSMS, ChannelWhatsApp}
}

func (c Channel) Valid() bool {
	switch c {
	case ChannelEmail, ChannelSMS, ChannelWhatsApp:
		return true
	}
	return false
}

// e164 is a plus sign, a country code that does not start with zero, and up to 15 digits in all.
var e164 = regexp.MustCompile(`^\+[1-9][0-9]{6,14}$`)

// ValidContact reports whether contact is a usable address on the channel: a plain email
// address with a dotted domain for email, an E.164 number for SMS and WhatsApp.
func (c Channel) ValidContact(contact string) bool {
	if c == ChannelEmail {
		addr, err := mail.ParseAddress(contact)
		if err != nil || addr.Address != contact || strings.Contains(contact, `"`) {
			return false
		}
		_, host, _ := strings.Cut(contact, "@")
		return strings.Contains(host, ".")
	}
	return e164.MatchString(contact)
}
