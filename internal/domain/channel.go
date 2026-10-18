package domain

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
