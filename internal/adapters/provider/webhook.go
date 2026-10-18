package provider

// Webhook statuses a provider reports.
const (
	WebhookDelivered = "delivered"
	WebhookFailed    = "failed"
)

// WebhookPayload is the JSON body of a provider webhook. DeliveryID is the reference key
// the provider was sent. Reason is a provider-side description for a failure; the API maps
// it to a FailureReason, and the raw text is kept in the stored event.
type WebhookPayload struct {
	DeliveryID int64  `json:"delivery_id"`
	Status     string `json:"status"`
	Reason     string `json:"reason,omitempty"`
}
