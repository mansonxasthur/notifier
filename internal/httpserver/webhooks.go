package httpserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/mansonxasthur/notifier/internal/adapters/provider"
	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

// Error codes of the webhook endpoint, besides the shared ones.
const (
	codeUnknownProvider = "UNKNOWN_PROVIDER"
	codeUnknownDelivery = "UNKNOWN_DELIVERY"
)

// WebhooksOptions configures the provider webhook endpoint.
type WebhooksOptions struct {
	Log   *slog.Logger
	Store ports.WebhookStore
	// Authenticators checks each provider's webhooks, by provider name.
	Authenticators map[string]ports.WebhookAuthenticator
	// Channels is the channel each provider serves, by provider name.
	Channels map[string]domain.Channel
	// MaxBodyBytes caps the request body, which is read before it is authenticated. Zero
	// means defaultMaxBody: webhook payloads are small.
	MaxBodyBytes int64
}

// defaultMaxBody caps a webhook body when no cap is configured.
const defaultMaxBody = 64 << 10

// RegisterWebhooks adds POST /v1/webhooks/{provider} to mux. The provider named in the
// path picks the authenticator, since providers sign in different ways. It refuses a
// configuration with nothing to authenticate against, which would answer 404 to everyone.
func RegisterWebhooks(mux *http.ServeMux, o WebhooksOptions) error {
	if len(o.Authenticators) == 0 {
		return errors.New("register webhooks: no provider authenticators")
	}
	if o.Store == nil {
		return errors.New("register webhooks: no store")
	}
	for name := range o.Authenticators {
		if !o.Channels[name].Valid() {
			return fmt.Errorf("register webhooks: provider %s has no channel", name)
		}
	}
	if o.MaxBodyBytes <= 0 {
		o.MaxBodyBytes = defaultMaxBody
	}
	mux.HandleFunc("POST /v1/webhooks/{provider}", o.receive)
	return nil
}

// receive authenticates the webhook, then stores it and applies it to its delivery in one
// transaction. The answer is the acknowledgement: a duplicate or stale event is stored and
// acknowledged like any other, because the provider cannot do anything about it.
func (o WebhooksOptions) receive(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("provider")
	auth, ok := o.Authenticators[name]
	if !ok {
		writeError(w, http.StatusNotFound, codeUnknownProvider)
		return
	}

	// The signature covers the exact bytes, so the body is read whole before anything else.
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, o.MaxBodyBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, codeBodyTooLarge)
		} else {
			writeError(w, http.StatusBadRequest, codeMalformedBody)
		}
		return
	}
	if err := auth.Authenticate(r, body); err != nil {
		o.Log.WarnContext(r.Context(), "webhook not authenticated", "provider", name)
		writeError(w, http.StatusUnauthorized, codeUnauthorized)
		return
	}

	event, ok := parseWebhook(name, o.Channels[name], body)
	if !ok {
		writeError(w, http.StatusBadRequest, codeMalformedBody)
		return
	}

	applied, err := o.Store.Apply(r.Context(), event)
	switch {
	case errors.Is(err, ports.ErrDeliveryNotFound):
		o.Log.WarnContext(r.Context(), "webhook for an unknown delivery", "provider", name, "delivery_id", event.DeliveryID)
		writeError(w, http.StatusNotFound, codeUnknownDelivery)
		return
	case err != nil:
		o.Log.ErrorContext(r.Context(), "apply webhook", "provider", name, "delivery_id", event.DeliveryID, "error", err)
		writeError(w, http.StatusInternalServerError, codeInternal)
		return
	}

	o.Log.InfoContext(r.Context(), "webhook received", "provider", name, "delivery_id", event.DeliveryID,
		"event", event.EventType, "outcome", event.Outcome, "applied", applied)
	writeJSON(w, http.StatusOK, map[string]bool{"applied": applied})
}

// parseWebhook reads the provider's report. A failure reason the enum does not know is
// recorded as PROVIDER_FAILED; the provider's own wording stays in the stored payload.
func parseWebhook(providerName string, channel domain.Channel, body []byte) (ports.WebhookEvent, bool) {
	var p provider.WebhookPayload
	if err := json.Unmarshal(body, &p); err != nil || p.DeliveryID <= 0 {
		return ports.WebhookEvent{}, false
	}
	e := ports.WebhookEvent{
		DeliveryID: p.DeliveryID, Provider: providerName, Channel: channel, EventType: p.Status, Payload: body,
	}
	switch p.Status {
	case provider.WebhookDelivered:
		e.Outcome = domain.DeliveryDelivered
	case provider.WebhookFailed:
		e.Outcome = domain.DeliveryFailed
		e.Reason = domain.FailureReason(p.Reason)
		if !e.Reason.Valid() {
			e.Reason = domain.ReasonProviderFailed
		}
	default:
		return ports.WebhookEvent{}, false
	}
	return e, true
}
