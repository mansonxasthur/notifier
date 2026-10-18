package simprovider

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/mansonxasthur/notifier/internal/adapters/provider"
	"github.com/mansonxasthur/notifier/internal/config"
	"github.com/mansonxasthur/notifier/internal/domain"
)

// maxSendBody caps a send request body.
const maxSendBody = 1 << 20

// failureReason is what a late failure webhook reports: a name from the failure enum.
const failureReason = string(domain.ReasonBounced)

// Server is the simulated provider's HTTP API. One service plays every configured provider,
// told apart by the provider name in the path:
//
//	POST /providers/{provider}/send
//	GET  /providers/{provider}/status/{delivery_id}
//	GET  /scenarios and GET /scenarios/{delivery_id}
type Server struct {
	providers map[string]config.Provider
	cfg       config.SimProvider
	chooser   *Chooser
	ledger    *Ledger
	hooks     *Webhooks
	log       *slog.Logger
	now       func() time.Time
}

// NewServer returns a Server for the configured providers.
func NewServer(cfg *config.Config, hooks *Webhooks, log *slog.Logger) *Server {
	return &Server{
		providers: cfg.ProvidersByName(), cfg: cfg.SimProvider, chooser: NewChooser(cfg.SimProvider),
		ledger: NewLedger(), hooks: hooks, log: log, now: time.Now,
	}
}

// Register adds the simulator's routes to mux.
func (s *Server) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /providers/{provider}/send", s.send)
	mux.HandleFunc("GET /providers/{provider}/status/{id}", s.status)
	mux.HandleFunc("GET /scenarios", s.scenarios)
	mux.HandleFunc("GET /scenarios/{id}", s.scenario)
}

func (s *Server) send(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("provider")
	if _, ok := s.providers[name]; !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown_provider"})
		return
	}

	var p provider.SendPayload
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxSendBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "malformed_body"})
		return
	}
	if msg := validate(p); msg != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": msg})
		return
	}

	e, first := s.ledger.Record(name, p.DeliveryID, s.chooser.Pick, s.now())
	s.log.Info("send received", "delivery_id", p.DeliveryID, "provider", name,
		"scenario", e.Scenario, "first", first, "sends", e.Sends)

	// The webhook is scheduled on the first call only.
	if first {
		s.scheduleWebhook(name, e)
	}

	switch e.Scenario {
	case domain.ScenarioFailPermanent:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_destination"})
		return
	case domain.ScenarioFailTransient:
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "unavailable"})
		return
	case domain.ScenarioSlow:
		if !sleep(r.Context(), s.cfg.SlowDelay) {
			return // the caller gave up, or the server is stopping
		}
	case domain.ScenarioTimeout:
		if !sleep(r.Context(), s.cfg.TimeoutDelay) {
			return
		}
	case domain.ScenarioSucceed, domain.ScenarioLateFailure:
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"delivery_id": p.DeliveryID, "status": "accepted"})
}

// scheduleWebhook sends the webhook this scenario ends with, if it has one. A slow or
// timed-out call sends its webhook after the response it is delaying, so the webhook
// never races the caller's own timeout handling.
func (s *Server) scheduleWebhook(name string, e Entry) {
	switch e.Scenario {
	case domain.ScenarioSucceed:
		s.hooks.Schedule(name, provider.WebhookPayload{DeliveryID: e.DeliveryID, Status: provider.WebhookDelivered}, 0)
	case domain.ScenarioSlow:
		s.hooks.Schedule(name, provider.WebhookPayload{DeliveryID: e.DeliveryID, Status: provider.WebhookDelivered}, s.cfg.SlowDelay)
	case domain.ScenarioTimeout:
		s.hooks.Schedule(name, provider.WebhookPayload{DeliveryID: e.DeliveryID, Status: provider.WebhookDelivered}, s.cfg.TimeoutDelay)
	case domain.ScenarioLateFailure:
		s.hooks.Schedule(name, provider.WebhookPayload{DeliveryID: e.DeliveryID, Status: provider.WebhookFailed, Reason: failureReason}, 0)
	case domain.ScenarioFailPermanent, domain.ScenarioFailTransient:
	}
}

// validate checks the send request the way a real provider would: it needs a reference
// key, a known channel, a contact, and content in an accepted shape.
func validate(p provider.SendPayload) string {
	switch {
	case p.DeliveryID <= 0:
		return "missing_reference"
	case !p.Channel.Valid():
		return "unknown_channel"
	case p.Contact == "":
		return "missing_contact"
	}
	c := domain.Content{
		Text: p.Content.Text, Subject: p.Content.Subject, Body: p.Content.Body,
		TemplateID: p.Content.TemplateID, Variables: p.Content.Variables,
	}
	if !c.ValidFor(p.Channel) {
		return "invalid_content"
	}
	return ""
}

// status answers a lookup by delivery_id: 200 when the provider holds the message, 404
// when it does not, and 501 when this provider has no lookup.
func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("provider")
	cfg, ok := s.providers[name]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown_provider"})
		return
	}
	if !cfg.StatusLookup {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "status_lookup_unsupported"})
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_id"})
		return
	}
	e, found := s.ledger.Lookup(name, id)
	if !found || !e.Accepted {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"delivery_id": id, "status": "accepted"})
}

// scenarios lists what the simulator decided: a count per scenario and every entry.
func (s *Server) scenarios(w http.ResponseWriter, _ *http.Request) {
	entries := s.ledger.All()
	counts := map[domain.Scenario]int{}
	for _, e := range entries {
		counts[e.Scenario]++
	}
	writeJSON(w, http.StatusOK, map[string]any{"counts": counts, "entries": entries})
}

// scenario lists the entries for one delivery, one per provider it was sent to.
func (s *Server) scenario(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_id"})
		return
	}
	entries := s.ledger.ForDelivery(id)
	if len(entries) == 0 {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v) // the client may be gone; nothing to do about it
}
