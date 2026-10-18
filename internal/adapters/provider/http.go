// Package provider holds the provider-side adapters: the HTTP client that sends messages
// and answers status lookups, and the registry that maps channels to providers.
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

// SendPayload is the JSON body of a send call. The simulated provider decodes the same type.
type SendPayload struct {
	DeliveryID int64          `json:"delivery_id"`
	Channel    domain.Channel `json:"channel"`
	Contact    string         `json:"contact"`
	Content    ContentPayload `json:"content"`
}

// ContentPayload carries either inline content or a template ID with variables, as given.
type ContentPayload struct {
	Text       string            `json:"text,omitempty"`
	Subject    string            `json:"subject,omitempty"`
	Body       string            `json:"body,omitempty"`
	TemplateID string            `json:"template_id,omitempty"`
	Variables  map[string]string `json:"variables,omitempty"`
}

// NewSendPayload converts a send request to its wire form.
func NewSendPayload(r ports.SendRequest) SendPayload {
	return SendPayload{
		DeliveryID: r.DeliveryID,
		Channel:    r.Channel,
		Contact:    r.Contact,
		Content: ContentPayload{
			Text: r.Content.Text, Subject: r.Content.Subject, Body: r.Content.Body,
			TemplateID: r.Content.TemplateID, Variables: r.Content.Variables,
		},
	}
}

// maxDrain bounds how much of a response body is read before the connection is reused.
const maxDrain = 64 << 10

var _ ports.Provider = (*HTTP)(nil)

// HTTP calls one provider over HTTP: POST <base>/send and GET <base>/status/<delivery_id>.
type HTTP struct {
	name         string
	base         string
	timeout      time.Duration
	statusLookup bool
	client       *http.Client
}

// Name implements ports.Provider.
func (h *HTTP) Name() string { return h.name }

// Send implements ports.Provider. The provider's configured timeout bounds the whole call.
func (h *HTTP) Send(ctx context.Context, req ports.SendRequest) error {
	body, err := json.Marshal(NewSendPayload(req))
	if err != nil {
		return &ports.SendError{Class: domain.FailureMessage, Err: fmt.Errorf("encode request: %w", err)}
	}
	callCtx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	hr, err := http.NewRequestWithContext(callCtx, http.MethodPost, h.base+"/send", bytes.NewReader(body))
	if err != nil {
		return &ports.SendError{Class: domain.FailureProvider, Err: err}
	}
	hr.Header.Set("Content-Type", "application/json; charset=utf-8")

	resp, err := h.client.Do(hr)
	if err != nil {
		return h.transportError(ctx, err)
	}
	defer closeBody(resp)

	return classifyStatus(resp.StatusCode)
}

// classifyStatus maps a send response to an error. Only the statuses that say the message
// itself is wrong are message-level. Every other failure, including 401, 403, 404, 408 and
// 429, describes the provider, its credentials or its load, so failover can still help.
func classifyStatus(code int) error {
	switch code {
	case http.StatusBadRequest, http.StatusRequestEntityTooLarge,
		http.StatusUnsupportedMediaType, http.StatusUnprocessableEntity:
		return &ports.SendError{Class: domain.FailureMessage, Err: statusError(code)}
	}
	if code >= 200 && code < 300 {
		return nil
	}
	return &ports.SendError{Class: domain.FailureProvider, Err: statusError(code)}
}

func statusError(code int) error { return errors.New("status " + strconv.Itoa(code)) }

// transportError classifies a failure to get any response. A cancelled caller (shutdown) is
// not a provider failure and comes back unclassified. A caller deadline that expires
// mid-call leaves the outcome unknown, so it counts as a timeout.
func (h *HTTP) transportError(parent context.Context, err error) error {
	if errors.Is(parent.Err(), context.Canceled) {
		return fmt.Errorf("send to %s: %w", h.name, err)
	}
	var ne net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()) {
		return &ports.SendError{Class: domain.FailureTimeout, Err: err}
	}
	return &ports.SendError{Class: domain.FailureProvider, Err: err}
}

// Lookup asks the provider whether it has the message for deliveryID: 200 means it has it
// and 404 means it does not. A provider without
// a status lookup, or one that answers 501, gives CheckUnknown without an error.
func (h *HTTP) Lookup(ctx context.Context, deliveryID int64) (ports.CheckResult, error) {
	if !h.statusLookup {
		return ports.CheckUnknown, nil
	}
	callCtx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	url := h.base + "/status/" + strconv.FormatInt(deliveryID, 10)
	hr, err := http.NewRequestWithContext(callCtx, http.MethodGet, url, nil)
	if err != nil {
		return ports.CheckUnknown, err
	}
	resp, err := h.client.Do(hr)
	if err != nil {
		return ports.CheckUnknown, fmt.Errorf("status lookup at %s: %w", h.name, err)
	}
	defer closeBody(resp)

	switch resp.StatusCode {
	case http.StatusOK:
		return ports.CheckAccepted, nil
	case http.StatusNotFound:
		return ports.CheckNotFound, nil
	case http.StatusNotImplemented:
		return ports.CheckUnknown, nil
	default:
		return ports.CheckUnknown, fmt.Errorf("status lookup at %s: status %d", h.name, resp.StatusCode)
	}
}

func closeBody(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrain))
	_ = resp.Body.Close()
}
