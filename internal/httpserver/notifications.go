package httpserver

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/mansonxasthur/notifier/internal/httpserver/requests"
	"github.com/mansonxasthur/notifier/internal/ports"
)

// Error codes returned in the body of a request-level failure.
const (
	codeUnauthorized       = "UNAUTHORIZED"
	codeMalformedBody      = "MALFORMED_BODY"
	codeBodyTooLarge       = "BODY_TOO_LARGE"
	codeTooManyRecipients  = "TOO_MANY_RECIPIENTS"
	codeNoRecipients       = "NO_RECIPIENTS"
	codeUnknownType        = "UNKNOWN_TYPE"
	codeExpiredDeadline    = "EXPIRED_DEADLINE"
	codeInvalidIdempotency = "INVALID_IDEMPOTENCY_KEY"
	codeRateLimited        = "RATE_LIMITED"
	codeInternal           = "INTERNAL"
)

// requestErrors maps a whole-request validation failure to its status and code.
var requestErrors = []struct {
	err    error
	status int
	code   string
}{
	{requests.ErrTooManyRecipients, http.StatusRequestEntityTooLarge, codeTooManyRecipients},
	{requests.ErrNoRecipients, http.StatusUnprocessableEntity, codeNoRecipients},
	{requests.ErrUnknownType, http.StatusUnprocessableEntity, codeUnknownType},
	{requests.ErrDeadlinePast, http.StatusUnprocessableEntity, codeExpiredDeadline},
	{requests.ErrInvalidIdempotencyKey, http.StatusUnprocessableEntity, codeInvalidIdempotency},
}

// NotificationsOptions configures the notification endpoint.
type NotificationsOptions struct {
	Log   *slog.Logger
	Store ports.NotificationStore
	// APIKey is the key producers must send in X-API-KEY.
	APIKey string
	Policy requests.Policy
	// Limiter is the global intake limit, charged in deliveries.
	Limiter ports.Limiter
	// MaxBodyBytes caps the request body.
	MaxBodyBytes int64
	// Now is the clock; it defaults to time.Now.
	Now func() time.Time
}

// RegisterNotifications adds POST /v1/notifications to mux, behind the API key check.
// It refuses an empty key, which would leave the endpoint answering 401 to everyone.
func RegisterNotifications(mux *http.ServeMux, o NotificationsOptions) error {
	if o.APIKey == "" {
		return errors.New("register notifications: API key is empty")
	}
	if o.Limiter == nil {
		return errors.New("register notifications: limiter is nil, which would leave intake unlimited")
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	mux.Handle("POST /v1/notifications", requireAPIKey(o.APIKey, http.HandlerFunc(o.create)))
	return nil
}

type createResponse struct {
	Accepted   int                  `json:"accepted"`
	Duplicates int                  `json:"duplicates"`
	Rejected   []requests.Rejection `json:"rejected"`
}

func (o NotificationsOptions) create(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeRequest(w, r, o.MaxBodyBytes)
	if !ok {
		return
	}

	v, err := req.Validate(o.Policy, o.Now())
	if err != nil {
		for _, e := range requestErrors {
			if errors.Is(err, e.err) {
				writeError(w, e.status, e.code)
				return
			}
		}
		o.Log.ErrorContext(r.Context(), "unmapped validation error", "error", err)
		writeError(w, http.StatusInternalServerError, codeInternal)
		return
	}

	resp := createResponse{Duplicates: v.Duplicates, Rejected: v.Rejected}
	if resp.Rejected == nil {
		resp.Rejected = []requests.Rejection{}
	}
	if len(v.Notification.Deliveries) == 0 {
		writeJSON(w, http.StatusUnprocessableEntity, resp)
		return
	}

	// Charged after validation, so only the deliveries that would be stored count, and before
	// any write, so a refused request leaves nothing behind. All or nothing: a request is
	// never partly accepted.
	if !o.admit(w, r, len(v.Notification.Deliveries)) {
		return
	}

	res, err := o.Store.Create(r.Context(), v.Notification)
	if err != nil {
		o.Log.ErrorContext(r.Context(), "create notification", "error", err)
		writeError(w, http.StatusInternalServerError, codeInternal)
		return
	}
	resp.Accepted = res.Created
	resp.Duplicates += res.Duplicates

	o.Log.InfoContext(r.Context(), "notification stored",
		"notification_id", v.Notification.ID, "type", req.Type,
		"accepted", resp.Accepted, "duplicates", resp.Duplicates, "rejected", len(resp.Rejected))
	writeJSON(w, http.StatusAccepted, resp)
}

// admit charges n deliveries to the global limit. It writes the response itself and returns
// false when the request is refused, or when the limiter could not decide.
func (o NotificationsOptions) admit(w http.ResponseWriter, r *http.Request, n int) bool {
	d, err := o.Limiter.Allow(r.Context(), n)
	if err != nil {
		// The fallback only fails when the caller's own context ended, so nobody is likely
		// waiting; a status is still written, because net/http would send an empty 200.
		if r.Context().Err() == nil {
			o.Log.ErrorContext(r.Context(), "rate limiter failed", "error", err)
		}
		writeError(w, http.StatusInternalServerError, codeInternal)
		return false
	}
	if d.Allowed {
		return true
	}
	w.Header().Set("Retry-After", retryAfterSeconds(d.RetryAfter))
	writeError(w, http.StatusTooManyRequests, codeRateLimited)
	return false
}

// retryAfterSeconds renders a wait as whole seconds, rounded up and never below one, because
// Retry-After has no finer unit and "0" would invite an immediate retry.
func retryAfterSeconds(wait time.Duration) string {
	return strconv.FormatInt(max(1, int64((wait+time.Second-1)/time.Second)), 10)
}

// decodeRequest reads one JSON request from the body. It writes the error response itself
// and returns false when the body is too large, malformed, has unknown fields, or has
// anything after the first JSON value.
func decodeRequest(w http.ResponseWriter, r *http.Request, maxBytes int64) (requests.CreateNotificationRequest, bool) {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBytes))
	dec.DisallowUnknownFields()

	var req requests.CreateNotificationRequest
	err := dec.Decode(&req)
	if err == nil {
		_, extra := dec.Token()
		switch {
		case errors.Is(extra, io.EOF):
		case extra != nil:
			err = extra
		default:
			err = errors.New("unexpected data after the JSON body")
		}
	}
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, codeBodyTooLarge)
		} else {
			writeError(w, http.StatusBadRequest, codeMalformedBody)
		}
		return requests.CreateNotificationRequest{}, false
	}
	return req, true
}

func writeError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]string{"error": code})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
