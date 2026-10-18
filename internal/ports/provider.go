package ports

import (
	"context"
	"errors"
	"fmt"

	"github.com/mansonxasthur/notifier/internal/domain"
)

// CheckResult is what a provider says about a delivery it may have been sent.
type CheckResult int

const (
	// CheckUnknown: no usable answer (no lookup, or it failed).
	CheckUnknown CheckResult = iota
	// CheckAccepted: the provider has the message.
	CheckAccepted
	// CheckNotFound: the provider has no record of it.
	CheckNotFound
)

// ProviderChecker asks a provider, by delivery_id, whether it received a message.
type ProviderChecker interface {
	Check(ctx context.Context, provider string, deliveryID int64) (CheckResult, error)
}

// SendRequest is one send attempt. DeliveryID is the reference key the provider echoes in
// its webhooks and answers status lookups by.
type SendRequest struct {
	DeliveryID int64
	Channel    domain.Channel
	Contact    string
	Content    domain.Content
}

// Provider sends messages for one channel through one external provider.
type Provider interface {
	Name() string
	// Send makes one call. A nil return means the provider accepted the message. A failure
	// is a *SendError carrying its class; any other error (such as the caller's context
	// being cancelled) is not a statement about the provider.
	Send(ctx context.Context, req SendRequest) error
}

// SendError is a failed send attempt, classified for the retry decision.
type SendError struct {
	Class domain.FailureClass
	Err   error
}

func (e *SendError) Error() string { return fmt.Sprintf("%s failure: %v", e.Class, e.Err) }
func (e *SendError) Unwrap() error { return e.Err }

// ClassOf returns the failure class of err, and false when err is not a *SendError.
func ClassOf(err error) (domain.FailureClass, bool) {
	var se *SendError
	if errors.As(err, &se) {
		return se.Class, true
	}
	return "", false
}
