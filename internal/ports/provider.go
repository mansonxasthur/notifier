package ports

import "context"

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
