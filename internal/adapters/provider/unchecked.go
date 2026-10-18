// Package provider holds the provider-side adapters.
package provider

import (
	"context"

	"github.com/mansonxasthur/notifier/internal/ports"
)

var _ ports.ProviderChecker = Unchecked{}

// Unchecked is a ProviderChecker that never has an answer. It stands in until the provider
// clients exist, so recovery treats every stuck row as "may not have been sent".
type Unchecked struct{}

// Check implements ports.ProviderChecker.
func (Unchecked) Check(context.Context, string, int64) (ports.CheckResult, error) {
	return ports.CheckUnknown, nil
}
