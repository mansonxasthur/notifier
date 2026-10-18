package domain

import "testing"

func TestScenarioValid(t *testing.T) {
	for _, s := range Scenarios() {
		if !s.Valid() {
			t.Errorf("%s should be valid", s)
		}
	}
	if Scenario("chaos").Valid() {
		t.Error("unknown scenario should be invalid")
	}
}

func TestFailureReasonValid(t *testing.T) {
	for _, r := range []FailureReason{
		ReasonMaxAttemptsExceeded, ReasonMaxClaimsExceeded, ReasonPublishFailed,
		ReasonProviderRejected, ReasonProviderFailed, ReasonBounced, ReasonInvalidDestination,
		ReasonExpired, ReasonNoProviderAvailable,
	} {
		if !r.Valid() {
			t.Errorf("%s should be valid", r)
		}
	}
	if FailureReason("MYSTERY").Valid() {
		t.Error("unknown reason should be invalid")
	}
}
