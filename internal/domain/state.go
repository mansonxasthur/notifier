package domain

import "time"

// DeliveryState is the business state of a delivery: did the user get it?
type DeliveryState string

const (
	DeliveryPending    DeliveryState = "PENDING"
	DeliveryAccepted   DeliveryState = "ACCEPTED"
	DeliveryDelivered  DeliveryState = "DELIVERED"
	DeliveryFailed     DeliveryState = "FAILED"
	DeliveryUnresolved DeliveryState = "UNRESOLVED"
)

// deliveryMoves lists the moves a delivery may make; this is the only place the rule lives.
// The sender's own updates are deliberately narrower: they move a delivery out of PENDING
// only, because a delivery that a webhook already advanced is not theirs to change.
// A timeout on the last provider ends a PENDING delivery UNRESOLVED without ever reaching
// ACCEPTED. UNRESOLVED means "not learned yet", so a later webhook may replace it with
// DELIVERED or FAILED, but nothing moves a delivery to UNRESOLVED from those two, which
// have no moves.
var deliveryMoves = map[DeliveryState][]DeliveryState{
	DeliveryPending:    {DeliveryAccepted, DeliveryDelivered, DeliveryFailed, DeliveryUnresolved},
	DeliveryAccepted:   {DeliveryDelivered, DeliveryFailed, DeliveryUnresolved},
	DeliveryUnresolved: {DeliveryDelivered, DeliveryFailed},
}

// allDeliveryStates lists every state in a fixed order, for building guards from the table.
var allDeliveryStates = []DeliveryState{
	DeliveryPending, DeliveryAccepted, DeliveryDelivered, DeliveryFailed, DeliveryUnresolved,
}

// CanMoveTo reports whether a delivery in state s may move to state to.
func (s DeliveryState) CanMoveTo(to DeliveryState) bool {
	for _, next := range deliveryMoves[s] {
		if next == to {
			return true
		}
	}
	return false
}

// Terminal reports whether no further move is possible from s. UNRESOLVED is not terminal:
// a late webhook can still settle it.
func (s DeliveryState) Terminal() bool { return len(deliveryMoves[s]) == 0 }

// MovesInto returns the states a delivery may move to to from, in a fixed order. It is
// the guard of a conditional update: the row changes only while its state is one of these.
func MovesInto(to DeliveryState) []DeliveryState {
	var from []DeliveryState
	for _, s := range allDeliveryStates {
		if s.CanMoveTo(to) {
			from = append(from, s)
		}
	}
	return from
}

// OutboxState is the operational state of an outbox row: has the work been published?
type OutboxState string

const (
	OutboxNew        OutboxState = "NEW"
	OutboxProcessing OutboxState = "PROCESSING"
	OutboxPublished  OutboxState = "PUBLISHED"
	OutboxDead       OutboxState = "DEAD"
)

// InboxState is the operational state of an inbox row: has the work been sent?
type InboxState string

const (
	InboxProcessing   InboxState = "PROCESSING"
	InboxWaitingRetry InboxState = "WAITING_RETRY"
	InboxDone         InboxState = "DONE"
	InboxDead         InboxState = "DEAD"
)

// FailureClass records how the last send attempt failed.
type FailureClass string

const (
	FailureMessage  FailureClass = "message"
	FailureProvider FailureClass = "provider"
	FailureTimeout  FailureClass = "timeout"
	// FailureNone is no class, for an end that was not a failed attempt, such as EXPIRED. It is stored as NULL.
	FailureNone FailureClass = ""
)

// FailureReason is the stored reason a row or delivery ended in a terminal failure.
type FailureReason string

const (
	ReasonMaxAttemptsExceeded FailureReason = "MAX_ATTEMPTS_EXCEEDED"
	ReasonMaxClaimsExceeded   FailureReason = "MAX_CLAIMS_EXCEEDED"
	ReasonPublishFailed       FailureReason = "PUBLISH_FAILED"
	// ReasonProviderRejected: the provider refused the message itself (a 4xx).
	ReasonProviderRejected FailureReason = "PROVIDER_REJECTED"
	// ReasonProviderFailed: the provider reported a failure after accepting the message.
	// It also stands in for a webhook reason the enum does not know.
	ReasonProviderFailed     FailureReason = "PROVIDER_FAILED"
	ReasonBounced            FailureReason = "BOUNCED"
	ReasonInvalidDestination FailureReason = "INVALID_DESTINATION"
	// ReasonExpired: the delivery's deadline passed before it was sent.
	ReasonExpired FailureReason = "EXPIRED"
	// ReasonNoProviderAvailable: the breaker is open for every provider left to try.
	ReasonNoProviderAvailable FailureReason = "NO_PROVIDER_AVAILABLE"
)

// Valid reports whether r is one of the enum values.
func (r FailureReason) Valid() bool {
	switch r {
	case ReasonMaxAttemptsExceeded, ReasonMaxClaimsExceeded, ReasonPublishFailed,
		ReasonProviderRejected, ReasonProviderFailed, ReasonBounced, ReasonInvalidDestination,
		ReasonExpired, ReasonNoProviderAvailable:
		return true
	}
	return false
}

// OutboxClaim is an outbox row a relay holds. Retries is the fencing token: the claim is
// only valid while the row still carries this value.
type OutboxClaim struct {
	DeliveryID int64
	Channel    Channel
	Retries    int
}

// InboxClaim is the outcome of a sender's claim. ClaimCount is the fencing token of a won
// claim: the sender's later updates apply only while the row still carries it.
type InboxClaim struct {
	Result     InboxClaimResult
	ClaimCount int
	// Provider is the provider for this attempt: the row's current one, which a failover moves.
	Provider string
	// Retries is how many retries the row has already used on its provider.
	Retries int
}

// InboxClaimResult says what a sender learned when it tried to claim a delivery.
type InboxClaimResult int

const (
	// InboxClaimWon: this sender owns the send.
	InboxClaimWon InboxClaimResult = iota
	// InboxClaimDuplicate: another sender owns it, or it is already done.
	InboxClaimDuplicate
	// InboxClaimDead: the delivery exceeded its claim cap and was failed.
	InboxClaimDead
	// InboxClaimMissing: no delivery has this ID, so there is nothing to claim.
	InboxClaimMissing
)

// InboxOutcomeKind says how a sender's attempt ended, and so which update settles its inbox row.
type InboxOutcomeKind int

const (
	// OutcomeComplete: the provider took the message. The row ends DONE, the delivery ACCEPTED.
	OutcomeComplete InboxOutcomeKind = iota
	// OutcomeFail: no retry can fix it. The row ends DEAD, the delivery FAILED with Reason.
	OutcomeFail
	// OutcomePark: a failed attempt to retry on the same provider, due at Next.
	OutcomePark
	// OutcomeSwitch: move to Provider with retries reset, due at Next.
	OutcomeSwitch
	// OutcomeDefer: the provider's rate limit had no token, so retry on the same provider at Next.
	OutcomeDefer
	// OutcomeUnresolve: the last provider's answer cannot be known. The row ends DEAD, the delivery UNRESOLVED.
	OutcomeUnresolve
)

// InboxOutcome is the result of one claimed attempt, to be written to its inbox row. ClaimCount
// is the fencing token: the write applies only while the row still carries it. Which of the other
// fields are used depends on Kind.
type InboxOutcome struct {
	Kind       InboxOutcomeKind
	DeliveryID int64
	ClaimCount int
	// Class is the failure class; empty is stored as NULL where the row keeps one.
	Class FailureClass
	// Reason is the failure reason of an OutcomeFail.
	Reason FailureReason
	// Provider is the provider an OutcomeSwitch moves to.
	Provider string
	// Next is when Park, Switch and Defer are due.
	Next time.Time
}

// DueInbox is a retry the sender's scan claimed because its next_attempt_at had passed.
// ClaimCount is the fencing token of the claim the scan took.
type DueInbox struct {
	DeliveryID int64
	Provider   string
	ClaimCount int
	// Retries is how many retries the row has already used on Provider.
	Retries int
}

// DefaultMaxDeferrals is how many times a row may be put back for a provider's rate limit
// before each further deferral counts as a claim.
const DefaultMaxDeferrals = 1000

// ClaimLimits bounds how often an inbox row may be claimed.
type ClaimLimits struct {
	// MaxClaims is how many claims a row may use before it ends DEAD.
	MaxClaims int
	// MaxDeferrals is how many deferrals a row may have without costing a claim.
	MaxDeferrals int
}

// Claims returns limits of max claims with the default allowance of free deferrals.
func Claims(max int) ClaimLimits {
	return ClaimLimits{MaxClaims: max, MaxDeferrals: DefaultMaxDeferrals}
}

// Used is how many of a row's claims count against MaxClaims: all of them, except the ones that
// only deferred the row, up to MaxDeferrals.
func (l ClaimLimits) Used(claims, deferrals int) int { return claims - min(deferrals, l.MaxDeferrals) }

// StuckInbox is an inbox row left in PROCESSING past its threshold. ClaimCount is the
// fencing token for the sweep's update.
type StuckInbox struct {
	DeliveryID int64
	Provider   string
	ClaimCount int
	// Retries is how many retries the row has spent on its provider.
	Retries int
	// ExpiresAt is the delivery's deadline.
	ExpiresAt time.Time
}
