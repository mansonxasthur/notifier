package domain

// DeliveryState is the business state of a delivery: did the user get it?
type DeliveryState string

const (
	DeliveryPending    DeliveryState = "PENDING"
	DeliveryAccepted   DeliveryState = "ACCEPTED"
	DeliveryDelivered  DeliveryState = "DELIVERED"
	DeliveryFailed     DeliveryState = "FAILED"
	DeliveryUnresolved DeliveryState = "UNRESOLVED"
)

// deliveryMoves lists the forward moves a delivery may make. Terminal states have none.
// A timeout on the last provider ends a PENDING delivery UNRESOLVED without ever
// reaching ACCEPTED.
var deliveryMoves = map[DeliveryState][]DeliveryState{
	DeliveryPending:  {DeliveryAccepted, DeliveryDelivered, DeliveryFailed, DeliveryUnresolved},
	DeliveryAccepted: {DeliveryDelivered, DeliveryFailed, DeliveryUnresolved},
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

// Terminal reports whether no further move is possible from s.
func (s DeliveryState) Terminal() bool { return len(deliveryMoves[s]) == 0 }

// MovesInto returns the states a delivery may move to to from, in a fixed order. It is
// the guard of a conditional update: the row changes only while its state is one of these.
func MovesInto(to DeliveryState) []DeliveryState {
	var from []DeliveryState
	for _, s := range []DeliveryState{DeliveryPending, DeliveryAccepted} {
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
)

// Valid reports whether r is one of the enum values.
func (r FailureReason) Valid() bool {
	switch r {
	case ReasonMaxAttemptsExceeded, ReasonMaxClaimsExceeded, ReasonPublishFailed,
		ReasonProviderRejected, ReasonProviderFailed, ReasonBounced, ReasonInvalidDestination:
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
)

// StuckInbox is an inbox row left in PROCESSING past its threshold. ClaimCount is the
// fencing token for the sweep's update.
type StuckInbox struct {
	DeliveryID int64
	Provider   string
	ClaimCount int
}
