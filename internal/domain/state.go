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
)
