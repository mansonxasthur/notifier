-- Why a delivery ended FAILED; values come from the FailureReason enum in internal/domain.
ALTER TABLE deliveries ADD COLUMN failure_reason text;
