CREATE TABLE inbox (
    delivery_id     bigint      NOT NULL UNIQUE REFERENCES deliveries (id),
    state           text        NOT NULL,
    provider        text        NOT NULL,
    retries         integer     NOT NULL DEFAULT 0,
    claim_count     integer     NOT NULL DEFAULT 0,
    last_try        timestamptz NOT NULL DEFAULT now(),
    next_attempt_at timestamptz,
    failure_class   text,
    failure_reason  text
);

-- Sender scan for retries that have become due.
CREATE INDEX inbox_due_idx ON inbox (next_attempt_at) WHERE state = 'WAITING_RETRY';
-- Janitor sweep for claims that never finished.
CREATE INDEX inbox_stuck_idx ON inbox (last_try) WHERE state = 'PROCESSING';
