CREATE TABLE outbox (
    delivery_id     bigint      PRIMARY KEY REFERENCES deliveries (id),
    state           text        NOT NULL DEFAULT 'NEW',
    retries         integer     NOT NULL DEFAULT 0,
    last_try        timestamptz NOT NULL DEFAULT now(),
    next_attempt_at timestamptz,
    failure_reason  text
);

-- Relay scan: an empty next_attempt_at means due now, so NULLs sort first.
CREATE INDEX outbox_due_idx ON outbox (next_attempt_at NULLS FIRST) WHERE state = 'NEW';
-- Janitor sweep for claims that never finished.
CREATE INDEX outbox_stuck_idx ON outbox (last_try) WHERE state = 'PROCESSING';
