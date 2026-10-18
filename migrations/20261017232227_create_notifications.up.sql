CREATE TABLE notifications (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    idempotency_key text        NOT NULL UNIQUE,
    type            text        NOT NULL,
    content         jsonb       NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);
