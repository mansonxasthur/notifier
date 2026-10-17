CREATE TABLE deliveries (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    notification_id bigint      NOT NULL REFERENCES notifications (id),
    recipient_id    text        NOT NULL,
    channel         text        NOT NULL,
    contact         jsonb       NOT NULL,
    expires_at      timestamptz NOT NULL,
    state           text        NOT NULL DEFAULT 'PENDING',
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (notification_id, recipient_id, channel)
);
