-- Every authenticated provider webhook, including duplicates and ignored ones.
-- applied says whether the event moved its delivery to a new state.
CREATE TABLE webhook_events (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    delivery_id bigint      NOT NULL REFERENCES deliveries (id),
    provider    text        NOT NULL,
    event_type  text        NOT NULL,
    payload     jsonb       NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    applied     boolean     NOT NULL
);

CREATE INDEX webhook_events_delivery_idx ON webhook_events (delivery_id);
