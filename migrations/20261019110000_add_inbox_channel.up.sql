-- The delivery's channel, copied when the row is created, so the sender's per-channel scan
-- can find a channel's due rows from one index without joining deliveries.
ALTER TABLE inbox ADD COLUMN channel text;
UPDATE inbox i SET channel = d.channel FROM deliveries d WHERE d.id = i.delivery_id;
ALTER TABLE inbox ALTER COLUMN channel SET NOT NULL;

DROP INDEX inbox_due_idx;
CREATE INDEX inbox_due_idx ON inbox (channel, next_attempt_at) WHERE state = 'WAITING_RETRY';
