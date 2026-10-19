DROP INDEX inbox_due_idx;
CREATE INDEX inbox_due_idx ON inbox (next_attempt_at) WHERE state = 'WAITING_RETRY';
ALTER TABLE inbox DROP COLUMN channel;
