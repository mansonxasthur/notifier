-- When the delivery moved from PENDING to ACCEPTED, so request-to-ACCEPTED latency
-- (accepted_at - created_at) can be read back from the table after a load run. Rows accepted
-- before this migration, and deliveries a webhook finishes first, have none.
ALTER TABLE deliveries ADD COLUMN accepted_at timestamptz;
