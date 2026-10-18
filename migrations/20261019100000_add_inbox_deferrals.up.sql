-- Times a sender put the row back because the provider's rate limit had no token. A deferral
-- is not a failed attempt: it spends no retry, and the claim cap counts claim_count minus this,
-- so the column can never exceed claim_count.
ALTER TABLE inbox ADD COLUMN deferrals integer NOT NULL DEFAULT 0 CHECK (deferrals >= 0);
ALTER TABLE inbox ADD CONSTRAINT inbox_deferrals_within_claims CHECK (deferrals <= claim_count);
