# Phase 2: Accept path: implementation plan

## Context

Phase 1 is merged: three entry points start and stop cleanly, migrations exist, `cmd/api` serves only `/healthz` and `/readyz`. Phase 2 (`docs/notification_system_implementation_plan.md` lines 28-80) makes the Dispatcher accept notifications: authenticate, validate per recipient, persist deliveries and outbox rows in one transaction, and answer with `accepted` / `duplicates` / `rejected`. It never touches the broker.

Done when: a 1,000-recipient batch with one duplicate and one invalid row returns the expected counts and the DB holds exactly the accepted rows.

Gap found in exploration: `config/config.yaml` and the decision log still say `batch_limit: 10000`; the implementation plan says 3,000 (the value you reduced it to). Step 1 fixes that.

## Workflow (CLAUDE.md)

On approval: copy this plan to `_plans/phase-2-accept-path.md`, create the GitHub issue "Phase 2: Accept path", branch `feat/<issue#>-accept-path`. One Conventional Commit per step; `make check` and the `code-review` skill before each; no Co-Authored footer; open a PR, never merge.

## Steps

### 1. Decision log and config: batch limit 3,000
- Edit the decision log (batch limit, "at most 10,000 recipients") first, then `config/config.yaml` `batch_limit: 3000`. Fix the log's example `type: "match_started"` to a configured type (`match_alert`).
- Record the Phase 2 decisions below in the log (status codes, reason codes, API key mechanism, inline content shape) before coding them.
- Add config fields: `accept.chunk_size` (default 5000), `accept.max_body_bytes`, and `auth.api_key` (a single key, loaded from env into config, never written in yaml). Extend `Validate()`; tests in `config_test.go`.

### 2. Domain types and validation (`internal/domain`)
- `Notification`, `Recipient`, `Content` (per-channel: SMS `{text}`; email/WhatsApp inline `{subject?, body}` or template `{template_id, variables}`), `Delivery`, and `RejectReason` enum.
- `Validate(request, cfg, now)` returns the valid recipients and `[]Rejection{recipient_id, reason}`. It checks: recipient limit (whole-request error), known `type`, every listed channel has content in an accepted shape, contact format per channel (email address; E.164 for SMS/WhatsApp), `expires_at` not in the past, and effective `expires_at` = request value or `now + type expiry`.
- In-memory de-duplication of repeated `recipient_id` within the payload (first occurrence wins; the repeat counts as a duplicate).
- Reuse `Channel`/`Valid()` in `domain/channel.go`. Reason codes: `INVALID_RECIPIENT` (from the log) plus new `INVALID_CONTACT`, `MISSING_CONTENT`, `INVALID_CONTENT`, `UNKNOWN_CHANNEL`, `EXPIRED_DEADLINE`, `DUPLICATE_RECIPIENT`. The enum lives in one Go file now; Phase 8 generates the OpenAPI enum from it.
- Pure table-driven unit tests, no DB.

### 3. Ports and Postgres adapter
- `internal/ports/notifications.go`: `NotificationStore.Create(ctx, *domain.Notification) (CreateResult, error)`. The `Notification` aggregate carries idempotency key, type, content and the validated `Deliveries`; `Create` sets `n.ID` on the passed notification (the new id, or the existing one on an idempotent replay). `CreateResult{Created, Duplicates}` are the delivery counts; the handler maps `Created` to the response's `accepted`. No auth port: the key is validated in middleware against config.
- `internal/adapters/postgres/notifications.go`: one `pgx` transaction (READ COMMITTED): two-step notification insert (`ON CONFLICT DO NOTHING RETURNING id`, then `SELECT` on no row), then deliveries+outbox per chunk using the `unnest` CTE from the implementation plan, rows sorted by `(recipient_id, channel)`. Created = `CommandTag.RowsAffected()` of the outer insert; duplicates = chunk rows minus created. If the idempotency key already existed, still run the chunks (the unique constraint turns every row into a duplicate; first request wins, payload not compared).
- Use `Pool.Pool()` from `adapters/postgres/pool.go`; keep the state strings from `domain/state.go`.

### 4. HTTP layer (`internal/httpserver`)
- `POST /v1/notifications` handler on the existing `ServeMux` (Go 1.22 patterns); API-key middleware reads `X-API-KEY` and compares it to the configured key (`subtle.ConstantTimeCompare`); no service identity (out of scope); body size cap via `http.MaxBytesReader`; strict JSON decode.
- Single and batch share one request shape (`recipients[]` of length 1..3000).
- Responses: `202 {"accepted","duplicates","rejected":[...]}` when at least one row is accepted or the whole request is a duplicate replay; `422` when every recipient is rejected; `400` malformed body; `401` bad/missing key; `413` over the recipient limit or body cap; `500` DB failure (safe to retry with the same key). 429 stays for Phase 6.
- Wire in `cmd/api/main.go`: compose `Health(...)` with the new routes (`Health` returns a bare mux, so mount both on one parent mux); `slog` per request with counts, no contact data or key.

### 5. Tests
- Move the testcontainers helpers (`newDB`, `newMigrate`, `up`) out of `postgres/schema_test.go` into a shared test-helper package so adapter tests reuse them.
- Integration (real Postgres, skipped under `-short`): replay of the same request creates no new rows; same key with different payload is a duplicate; overlapping concurrent requests do not deadlock; 3,000 recipients x 3 channels = 9,000 rows runs as two chunks in one transaction and rolls back fully on failure; outbox rows exist only for inserted deliveries.
- Handler tests with a fake store: each status code, auth failure, size cap.
- Acceptance test: the 1,000-recipient batch with one duplicate and one invalid row.

## Critical files
- Edit: `config/config.yaml`, `internal/config/config.go`, `cmd/api/main.go`, `internal/adapters/postgres/schema_test.go`, `docs/notification_system_design_decision_log.md`
- New: `internal/domain/{notification,validate,reason}.go`, `internal/ports/notifications.go`, `internal/adapters/postgres/notifications.go`, `internal/httpserver/{notifications,auth}.go`, `_plans/phase-2-accept-path.md`

## Verification
1. `make check` (fmt, vet, lint, test with `-race`; Docker running).
2. `go test -race -run TestCreate ./internal/adapters/postgres` for the integration cases.
3. `make up`, then `curl -X POST localhost:<port>/v1/notifications -H 'X-API-Key: ...'` with a small batch; run it twice and confirm the second returns `duplicates` and `psql` row counts for deliveries and outbox are unchanged.
4. Confirm `cmd/api` imports nothing from `adapters/rabbitmq`.

## Open points for approval (defaults chosen, change if you disagree)
- **API key (decided):** one key, header `X-API-KEY`, validated against the config value; service identity is out of scope, no migration. Note this deviates from the implementation plan line 33 ("service identity derived from the key"); step 1 records it in the decision log and the checklist item is amended.
- **Over-limit status:** 413. Alternatives: 422 or 400.
- **Inline content fields:** `{subject, body}` for email, `{body}` for WhatsApp. The log leaves these unspecified.
- **New reason codes** listed in step 2; the log defines only `INVALID_RECIPIENT`.
- **Idempotency replay:** returns `202` with all rows counted as duplicates (not the original response body).
