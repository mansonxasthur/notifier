# Phase 4: Sender and simulated provider: implementation plan

(On approval, copy to `_plans/phase-4-sender-simulated-provider.md`, per CLAUDE.md.)

## Context

Phases 1-3 get every accepted delivery onto the queue and into a claimed inbox row, but the sender only logs (`sender.LogOnly`). Phase 4 makes the path real end to end: sender makes one provider call per attempt, the simulated provider answers in configurable modes, deliveries move through guarded states, and webhooks (served by the API) finish them. Done when a message flows request → `DELIVERED` via the simulator, including the late-webhook path.

**Boundary with Phase 5 (default):** Phase 4 does one call to the channel's primary provider per claim and settles the outcome simply. Retries, backoff scheduling, failover, breaker, expiry check and the timeout-lookup loop are Phase 5. In Phase 4 a provider failure or timeout therefore ends the attempt with the failure class recorded and the inbox row put in `WAITING_RETRY` with `failure_class` set (no scan yet), and a message-level error ends `FAILED`. This keeps Phase 4 a thin slice while leaving Phase 5 the hooks.

## Workflow (CLAUDE.md)

1. Wait for approval of this plan. 2. `gh issue create` on origin. 3. Branch off master for the issue (current branch is the Phase 3 one). 4. Record decision changes in `docs/notification_system_design_decision_log.md` first (see step 1). 5. `make check` + `code-review` skill before each commit; Conventional Commits, no Co-Authored footer. 6. Open a PR; never merge.

## Steps

### 1. Decision log and config
- Log the Phase 4 choices: per-provider webhook authentication via a port, `webhook_events` table, global random simulation with a scenario registry, Phase 4/5 boundary, new `FailureReason` enum values.
- Config (`internal/config/config.go`, `config/config.yaml`): per provider, a `webhook` block: `auth` (scheme name, e.g. `hmac_signature`, `shared_secret_header`), `header` (name of the header carrying the signature/secret), and `secret_env` (name of the env var holding the secret; the value never lives in YAML). Validation: every provider needs a known scheme and its env var set when the api runs. Also `webhook_url` for the simulator to call back, and a global `simprovider` block (see step 5). Unknown YAML keys are errors, so all keys must be declared.
- `FailureReason` stays a closed enum in `internal/domain/state.go` (OpenAPI enum comes in Phase 8). Add values needed now: `PROVIDER_REJECTED` (message-level, provider 4xx) and webhook-reported reasons such as `PROVIDER_FAILED`, `BOUNCED`, `INVALID_DESTINATION`. Unknown provider reason strings are mapped to `PROVIDER_FAILED`; the raw text survives in the `webhook_events` payload.

### 2. Provider port and registry
- `internal/ports/provider.go`: add `Provider` with `Send(ctx, SendRequest) (SendResult, error)`; `SendRequest{DeliveryID, Channel, Contact, Content}`; errors classified into `domain.FailureClass` (message / provider / timeout) via a typed error. Keep `ProviderChecker` and add `Lookup` support to the real checker.
- `internal/adapters/provider/`: HTTP adapter (JSON POST with `delivery_id` as reference key, per-provider `Timeout` from config, UTF-8 body; 4xx→message, 5xx/refused→provider, deadline→timeout); `Registry` mapping channel → providers in config order, selecting the simulator URL when `simulation: true`; HTTP `Checker` calling `GET /status/{delivery_id}` honoring `status_lookup` (replaces `provider.Unchecked` in `cmd/relay`).

### 3. Delivery state machine and stores
- `internal/domain`: `CanTransition(from, to DeliveryState)` encoding PENDING→ACCEPTED|DELIVERED|FAILED, ACCEPTED→DELIVERED|FAILED|UNRESOLVED; DELIVERED and FAILED stay final, and a late webhook can settle UNRESOLVED. Unit-tested table.
- Migrations (`make migrate-create`): (a) add `failure_reason text` (nullable, values from the enum) to `deliveries`; (b) new `webhook_events` table: `id bigserial`, `delivery_id` FK → `deliveries`, `provider text`, `event_type text`, `payload jsonb`, `received_at timestamptz`, `applied boolean` (whether it moved the delivery), all NOT NULL; index on `delivery_id`. Every event is stored, including duplicates and ignored ones, so the audit trail is complete.
- New port `ports.WebhookEventStore`: `Record(ctx, event) error`, written in the same transaction as the conditional delivery `Transition` so `applied` is exact.
- New port `ports.DeliveryStore`: `Load(ctx, id) (DeliveryForSend, error)` (channel, contact, content from notification, expires_at), `Transition(ctx, id, to, reason)` as a conditional `UPDATE ... WHERE state IN (allowed from-states)` returning whether it applied (webhook idempotence, first terminal wins). Postgres impl in `internal/adapters/postgres/deliveries.go`.
- Extend `InboxStore` (and Postgres impl) with fenced methods on `claim_count`: `Complete` (→DONE + delivery ACCEPTED, reuse existing `Accept` CTE), `Fail` (inbox DEAD + delivery FAILED with reason + failure_class), `Park` (→WAITING_RETRY with `failure_class`, `retries`, `next_attempt_at`).

### 4. Sender
- Replace `sender.Send`/`LogOnly` in `internal/sender/intake.go`: the send func receives channel and claimed provider, loads the delivery, calls the registry's provider once, and settles via the inbox methods above. Handler contract unchanged: ack after the claim write.
- Wire `cmd/sender/main.go` with registry + stores. Log one line per attempt: `delivery_id`, provider, failure class, outcome; never contacts (invariant from the log).
- Leave `Sender.PollInterval` scan to Phase 5.

### 5. Simulated provider (`cmd/simprovider`, `internal/simprovider`)
- Endpoints: `POST /send` (accepts inline content or template ID + variables), `GET /status/{delivery_id}` (404 if unknown; disabled per provider config → 501), `GET /scenarios` and `GET /scenarios/{delivery_id}` (the registry), plus health.
- **Global mode** in the `simprovider` block: `mode: random` picks a scenario per request from configured `weights` (seeded RNG, `seed` configurable for reproducible runs), or `mode: <scenario>` pins one scenario for everything. Scenarios: `succeed`, `fail_permanent` (400), `fail_transient` (503), `slow` (delay under timeout), `timeout` (delay over timeout), `late_failure` (202, then a failure webhook after a delay). Success also posts a `DELIVERED` webhook after a short delay.
- **Scenario registry:** each chosen scenario is registered against `delivery_id` + provider (in memory, behind the endpoints above, and logged), so tests and load runs can assert that the system's final state matches what the simulator decided. A repeat send of the same `delivery_id` replays the registered scenario's result instead of rolling again.
- Webhook posts are authenticated per provider with the same scheme/header/secret config the API validates against (the simulator signs using the provider's `webhook` block) and echo `delivery_id`.

### 6. Webhook endpoint (API)
- New port `ports.WebhookAuthenticator`: `Authenticate(r *http.Request, body []byte) error`. Adapters in `internal/adapters/webhookauth/`, one per scheme (`hmac_signature`: HMAC-SHA256 of the raw body in the configured header; `shared_secret_header`: constant-time compare of the header with the secret), built from each provider's `webhook` config by a registry keyed by provider name. A new provider with a different scheme means a new adapter, no handler change.
- `httpserver.RegisterWebhooks(mux, opts)` modeled on `RegisterNotifications`; route `POST /v1/webhooks/{provider}`. Handler: size-limited raw body read (before JSON decode, since `decodeRequest` consumes the body) → look up the provider's authenticator (unknown provider → 404) → authenticate (fail → 401) → parse `{delivery_id, status, reason}` (400 on malformed) → in one transaction record the `webhook_events` row and run the conditional `Transition` → 200 even when the transition is ignored (duplicate/stale). An early webhook (delivery still PENDING) goes straight to DELIVERED/FAILED; first terminal webhook wins.
- Wire into `cmd/api/main.go`.

### 7. Compose and docs
- `docker-compose.yml`: the per-provider webhook secret env vars named in config (`secret_env`) for api and simprovider. Update CLAUDE.md Status and add a `docs/sessions/` log. Tick Phase 4 boxes in the implementation plan.

## Critical files
`internal/ports/{provider,inbox}.go`, `internal/adapters/provider/*` (new), `internal/adapters/postgres/{inbox,deliveries}.go`, `internal/sender/intake.go`, `internal/httpserver/webhooks.go` (new), `internal/simprovider/*` (new), `internal/domain/state.go`, `internal/config/config.go`, `cmd/{api,relay,sender,simprovider}/main.go`, `migrations/`.

## Verification
- Unit: state-transition table, each `WebhookAuthenticator` (valid, wrong secret, missing header, tampered body), HTTP provider error classification, simulator scenario selection (seeded random, pinned) and registry replay.
- Integration (existing `pgtest`, `rmqtest`, httptest simulator): per scenario, sender ends in the expected inbox/delivery state (success→ACCEPTED→DELIVERED; permanent→FAILED; transient/timeout→WAITING_RETRY with class; late_failure→ACCEPTED→FAILED via webhook); webhook before ACCEPTED write; duplicate/out-of-order webhooks all land in `webhook_events` with correct `applied`; bad auth 401; unknown provider 404; failure reason always an enum value; Arabic content reaches the provider intact.
- End to end: `make up` in random mode, POST a batch, then compare final delivery states against `GET /scenarios` (every delivery's state must be consistent with its registered scenario); repeat pinned to `late_failure`. `make check` green.

## Decisions taken from review feedback
- Webhook authentication is per provider behind a `WebhookAuthenticator` port (signature header or secret env var varies by provider).
- Simulation uses one global mode that can be random, with every outcome registered per delivery.
- Webhook payloads are persisted in a `webhook_events` table linked to `delivery_id`.
- `failure_reason` is a closed enum.

- Phase 4/5 boundary (confirmed): single attempt; provider failures and timeouts are parked in `WAITING_RETRY` with `failure_class`; no scan, retry or failover until Phase 5.

## Deviations made while building (also recorded in the decision log)
- `Provider.Send` returns only an `error`; the empty `SendResult` was dropped.
- The simulator serves every provider from one service, so its routes carry the provider name: `POST /providers/{name}/send` and `GET /providers/{name}/status/{id}`.
- `PENDING` may move to `UNRESOLVED`, because a timeout on the last provider never reaches `ACCEPTED`.
- `WebhookStore.Apply` replaces `WebhookEventStore.Record` plus `DeliveryStore.Transition`: the event and the move share one transaction.
- `Park` adds one to `retries`, and the backoff exponent is the retries already spent.
- `Claim` returns the claim count (the fencing token) and the retries used.
- A webhook applies only to a delivery on the channel its provider serves.
- The simulator sends the webhook of a slow or timed-out call after the response it delays.
