# Phase 3: Relay, queue and janitor: implementation plan

## Context

Phase 2 (accept path) is merged on `feat/3-accept-path`: the API writes `deliveries` + `outbox` rows (`state='NEW'`) in one transaction and never touches the broker. Nothing consumes the outbox yet: `cmd/relay` and `cmd/sender` only connect Postgres + RabbitMQ and wait (`cmd/relay/main.go`, `cmd/sender/main.go`).

Goal (plan, Phase 3): the relay and janitor get every accepted delivery onto a queue at least once; a crashed relay or sender loses nothing.
Done when: killing a relay mid-batch, or restarting the broker, ends with every delivery processed and nothing stuck.

Already in place and reused:
- Config keys: `relay.{poll_interval,batch_size,max_attempts,backoff,janitor.{interval,outbox_stuck_after,inbox_stuck_margin}}`, `sender.{poll_interval,max_claims}`, per-provider `timeout` (`internal/config/config.go`, `config/config.yaml`).
- Schema: `outbox` (PK `delivery_id`, `state NEW|PROCESSING|PUBLISHED|DEAD`, `retries`, `last_try`, `next_attempt_at`, `failure_reason`, partial indexes `outbox_due_idx`, `outbox_stuck_idx`); `inbox` (unique `delivery_id`, `state PROCESSING|WAITING_RETRY|DONE|DEAD`, `provider`, `retries`, `claim_count`, `last_try`, ..., indexes `inbox_due_idx`, `inbox_stuck_idx`). No migration needed (outbox has no channel column; the relay joins `deliveries`).
- `rabbitmq.Conn` (`internal/adapters/rabbitmq/conn.go`, exposes `Connection()`, no channel/topology/reconnect yet); `amqp091-go` already in `go.mod`.
- `postgres.Pool` + raw pgx pattern (`internal/adapters/postgres/notifications.go`); `pgtest.NewMigratedDB` for tests; `lifecycle.Component` / `Failer` / `lifecycle.Retry`; domain enums in `internal/domain/state.go`.

Gaps found in exploration (decided below):
- No `Publisher`/`Consumer`/outbox/inbox ports, no RabbitMQ test helper, no backoff helper, no reconnect logic.
- Checklist asks for a "provider check before any resend" in the inbox sweep, but the `Provider` port arrives in Phase 4 -> Phase 3 defines a small `ProviderChecker` port with a "cannot check" no-op adapter.
- Queue names, message wire format, prefetch, DEAD reason strings and the `next_attempt_at` formula are not specified in the log.
- `CLAUDE.md` Status paragraph is stale (says Phase 1 only).

## Workflow (CLAUDE.md)

1. Copy this plan to `_plans/phase-3-relay-queue-janitor.md`; wait for engineer approval.
2. `gh issue create` "Phase 3: Relay, queue and janitor"; branch `feat/<issue#>-relay-queue-janitor` from `master` (or from the Phase 2 branch if unmerged).
3. One Conventional Commit per step below; before each: `make check` + `code-review` skill. No `Co-Authored` footer.
4. Tick the Phase 3 checklist in the implementation plan, add `docs/sessions/` log, refresh CLAUDE.md Status; open a PR, never merge.

## Steps

### 1. Decision log and config
Record first in `docs/notification_system_design_decision_log.md` (section "Relay, queue and janitor"):
- Queue topology: default exchange, one durable queue per channel named after the channel only (`email`, `sms`, `whatsapp`); persistent messages whose body holds only the `delivery_id`.
- `outbox.next_attempt_at` is NULL by default (due now); it is set only when a retry is scheduled (publish failure or janitor reclaim) and is never carried in the message.
- Retry/release formula: `delay = min(max, initial * multiplier^(retries-1))`, jittered by `±jitter` fraction; one shared helper for relay publish-failure and janitor reclaim.
- Attempt cap: a claim that would take `retries` above `relay.max_attempts` ends the row `DEAD` with `failure_reason` (`MAX_ATTEMPTS_EXCEEDED`); janitor reclaims count (they follow a claim). Reason strings come from the reason enum used by the API (define in one place).
- Sender intake semantics (below) and the `sender.max_claims` meaning (cap on `claim_count`; over it -> inbox `DEAD`, delivery `FAILED`).
Config additions (`internal/config`, `config.yaml`, `Validate()` + tests): `sender.prefetch` (default 50), `relay.confirm_timeout` (default 5s).

### 2. Domain and ports
- `internal/domain`: `Backoff.Delay(retries int, rnd)` helper (pure, unit-tested); outbox/inbox claim value types (`OutboxClaim{DeliveryID, Channel, Retries}`).
- `internal/ports`:
  - `OutboxStore`: `Claim(ctx, limit) ([]OutboxClaim, error)` (also marks over-cap rows `DEAD`), `MarkPublished(ctx, claim)`, `Release(ctx, claim, nextAttemptAt, reason)`, `ReclaimStuck(ctx, olderThan) (int, error)`.
  - `InboxStore`: `Claim(ctx, deliveryID, provider) (won bool, err)`, `ReclaimStuck(ctx, provider, olderThan)`.
  - `Publisher`: `Publish(ctx, channel, ids []int64) (confirmed []bool/err per id)`; `Consumer`: `Consume(ctx, channel, handler func(ctx, Message) error)` with `Message{DeliveryID, Ack(), Nack(requeue)}`.
  - `ProviderChecker`: `Check(ctx, provider, deliveryID) (CheckResult{Accepted, NotFound, Unknown}, error)`.

### 3. Postgres adapters
`internal/adapters/postgres/outbox.go`, `inbox.go` (raw pgx, SQL in consts, `var _ ports.X = ...`):
- Claim: one statement, CTE `SELECT delivery_id FROM outbox WHERE state='NEW' AND (next_attempt_at IS NULL OR next_attempt_at <= now()) ORDER BY next_attempt_at NULLS FIRST LIMIT $1 FOR UPDATE SKIP LOCKED`, then `UPDATE ... SET state='PROCESSING', retries=retries+1, last_try=now()` `RETURNING delivery_id, retries`, joined to `deliveries.channel`. Over-cap rows (`retries >= max_attempts`) updated to `DEAD` + `failure_reason` in the same CTE.
- Fencing: `MarkPublished`/`Release` use `WHERE delivery_id=$1 AND state='PROCESSING' AND retries=$2`; 0 rows -> stale claim, logged, not an error.
- Outbox reclaim: `UPDATE outbox SET state='NEW', next_attempt_at=... WHERE state='PROCESSING' AND last_try < now()-$1`.
- Inbox claim: `INSERT ... (state='PROCESSING', claim_count=1, provider=<channel primary>) ON CONFLICT (delivery_id) DO NOTHING`; if no row inserted, conditional `UPDATE ... SET state='PROCESSING', claim_count=claim_count+1, last_try=now() WHERE state='WAITING_RETRY' AND claim_count=$read` (legit retry); otherwise the message is a duplicate -> loser, ack and drop. `claim_count > max_claims` -> `DEAD`.
- Inbox reclaim: per provider, threshold = provider `timeout` + `inbox_stuck_margin`; stuck rows listed, `ProviderChecker.Check` called, result `Unknown`/`NotFound` -> `WAITING_RETRY` with `next_attempt_at` per backoff (Phase 5 refines per the log's timeout path); `Accepted` -> delivery `ACCEPTED`, inbox `DONE`.

### 4. RabbitMQ adapter
`internal/adapters/rabbitmq/`:
- `Conn`: add reconnect (watch `NotifyClose`, redial with `lifecycle.Retry`-style loop, expose current connection to channel users) so a broker restart does not require a process restart; failure after exhausting retries surfaces via `Failer`.
- `topology.go`: idempotent `QueueDeclare(durable)` for the three channels, called by both publisher and consumer on (re)connect.
- `publisher.go`: confirm-mode channel, `PublishWithDeferredConfirmWithContext` for the batch, wait on confirms within `relay.confirm_timeout`; per-message result so the relay marks only acked ids `PUBLISHED`; nack/timeout/closed channel -> unconfirmed. `DeliveryMode: Persistent`.
- `consumer.go`: manual ack, `Qos(prefetch)`, one goroutine per channel queue, re-establishes on channel/connection loss; handler error -> `Nack(requeue=true)`.
- `memory/` in-memory `Publisher`+`Consumer` pair used only by tests.

### 5. Relay service and janitor
`internal/relay/` (app-level orchestration, ports only):
- `Relay` component: ticker at `poll_interval`; loop: Claim -> group by channel -> Publish -> `MarkPublished` for confirmed, `Release` (backoff, reason `PUBLISH_FAILED`) for the rest. Immediately re-poll if the batch was full. Log `delivery_id`, count, never PII.
- `Janitor` component: ticker at `janitor.interval`; runs outbox sweep then inbox sweep; logs reclaimed counts.
- Wire in `cmd/relay/main.go` (`[]Component{db, rmq, relay, janitor}`; start order puts workers last so reverse stop drains them first). `Stop` waits for the in-flight batch (graceful shutdown).

### 6. Sender intake
`internal/sender/` intake handler + `Component` consuming each channel queue: on message -> `InboxStore.Claim`; **ack only after that write succeeds**; loser -> ack and drop; winner -> hands to a `Handler` hook (no-op logging stub now; Phase 4 plugs in the provider call). Wire in `cmd/sender/main.go`.

### 7. Tests (real Postgres + RabbitMQ)
- New `rmqtest` helper (generic container `rabbitmq:4-alpine`, fixed host port via host config so restart keeps the address).
- Unit: backoff helper, relay loop with fakes/in-memory adapters, janitor with fake checker.
- Integration: two relays never claim the same row; killed relay (context cancelled mid-batch / row left `PROCESSING`) recovered by janitor and fully published; stale relay's `MarkPublished` updates 0 rows (fencing); over-cap row -> `DEAD` with reason; broker stop/start loses nothing and relay/consumer reconnect; redelivered message handled once (inbox claim loser); inbox stuck row recovered after `timeout+margin`.
- Update `schema_test.go` only if a migration is added (none planned).

### 8. Docs
Tick Phase 3 checklist; note in decision log; refresh CLAUDE.md Status; session log in `docs/sessions/`.

## Critical files

Edit: `cmd/relay/main.go`, `cmd/sender/main.go`, `internal/adapters/rabbitmq/conn.go`, `internal/config/config.go`, `config/config.yaml`, `docs/notification_system_design_decision_log.md`, `docs/notification_system_implementation_plan.md`, `CLAUDE.md`.
New: `internal/ports/{outbox,inbox,queue,provider}.go`, `internal/domain/backoff.go`, `internal/adapters/postgres/{outbox,inbox}.go`, `internal/adapters/rabbitmq/{topology,publisher,consumer}.go`, `internal/adapters/rabbitmq/memory/`, `internal/adapters/rabbitmq/rmqtest/`, `internal/relay/`, `internal/sender/`, `_plans/phase-3-relay-queue-janitor.md`.

## Verification

1. `make check` (fmt, vet, lint, `go test -race ./...`; Docker running).
2. `go test -race -run 'TestRelay|TestJanitor|TestBrokerRestart|TestFencing' ./internal/...`.
3. `make up`; POST a batch to the API (curl with `X-API-KEY`); `psql`: outbox rows reach `PUBLISHED`; RabbitMQ management (15672) shows messages on the `email`/`sms`/`whatsapp` queues; sender logs the inbox claims; inbox rows `PROCESSING`.
4. Manual crash drills: `docker kill` the relay mid-load -> rows recover within `outbox_stuck_after`; `docker restart rabbitmq` -> no rows stuck, publishing resumes.
5. Import-boundary check: `internal/relay`, `internal/sender` and `domain` import only ports/domain, not adapters.

## Open points for approval (defaults in bold)

- Decided by the engineer: queues are named by channel (`email`, `sms`, `whatsapp`); the message holds only `delivery_id`; `next_attempt_at` is NULL by default and set only for a scheduled retry.
- Confirmed by the engineer: Phase 3 sender handler logs and leaves the row `PROCESSING` (the inbox sweep exercises recovery); a duplicate message is acked and dropped; `claim_count > max_claims` -> inbox `DEAD` + delivery `FAILED` with a new reason code.
- Message body encoding of `delivery_id`: **decimal text** vs 8-byte binary.
- Inbox winner on first delivery: **insert directly as `PROCESSING`** (claim_count=1); an existing row not in `WAITING_RETRY` means duplicate -> drop.
- `sender.max_claims` exceeded: **inbox `DEAD` + delivery `FAILED`** (needs a reason code).
- Provider check in the inbox sweep: **`ProviderChecker` port with a no-op "Unknown" adapter now**, real implementation in Phase 4/5.
- Broker reconnect lives in **`rabbitmq.Conn`** (shared by publisher and consumer) rather than per-component.
- New config keys: **`sender.prefetch=50`, `relay.confirm_timeout=5s`**.
