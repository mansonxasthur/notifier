# Phase 1: Foundations: implementation plan

## Context

The repo holds only design docs, a Makefile, and empty `cmd/ internal/ infra/ _plans/` directories. Phase 1 of `docs/notification_system_implementation_plan.md` turns that into an empty but wired project: it builds, starts three entry points (api, relay, sender), connects them to their dependencies, and shuts them down cleanly. Later phases only add behaviour on top of this.

Done when: all three entry points start, connect, and exit cleanly on SIGTERM (exit code 0).

Choices already confirmed with the engineer: config is **YAML file + env overrides**, the simulated provider is a **minimal stub service** (`/healthz` only; behaviour arrives in Phase 4), and CI is **GitHub Actions**.

## Workflow (from CLAUDE.md)

1. First action after approval: copy this plan to `_plans/phase-1-foundations.md`.
2. Create one GitHub issue, "Phase 1: Foundations", with the checklist below (`gh issue create`). Branch `feat/<issue#>-foundations`. One commit per step below (Conventional Commits, no Co-Authored footer), one PR at the end. Never merge it.
3. Before each commit: `make check` and the `code-review` skill.
4. **Record decisions in `docs/notification_system_design_decision_log.md` first** (a short "Foundations" section), before code:
   - Technology: pgx/v5 (`pgxpool`), `rabbitmq/amqp091-go`, `redis/go-redis/v9`, `yaml.v3`, golang-migrate (CLI in compose, library in tests), testcontainers-go.
   - Column types: `bigint generated always as identity` ids, `text`, `jsonb`, `timestamptz`. No CHECK constraints (the log says the inbox's only constraint is its key; state values are enforced by `internal/domain`).
   - State names the log leaves open: outbox `NEW | PROCESSING | PUBLISHED | DEAD`; inbox `PROCESSING | WAITING_RETRY | DONE | DEAD`; deliveries `PENDING | ACCEPTED | DELIVERED | FAILED | UNRESOLVED`. Please confirm or rename at approval.
   - Provider config: explicit `primary` flag (one per channel, others are fallbacks in list order), `max_retries: 1` default, global `simulation` flag routing providers to the simulator.
   - Latency verification: simulation and load-test checks gate on **p95** request-to-ACCEPTED; p99 against the 5s target is still recorded and reported (the target text itself is unchanged).
   - Migrations run as a one-shot compose service; JSON log handler by default (text via `LOG_FORMAT=text`).

## Layout

One bounded context, so a flat hexagonal layout (not the skill's multi-module tree):

```
cmd/{api,relay,sender,simprovider}/main.go   thin: signals, config, logger, wiring, run
internal/domain/        Channel, DeliveryState, OutboxState, InboxState, FailureClass (typed string enums, no imports)
internal/ports/         Pinger (health port); Publisher/Consumer/etc. arrive in later phases
internal/adapters/postgres/   pgxpool constructor + Ping
internal/adapters/rabbitmq/   amqp connection + Ping (queue declaration is Phase 3)
internal/adapters/redis/      go-redis client + Ping
internal/config/        Config structs, Load, Validate
internal/logging/       slog setup
internal/lifecycle/     ordered startup/shutdown helper
config/config.yaml      checked-in defaults
migrations/             golang-migrate SQL
infra/                  Dockerfile, dev.Dockerfile
docker-compose.yml, docker-compose.override.yml, .golangci.yml, .githooks/pre-commit, .github/workflows/ci.yml
```

Dependency rule: `domain` imports nothing; `ports` imports `domain`; `adapters` import `ports`; `cmd` is the only place that wires concrete adapters.

## Steps

### 1. Tooling skeleton
- `.golangci.yml` (v2): standard linters plus `errorlint`, `noctx`, `sloglint`, `gosec`; `gofmt` formatter.
- `.githooks/pre-commit` runs `make check`; `.gitignore`, `.dockerignore`.
- `go get` the dependencies above.

### 2. Logging and config
- `internal/logging.New(level, format, w io.Writer, service string) *slog.Logger`: `slog` JSON handler on stdout, `service` attribute on every line; `LOG_LEVEL`, `LOG_FORMAT` env.
- `internal/config` loads `config/config.yaml` (path from `CONFIG_PATH`) with `yaml.v3` `KnownFields(true)`, so a typo fails fast. Env overrides: `DATABASE_URL`, `AMQP_URL`, `REDIS_URL`, `HTTP_ADDR`. Connection strings are never in the YAML.
- Shape (durations as Go duration strings):
  ```yaml
  shutdown_timeout: 20s
  http: {addr: ":8080"}
  batch_limit: 10000
  notification_types:            # name -> default expiry
    password_reset: {expiry: 15m}
    match_alert:    {expiry: 2h}
    marketing:      {expiry: 24h}
  simulation: true               # true: every provider's calls go to the simprovider service; false: real provider URLs
  simulation_url: http://simprovider:8081
  channels:
    email:
      providers:                 # exactly one primary; the others are fallbacks, tried in list order
        - {name: email-a, primary: true, url: https://..., timeout: 1s, status_lookup: true, max_retries: 1,
           backoff: {initial: 200ms, max: 5s, multiplier: 2.0, jitter: 0.2}}
        - {name: email-b, primary: false, url: https://..., timeout: 1s, status_lookup: false, max_retries: 1, backoff: {...}}
    sms: ...
    whatsapp: ...
  relay:
    poll_interval: 100ms
    batch_size: 500
    max_attempts: 10
    backoff: {initial: 1s, max: 1m, multiplier: 2.0, jitter: 0.2}
    janitor: {interval: 5s, outbox_stuck_after: 30s, inbox_stuck_margin: 2s}  # inbox threshold = provider timeout + margin
  sender:
    poll_interval: 200ms          # due-retry scan
    max_claims: 20
  ```
- Provider rules: exactly one `primary: true` per channel (the rest are fallbacks), and `max_retries` is 1 for every provider in the shipped config (the field stays configurable). `simulation: true` requires `simulation_url`; `simulation: false` requires every provider `url`. In Phase 1 the flag is only loaded, validated and logged at startup; Phase 4 acts on it. The decision log's retry text ("configured maximum") is unchanged; the value 1 is recorded as the default.
- `Validate()` rejects: empty channel/provider lists, zero or several primaries in a channel, duplicate provider names, negative `max_retries`, non-positive durations/limits, provider timeout >= 5s (the request-to-ACCEPTED target), `batch_limit` != positive, jitter outside [0,1], empty type name. Table-driven unit tests for `Load` (good file, unknown field, each invalid case, env override precedence).
- Helper `Config.StuckThreshold(provider)` is not added yet; Phase 3 consumes the data.

### 3. Lifecycle
- `internal/lifecycle`: `Run(ctx, log, shutdownTimeout, components...)` where a component has `Start(ctx) error` and `Stop(ctx) error`. Starts in order (stops already-started ones if one fails), waits for ctx cancellation (`signal.NotifyContext` SIGINT/SIGTERM in `main`), stops in reverse order under the shutdown timeout, returns the joined error. Unit tests with fakes: order, partial startup failure, stop timeout, error joining.
- Closes the mandatory graceful-shutdown requirement for the scaffolding; real work loops added in later phases plug in as components.

### 4. Adapters and entry points
- Adapters expose `New(ctx, url) (*X, error)`, `Ping(ctx) error`, `Close()`; connect with bounded retry (dependencies may still be booting in compose).
- `api`: Postgres + Redis, HTTP server (`net/http`, read/write/idle timeouts) with `/healthz` (process up) and `/readyz` (pings dependencies); `Shutdown(ctx)` on stop.
- `relay`: Postgres + RabbitMQ; idle loop placeholder that logs "relay started" and waits for ctx.
- `sender`: Postgres + RabbitMQ; same shape. (Redis joins the sender when the token bucket lands in Phase 6.)
- `simprovider`: HTTP `/healthz` only.
- Every entry point logs start/connected/stopping/stopped; exit 0 on clean signal, 1 on startup failure.

### 5. Migrations
Four files made with `make migrate-create name=...` (timestamp format), each with a working `down`:
1. `create_notifications`: `id`, `idempotency_key text UNIQUE`, `type`, `content jsonb`, `created_at`.
2. `create_deliveries`: `id` (the `delivery_id`), `notification_id` FK, `recipient_id`, `channel`, `contact jsonb`, `expires_at`, `state default 'PENDING'`, `created_at`, `updated_at`; `UNIQUE (notification_id, recipient_id, channel)`.
3. `create_outbox`: `delivery_id` PK+FK, `state default 'NEW'`, `retries default 0`, `last_try default now()`, `next_attempt_at NULL`, `failure_reason NULL`. Partial indexes: `(next_attempt_at NULLS FIRST) WHERE state='NEW'` (relay; NULL = due now) and `(last_try) WHERE state='PROCESSING'` (janitor).
4. `create_inbox`: `delivery_id` UNIQUE+FK, `state`, `provider`, `retries`, `claim_count`, `last_try`, `next_attempt_at NULL`, `failure_class NULL`, `failure_reason NULL`. Partial indexes: `(next_attempt_at) WHERE state='WAITING_RETRY'` (sender scan) and `(last_try) WHERE state='PROCESSING'` (janitor).
Everything else NOT NULL, as the log requires.

Schema integration test (`internal/adapters/postgres`, testcontainers, skipped under `-short`): apply all migrations up, down, up again; assert nullability against `information_schema` (only the six listed columns are nullable), the unique constraint and partial indexes exist in `pg_indexes`, and `INSERT ... ON CONFLICT DO NOTHING RETURNING` returns no row on a duplicate `(notification_id, recipient_id, channel)`. Also an `EXPLAIN` check that the relay's due-row query can use the outbox partial index (with `enable_seqscan=off` on a tiny table) so the NULL-handling choice is proven, not assumed.

### 6. Docker and compose
- `infra/Dockerfile`: multi-stage, `ARG SERVICE`, static build of `./cmd/${SERVICE}`, distroless/alpine runtime, non-root. `infra/dev.Dockerfile`: golang image with `air`.
- `docker-compose.yml`: `postgres`, `rabbitmq` (management), `redis`, one-shot `migrate` (migrate/migrate image mounting `./migrations`, runs after Postgres is healthy), `api`, `relay`, `sender`, `simprovider`. Healthchecks on infra; app services `depends_on: condition: service_healthy` (and `service_completed_successfully` for `migrate`). Config file mounted read-only; DSNs via env. Images pinned to explicit versions.
- `docker-compose.override.yml` (picked up by `make up`): dev image, source bind-mount, `air` rebuilding `./cmd/$SERVICE` for live reload, published ports (8080 api, 8081 simprovider, 5432, 5672/15672, 6379).

### 7. CI
`.github/workflows/ci.yml` on push and PR: `actions/setup-go` (version from `go.mod`), `golangci-lint-action` (pinned v2), then `make fmt-check vet test`. Runners provide Docker, so testcontainers tests run. Action versions pinned at implementation time.

### 8. Docs
Update CLAUDE.md's Status section (Phase 1 done, layout, config file, how to run). Tick the Phase 1 boxes in the implementation plan only when verified.

## Critical files
New: everything listed in Layout. Existing and touched: `Makefile` (only if a target needs adjusting, e.g. a `migrate-up` helper is not added unless wanted), `go.mod`/`go.sum`, `CLAUDE.md`, `docs/notification_system_design_decision_log.md`.
Reuse: Makefile targets (`check`, `up`, `migrate-create`); nothing else exists to reuse.

## Verification
1. `make check` passes locally (Docker running): fmt, vet, lint, `go test -race ./...` including config, lifecycle, and schema tests.
2. `make up`, then `docker compose ps`: all services healthy, `migrate` exited 0; `curl localhost:8080/readyz` returns 200; `curl localhost:8081/healthz` returns 200.
3. Inspect schema: `docker compose exec postgres psql -U ... -c '\d+ outbox'` (and the other three) match the migrations; indexes present.
4. Clean exit: `docker compose stop api relay sender simprovider`, then `docker compose ps -a --format '{{.Service}} {{.ExitCode}}'` shows 0 for all, and logs show the "stopping/stopped" lines in reverse-dependency order.
5. Startup failure: stop Postgres, start `api`; it retries within the bound, then exits 1 with a clear log line.
6. `migrate ... down` then `up` round-trips on a scratch database (covered by the schema test).
7. Push the branch; CI is green on the PR.

## Open points for approval
- State names in the decision-log step above.
- One issue/branch/PR for the whole phase (assumed); say if you want one per step instead.
