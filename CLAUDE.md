# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Status

Phases 1-6 are built: foundations, the accept path, the relay with its queue and janitor, the sender with the simulated provider and the webhook endpoint, resilience, and limits. The sender retries a provider with backoff from a scan of due `WAITING_RETRY` rows, switches to the channel's next provider once a provider's retries are spent, asks the provider whether it holds a message after a timeout, runs a local circuit breaker per provider, and checks the deadline before every send; the janitor makes the same check for rows stuck in `PROCESSING`. The Dispatcher charges each request, in deliveries, to one global token bucket kept in Redis (a local bucket takes over per instance if Redis is unreachable) and answers 429 with `Retry-After` when it is empty; the sender takes a token from a per-provider bucket before every call and, when there is none, defers the row on the same provider without spending a retry or a claim. Phase 7 (verification) is in progress: the tests and benchmarks and the metrics (`/metrics` on api, relay and sender, `deliveries.accepted_at`) are built; the load driver, load runs and fault-injection runs are not. The design lives in `docs/` and is the source of truth:

- `docs/notification_system_design_decision_log.md` — every design decision and its rationale. **Record any change to a decision here first**, before changing code.
- `docs/notification_system_implementation_plan.md` — 8 phases (foundations → accept path → relay/queue/janitor → sender + simulated provider → resilience → limits → verification → deliverables) with checklists and "done when" criteria. Build in that order, as thin vertical slices.

`go.mod` declares module `github.com/mansonxasthur/notifier` (Go 1.26.3). Layout: `cmd/{api,relay,sender,simprovider}` (thin mains on `internal/app`), `internal/{domain,ports,adapters,config,lifecycle,httpserver,logging,relay,sender,simprovider,e2e}`, `config/config.yaml` (defaults; connection strings come from `DATABASE_URL`, `AMQP_URL`, `REDIS_URL`; each provider's webhook secret from the variable its `webhook.secret_env` names), `migrations/`, `infra/`. Provider config marks one `primary` per channel (the rest are fallbacks in list order); `simulation: true` routes provider calls to the simulator. `.githooks/` exists; run `make hooks` once per clone.

## Workflow (mandatory)

- **Plans first.** Every planned feature is documented in `_plans/`. Do not start work on a plan until the engineer has approved it and every point in it is covered.
- **Issue per task.** For each new task, create a GitHub issue on `origin` (`gh issue create`) before starting.
- **Branch per issue.** Work on a separate branch created for that issue; never commit directly to `master`.
- **Before each commit:** run `make check`, and pass the changes through the `code-review` skill. Fix what it reports.
- **Commits** follow the `/commit` skill (Conventional Commits). Never add a `Co-Authored-By` (or any `Co-Authored`) footer to commit messages.
- **After completion,** open a pull request for the branch. The engineer does the final review and merges into `master`; never merge it yourself.

## Infrastructure

The project runs on docker-compose, not Kubernetes. Infra files (Dockerfiles and the like) live in `infra/`. Compose files (`docker-compose.yml`, `docker-compose.override.yml`) stay in the repo root, where the Makefile's `up`/`down`/`logs` targets expect them.

## Commands

- `make check` (default) — `fmt-check`, `vet`, `lint`, `test`; run before committing
- `make test` — `go test -race ./...`; Postgres tests use testcontainers, so Docker must be running
- Single test: `go test -race -run TestName ./internal/path/to/pkg`
- `make lint` — golangci-lint v2 (config `.golangci.yml`)
- `make hooks` — once per clone, sets `core.hooksPath` to `.githooks`
- `make up` / `down` / `logs` — dev stack with live reload (`make logs s=<service>` follows one service, default all)
- `make migrate-create name=<name>` — new migration in `migrations/` (golang-migrate, timestamp format)

## Architecture (planned)

A notification system (email, SMS, WhatsApp) targeting 500K deliveries/sec peak and a 5s p99 request-to-ACCEPTED target. One Go codebase, hexagonal layout (`domain`, `ports`, `adapters`), three entry points under `cmd/`:

- **api** — Dispatcher: validates per recipient, writes deliveries + outbox rows in one Postgres transaction (`ON CONFLICT DO NOTHING ... RETURNING` on unique `(notification_id, recipient_id, channel)`), never touches the broker. Also serves the provider webhook endpoint. Rate limited (Redis counters, local fallback), 429 + `Retry-After`.
- **relay** — claims outbox rows with `FOR UPDATE SKIP LOCKED`, publishes to RabbitMQ with publisher confirms (one durable queue per channel; messages carry only `delivery_id`), and runs the janitor sweeps for rows stuck in `PROCESSING`.
- **sender** — consumes a channel queue, claims the inbox row with a conditional update (decides which worker sends), makes one provider call per attempt. Retry timing is stored in the inbox (`next_attempt_at`, scanned by the sender) because RabbitMQ has no per-message delay. Per-provider retries with backoff+jitter, then failover to the second provider; local circuit breaker; per-provider token bucket; expiry check before every send.

Key invariants: the outbox/inbox pattern is the only hand-off between components; state transitions are guarded (`PENDING`, `ACCEPTED`, `DELIVERED`, `FAILED`, `UNRESOLVED`); `delivery_id` is the provider reference key echoed in webhooks and used for status lookups; a timeout on the second provider ends `UNRESOLVED`, a 5xx ends `FAILED`. Graceful shutdown is mandatory for all entry points. Local stack: Postgres, RabbitMQ, one Redis, and a simulated provider service with configurable failure modes.

## Conventions

- Logging with `slog`, stdout default handler.
- Reason codes and failure values are defined once as an enum in the OpenAPI spec and reused in code.
- Skill `go-architecutre` (modular monolith, hexagonal/clean) describes the intended structure. Ignore the `observability-lgtm` skill for this project.
