# Phase 6: Limits and backpressure: implementation plan

## Context

Phases 1-5 are built. Nothing limits intake or provider load yet. The design is settled in the decision log (lines 233-242, 193, 195, 216): one global deliveries-per-second limit in the Dispatcher, shared through Redis with a local in-memory fallback, 429 + `Retry-After`, and a per-provider token bucket in the sender in front of every provider call. Redis is already wired (compose, `REDIS_URL`, `internal/adapters/redis/client.go` exposing `Client()` "for the rate limiter", go-redis v9 in go.mod) but unused apart from health.

Goal: overload is refused at the door, and accepted bursts drain at a pace providers can take.
Done when: a burst above the limit gets 429 + `Retry-After`, and a spike inside the limit drains without exceeding any provider's rate.

Decision: **token bucket for both limiters** (user choice). One bucket algorithm serves the Redis global limit, its local fallback, and the sender's per-provider limit.

Not in scope: per-service quotas, shared breaker state, priority between types, load-test numbers (Phase 7).

## Steps

### 0. Housekeeping (CLAUDE.md workflow)
- Copy this plan to `_plans/phase-6-limits.md` (same section layout as `phase-5-resilience.md`) and wait for approval.
- `gh issue create` on origin, then branch `feat/<n>-limits` off master.

### 1. Decision Log first
Edit `docs/notification_system_design_decision_log.md`:
- Global limiter is a token bucket (rate = deliveries/s, burst = capacity), charged per request by delivery count, all-or-nothing (no partial accept).
- Charged after validation, before any DB write; rejected requests consume nothing.
- Add the 429 row to the API status table (line 79) and a `RATE_LIMITED` code.
- A throttled provider send is deferred on the same provider without spending a retry or a claim.
- Record the rate-limit-semantics reason the plan's risk table asks for.

### 2. Domain
- `internal/domain/bucket.go`: pure token bucket `Bucket{rate, burst, tokens, last}` with `Take(n, now) (ok bool, wait time.Duration)`. Used by the local fallback and the sender; unit-tested with a fake `now`.
- `domain.BucketPolicy{Rate float64, Burst int}` with validation.

### 3. Config (`internal/config/config.go`, `config/config.yaml`)
- `Config.Limits.Global{Rate, Burst}` in deliveries/s; default sized from the 500K/s peak, with a comment about the single-machine caveat. `0` disabled is not allowed; tests set a high value.
- `Provider.RateLimit{Rate, Burst}` on all six provider entries.
- Declare and validate in `Validate` (`KnownFields(true)` rejects undeclared keys). Add `Policy()` converters like `Breaker.Policy()`.

### 4. Ports and adapters
- `internal/ports/limiter.go`: `Limiter{ Allow(ctx, n int) (Decision, error) }` where `Decision{Allowed bool, RetryAfter time.Duration}`.
- `internal/adapters/redis/limiter.go`: Lua script (atomic refill + take, state in a hash `rl:global` with `tokens` and `ts`, using Redis `TIME`, with a TTL). Uses `Client().Eval`/`Script`.
- `internal/adapters/ratelimit/local.go`: in-memory limiter wrapping `domain.Bucket` behind a mutex.
- `internal/adapters/ratelimit/fallback.go`: tries Redis; on error logs once per outage (not per request) and uses the local bucket, so limiting becomes approximate, not absent. Returns to Redis when it recovers.

### 5. API enforcement
- `internal/httpserver/notifications.go`: add `Limiter ports.Limiter` to `NotificationsOptions`; in `create`, after `Validate` and in-payload de-duplication gives the delivery count and before the store write, call `Allow(ctx, count)`.
- Denied: `Retry-After` (whole seconds, rounded up, min 1) and `writeError(w, 429, "RATE_LIMITED")`. Add the constant next to the others (L16-26).
- A request larger than `Burst` can never succeed; validate `batch_limit x channels <= burst` at config load so it never gets a permanent 429.
- Wire in `cmd/api/main.go`: build the fallback limiter from the existing `cache` client; no new compose changes.

### 6. Sender per-provider bucket
- `internal/sender/limiter.go`: `ProviderLimits` keyed by provider name (shape of `Breakers`: map + mutex + injected `now`), `Take(provider) (ok, wait)`.
- `internal/sender/attempt.go`, in `Send`: take the token **before** `breakers.Allow` so a denial cannot leak a half-open probe slot. Denied: defer, do not call the provider.
- `internal/ports/inbox.go` + `internal/adapters/postgres` inbox store: new fenced `Defer(ctx, id, claimCount, next)`: `WAITING_RETRY`, `next_attempt_at = now + wait` (plus a little jitter), **no** `retries` or `claim_count` increment, so sustained throttling cannot trip `sender.max_claims`. The existing `Scan` picks it up.
- New log outcome `outcomeThrottled`. Expiry is already checked first on the next attempt, so a deadline passing while throttled ends `EXPIRED`.
- Update `NewAttempt` callers: `cmd/sender/main.go`, `internal/e2e/e2e_test.go`, `attempt_test.go` fixture.

### 7. Docs
- Phase 6 checklist ticks in the implementation plan, `CLAUDE.md` Status (Phases 1-6; drop "no Redis use"), `docs/sessions/<date>_phase-6-*.md`.

## Critical files
`internal/domain/bucket.go` (new), `internal/config/config.go`, `config/config.yaml`, `internal/ports/limiter.go` (new), `internal/ports/inbox.go`, `internal/adapters/redis/limiter.go` (new), `internal/adapters/ratelimit/` (new), `internal/adapters/postgres` inbox store, `internal/httpserver/notifications.go`, `cmd/api/main.go`, `cmd/sender/main.go`, `internal/sender/attempt.go`, `internal/sender/limiter.go` (new).

## Verification
- Domain: bucket table tests with a fake clock (refill, burst cap, wait calculation, all-or-nothing).
- Redis limiter: against a real Redis (new `redistest` helper mirroring `rmqtest`, testcontainers): two limiter instances share one budget; wait/`Retry-After` is sane.
- Fallback: point at a closed port / stop the container, assert requests are still limited locally and that it recovers.
- HTTP: burst above the limit returns 429 with `Retry-After`, nothing written to the DB; request within the limit accepted; rejected request does not consume tokens.
- Sender: with a low provider rate, a spike inside the global limit drains with calls never exceeding the provider rate (measured against a fake clock/recording provider); retries and claim_count unchanged by deferral; expiry while throttled ends `EXPIRED`; the half-open probe is not leaked.
- E2E: existing tests keep passing with a high limit; add one 429 burst case and one throttled-drain case.
- `make check` (Docker running), then the `code-review` skill, before each commit; PR at the end, not merged by me.

## Assumptions to confirm
1. Counting happens in the handler after validation (exact delivery count), not in middleware.
2. A throttled send is deferred via the new `Defer` rather than blocking a worker.
3. Single global bucket key in Redis; the rate is split by no one, so one producer can use all of it (as the log accepts).
4. Redis outages fall back per instance, so the effective limit becomes up to instances x rate until Redis recovers.
