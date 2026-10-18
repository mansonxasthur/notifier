# Phase 5: Resilience: implementation plan

## Context

Phase 4 makes one call to the primary provider per claim. A provider failure or timeout is parked in `WAITING_RETRY` with `retries+1` and `next_attempt_at`, and nothing picks it up. `max_retries`, `sender.poll_interval` and `ProviderChecker` exist but the sender does not use them. `Claim` ignores `inbox.provider` on reclaim, so every attempt goes to the primary.

Goal (plan, Phase 5): the system keeps its promises when providers misbehave. **Done when** each simulated failure mode ends in the state the Decision Log describes, with a test per mode.

Scope: error classification, retry scan, per-provider retries then failover, timeout lookup loop, local breaker, `NO_PROVIDER_AVAILABLE`, second-provider closure (`UNRESOLVED`/`FAILED`), expiry, janitor alignment.
Not in scope: the per-provider token bucket and 429 `Retry-After` (Phase 6, per the plan and the Decision Log), the reconciler sweep and Redis-shared breaker (enhancements, separate plan if time allows), preference-service events (documented only).

## Steps

### 1. Decision Log first
Edit `docs/notification_system_design_decision_log.md` before code:
- Add `EXPIRED` and `NO_PROVIDER_AVAILABLE` to the `failure_reason` enum (and the OpenAPI enum, which is the single source).
- Breaker is local-only in this build: drop the "falls back to local if Redis is unreachable" sentence; drop the shared-state cache wording (a local state machine needs no cache).
- Record: a `NotFound` lookup on the second provider with retries exhausted ends `FAILED` (`PROVIDER_FAILED`), since "no" is a usable answer; only an unusable answer (lookup error, 5xx, unsupported) ends `UNRESOLVED`.
- Record: a queue redelivery of a `WAITING_RETRY` row whose `next_attempt_at` is in the future is dropped as a duplicate; the scan owns retries.
- Record: the token bucket stays in Phase 6.

### 2. Domain: reasons and breaker
- `internal/domain/state.go`: add the two reasons (+ `Valid()`), regenerate from the spec where applicable.
- New `internal/sender/breaker.go`: per-provider breaker (closed → open → half-open), injectable clock, atomic single probe slot in half-open, `Allow(provider)`, `Success`, `Failure`. Reports a transition to open/closed so the sender can log. Table-tested.
- Config (`internal/config/config.go`, `config/config.yaml`): `sender.breaker {failure_threshold, open_for}` plus `sender.scan_batch`; declare and validate in `Validate` (unknown keys are errors).

### 3. Inbox store (`internal/ports/inbox.go`, `internal/adapters/postgres/inbox.go`)
All writes stay fenced by `claim_count`.
- `InboxClaim`/`Claimed` carry the row's current `Provider` and `Retries`; `Claim` reads `inbox.provider` on reclaim (the argument is only used on insert) and reclaims `WAITING_RETRY` only when `next_attempt_at <= now()`.
- `ClaimDue(ctx, limit, maxClaims)`: `FOR UPDATE SKIP LOCKED` over `inbox_due_idx`, moves rows to `PROCESSING`, `claim_count+1`, same max-claims dead-lettering as `Claim`.
- `Switch(ctx, id, claimCount, provider, next)`: `WAITING_RETRY`, new provider, `retries = 0`.
- `Unresolve(ctx, id, claimCount, class)`: inbox `DEAD`, delivery `PENDING → UNRESOLVED` (guarded via `domain.CanMoveTo`).
- Reuse `Park`, `Complete`, `Fail` (reason param), `Accept`. No migration needed.
- Tests in `internal/adapters/postgres/inbox_test.go` (pgtest): due-only claim, concurrent `ClaimDue` with no double claim, fencing, max-claims.

### 4. Sender decision flow (`internal/sender/attempt.go`)
Refactor `Attempt.Send` around an ordered provider chain from `Registry.Providers(ch)` (primary first) and `c.Provider`:
1. Load delivery; not `PENDING` → close, skip (as today).
2. Expiry: `now >= ExpiresAt` → `Fail(EXPIRED)`.
3. Breaker: open for current provider → if a later provider exists, `Switch` with no retry spent; if on the last (second) provider → `Fail(NO_PROVIDER_AVAILABLE)` with one critical log per outage (flag set on all-open, reset when any breaker closes).
4. Call the provider (provider timeout already applied by the adapter). Then by class:
   - success: breaker success, `Complete`.
   - message-level: `Fail(PROVIDER_REJECTED)`, breaker untouched.
   - provider-side: breaker failure; `retries < max_retries` → `Park` with backoff+jitter; else first provider → `Switch` (retries reset, due now); second provider → `Fail(PROVIDER_FAILED)`.
   - timeout: breaker failure; `Check(provider, id)` using the provider timeout: `Accepted` → `Accept`; `NotFound` → same budget logic as provider-side; `Unknown`/error/timeout → first provider `Switch`, second provider `Unresolve`.
5. One log line per attempt (delivery_id, provider, class, outcome; never the contact), as in `logAttempt`.
Inject the checker, breaker and clock into `NewAttempt`. Backoff exponent stays "retries already spent".

### 5. Retry scan
- `internal/sender/sender.go`: add a scan goroutine alongside the per-channel consumers, ticking at `cfg.Sender.PollInterval` (currently unused), calling `ClaimDue(scan_batch)` and `Attempt.Send` per row with bounded concurrency; stops on `Stop` (graceful shutdown; outcomes written via `settleContext`).
- `internal/sender/intake.go`: use the claim's provider rather than the fixed primary.

### 6. Janitor alignment (`internal/relay/janitor.go`)
Stuck `PROCESSING` rows use the same check: `Accepted` → `Accept`; `NotFound` → `Requeue` on the same provider; `Unknown`/error → `Switch` to the next provider (or `Unresolve` if already on the last). Use the provider's own backoff instead of the relay-wide one. Update `TestJanitorRecoversStuckInbox`.

### 7. Docs
Update CLAUDE.md Status, the plan checklist boxes, and add `docs/sessions/2026-10-..._phase-5-resilience.md`. Append "Deviations made while building" to the plan file.

## Critical files
`internal/sender/{attempt,intake,sender,breaker}.go`, `internal/adapters/postgres/inbox.go`, `internal/ports/inbox.go`, `internal/domain/state.go`, `internal/relay/janitor.go`, `internal/config/config.go`, `config/config.yaml`, `internal/adapters/provider/registry.go` (reuse `Providers`, `Check`), `internal/e2e/e2e_test.go`, the OpenAPI spec, the decision log.

## Verification
- **Unit, one test per mode** (extend the `fixture` in `internal/sender/attempt_test.go` with a fake provider per position, fake checker, fixed clock and jitter): message error → `FAILED`; 5xx retries then switches then `FAILED`; timeout + lookup yes → `ACCEPTED`; lookup no → retry; lookup error/unsupported → switch; second-provider timeout → `UNRESOLVED`; second-provider 5xx → `FAILED`; open breaker → switch without spending retries; all open → `NO_PROVIDER_AVAILABLE` with a single critical log; expired → `EXPIRED`; stale claim; shutdown mid-attempt.
- **Store**: pgtest tests from step 3. **Breaker**: state-machine table test.
- **E2E** (`internal/e2e`): `fail_transient` and `timeout` no longer end parked; assert final states against the simulator registry. Failover needs per-provider scenarios: use the simulator's per-provider draw if a seed gives both outcomes, otherwise add a small per-provider pin to the simulator config.
- `make check` (Docker running), then the `code-review` skill, before each commit; PR at the end, not merged by me.

## Assumptions to confirm
1. Token bucket stays in Phase 6 (the Phase 5 checklist omits it).
2. Reconciler and Redis breaker excluded.
3. `NotFound` on the second provider ends `FAILED`, not `UNRESOLVED` (Decision Log addition in step 1).

## Deviations made while building

- **No OpenAPI spec exists yet**, so the two new reasons went into `internal/domain/state.go` only, and the Decision Log says so. The plan had them in both.
- **`NewAttempt` takes a `Routing`** (provider order per channel and a `domain.RetryPolicy` per provider) instead of a backoff map. `Config.ProviderBackoffs()` became `ProviderRetryPolicies()`, and `ProviderOrder()` is new. The order comes from the config rather than from `Registry.Providers(ch)`, because the sender needs names, not provider objects.
- **`Switch` takes the failure class** (the plan's signature had none), so the next attempt still knows how the last one failed.
- **`Breakers.Release`** was added to give back a half-open probe slot when a call tells us nothing about the provider (a message-level rejection or a cancelled caller). Without it the breaker could stay half-open forever.
- **The breaker has no cache.** The plan's checklist line mentioned one; with state in the sender's own memory it has nothing to save. The implementation plan's line was reworded.
- **Expiry stores no failure class.** An expired delivery was never attempted, so `Fail` is called with an empty class.
- **`NO_PROVIDER_AVAILABLE`** is raised when the breaker is open on the last provider. When the primary's breaker is open the row switches to the fallback first, and the next attempt fails if that breaker is open too; the plan allowed either.
- **The end-to-end test turns the breaker off** (a very high failure threshold). With it on, a run of failures would change which provider a delivery uses and the oracle could not predict the outcome. The breaker has its own unit tests. The oracle now covers both providers, and the test also fails if any scenario is never drawn at a fallback.
- **Intake sends to the claimed row's provider** (`InboxClaim.Provider`), a change to step 5 that step 4 needed.
- **The janitor's `Provider` carries `Next` and `Backoff`**, built in `cmd/relay` from the channel's provider order.
- **No migration was needed**; `inbox_due_idx` already served the scan.

Not built, as planned: the per-provider token bucket and the 429 `Retry-After` (Phase 6), the reconciler sweep, and the Redis-shared breaker.

Review findings left: a `ClaimDue` batch aborts if one row's dead-letter write fails (the transaction would be poisoned by the failed statement anyway), and `reclaim` does not check that its update matched a row (the row is locked and its state was just read).

