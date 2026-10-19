# Plan #21: Sender batching: implementation plan

## Context

Issue #21. The Phase 7 load runs (`docs/results/README.md`) stop scaling at about 525-600 deliveries/s: Postgres CPU plateaus while the sender uses ~28% of a core. The sender does its database work one delivery at a time: `InboxStore.Claim` (a transaction: insert, maybe lock+update), `DeliveryStore.Load`, one provider call, then `InboxStore.Complete` (one statement, one commit). That is about 3 commits and 4 round trips per delivery in the sender alone; the wait samples showed `WALWrite`/`WalSync`. The cause was never isolated, so this work is the experiment as well as the fix.

Goal: commits per delivery fall by roughly the batch size, with the per-row semantics (fencing by `claim_count`, guarded transitions, retry path) unchanged.
Done when: the sender claims, loads and settles in batches, tests prove per-row semantics and partial-failure isolation, and the benchmark shows fewer commits per delivery. Load runs (Part C) are left for later.

Out of scope (per the issue): changing what a queue message carries (companion #22, independent), merging inbox into `deliveries`, sharding, the webhook path, the janitor's `Requeue`/`Accept` (low volume).

## Approach: coalesce at the store boundary

`ports.Consumer`/`Handler` stay per-message and `Attempt.Send` stays per-delivery. The consumer already runs up to `prefetch` handlers as concurrent goroutines, so sends are already concurrent and bounded. A small generic **coalescer** sits between those goroutines and the store: each caller submits one item and blocks until the batch containing it has been written, then gets its own result. A batch flushes when it holds `batch_size` items or `linger` has passed since its first item.

Why this over restructuring around explicit batches: per-delivery logic (`plan`, breaker, token, `settle*` classification, logging) is untouched, a failing delivery fails only its own goroutine (partial failure is free), and the scan path (`Scan.Once` already claims a batch and sends concurrently) gets the batched settle with no further change. Cost: up to `linger` added latency per stage (default 5 ms, far under the 5 s target).

## Steps

### 0. Housekeeping (CLAUDE.md workflow)
- Copy this plan to `_plans/sender-batching.md`; wait for approval.
- Issue is #21 (exists). Branch `feat/21-sender-batching`. Base: `master` if `feat/19-load-test` is merged by then, else branch off `feat/19-load-test` (the results README the issue refers to lives there).

### 1. Decision log first
Edit `docs/notification_system_design_decision_log.md`: the sender claims, loads and settles through batch store calls with a size-or-linger coalescer; each row keeps its own fencing token and result; one failing row does not affect others; retry still goes through `WAITING_RETRY` and the scan; new config `sender.batch_size` / `sender.linger`. Record the trade-off (up to `linger` added latency per stage, and a row's ack waits for its batch).

### 2. Coalescer (`internal/sender/coalesce.go`, new)
- `Coalescer[In, Out]` with `Do(ctx, in) (Out, error)` and a flush func `func(ctx, []In) ([]Out, error)`. Results align with inputs; a flush error is returned to every caller in that batch; a canceled caller returns `ctx.Err()` without breaking the batch.
- Flush runs on a context that survives caller cancellation (like `settleContext`), with `settleTimeout`.
- Stops cleanly: `Close` flushes what is pending (graceful shutdown mandatory), wired into `Sender.Stop`.
- Unit tests with a fake clock: size flush, linger flush, error fan-out, cancel, close-flush; `-race`.

### 3. Batch store methods
Ports (`internal/ports/inbox.go`, `deliveries.go`), Postgres adapters in `internal/adapters/postgres`:
- `InboxStore.ClaimMany(ctx, ids []int64, provider string, limits) ([]domain.InboxClaim, error)`, results aligned with `ids`; one transaction, ids sorted to avoid lock-order deadlocks: (a) `INSERT ... SELECT unnest($1) JOIN deliveries ON CONFLICT DO NOTHING RETURNING delivery_id` gives the won set; (b) `SELECT ... WHERE delivery_id = ANY(rest) FOR UPDATE` for the rest; decide in Go with the existing rules (`WAITING_RETRY` and due -> reclaim, `limits.Used` >= `MaxClaims` -> dead, else duplicate); (c) one `UPDATE ... = ANY` to reclaim, one pair of statements to kill capped rows and fail their deliveries. Add `domain.InboxClaimMissing` for an id with no delivery (today `Claim` errors there and the message is requeued; the handler keeps that).
- `DeliveryStore.LoadMany(ctx, ids) (map[int64]domain.DeliveryDetail, error)`: `loadDelivery` with `d.id = ANY($1)`; missing ids are absent and map to `ErrDeliveryNotFound` per caller.
- `InboxStore.Settle(ctx, []domain.Outcome) ([]bool, error)`: `domain.Outcome{Kind (Complete|Fail|Park|Switch|Defer|Unresolve), DeliveryID, ClaimCount, Class, Reason, Provider, Next}`. One transaction; one `pgx.Batch` (one round trip, one commit) with one statement per kind present, each the existing statement turned set-based (`FROM unnest(...)`), still fenced by `state = PROCESSING AND claim_count = $n` and, for the delivery-moving kinds (Complete/Fail/Unresolve), still guarded by `deliveries.state = PENDING`. Each statement `RETURNING delivery_id`; applied[i] is true iff its row came back, so a stale claim for one row affects only that row.
- Single-row `Claim`, `Load`, `Complete`, `Fail`, `Park`, `Switch`, `Defer`, `Unresolve` stay (janitor, `Accept`, tests, benchmark baseline); `Complete` etc. become thin wrappers over `Settle` only if that removes duplicated SQL without changing behavior, otherwise left alone.

### 4. Wire into the sender
- `Intake.Handler`: claim through a per-channel `Coalescer` over `ClaimMany` (provider = channel primary, as today). Result handling and ack semantics unchanged: ack only after the claim is written.
- `Attempt`: `Send` loads through a `Coalescer` over `LoadMany` and settles through a `Coalescer` over `Settle`; each `a.inbox.Complete/Fail/Park/Switch/Defer/Unresolve` call site (`attempt.go:109, 289, 297, 314, 339, 347, 357, 367`) becomes a submit of the matching `Outcome`. The "skipped" path (delivery already left PENDING) is a `Complete` outcome. `logAttempt(applied)` and the accept-latency recording keep using the per-row result.
- Constructors take the coalescer settings; `NewAttempt`/`NewIntake` callers updated: `cmd/sender/main.go`, `internal/e2e/e2e_test.go`, `attempt_test.go` and other sender test fixtures.
- Concurrency bound: `sender.prefetch` (raise the shipped default from 50 so a batch can fill; the batch can never exceed the number of in-flight messages) and `scan_batch`. The breaker and per-provider token bucket are used per call as now.
- Config (`internal/config/config.go`, `config/config.yaml`): `sender.batch_size` (default 50) and `sender.linger` (default 5ms), validated `> 0`; `prefetch` default raised to cover `batch_size` times a few batches. Comments in the yaml.

### 5. Tests
- Postgres integration (testcontainers, `internal/adapters/postgres/*_test.go`): `ClaimMany` matches `Claim` row for row over mixed batches (new, duplicate, due retry, not-due retry, capped, missing); `Settle` with a mixed batch where one row's claim is stale: only that row reports false and the others apply; delivery guard keeps a webhook-finished delivery's state; ordering/concurrency test with two senders claiming overlapping ids (no deadlock, no double win).
- Sender unit tests: existing attempt/resilience/limits tests still pass through the coalescers (use size 1 or a fake store); add a partial-failure test (one provider error in a batch does not requeue or fail the rest) and a shutdown test (pending outcomes flushed on `Stop`).
- E2E (`internal/e2e`) stays green unchanged in assertions.
- Benchmark in `internal/adapters/postgres/bench_test.go`: `ClaimMany`+`Settle` vs the single-row path, per delivery.

### 6. Docs and PR
- Tick/update `CLAUDE.md` Status and `docs/sessions/<date>_sender-batching.md`; open a pull request referencing #21 (the engineer merges).
- Out of scope here: load runs and `docs/results/` (Part C), including the commits-per-delivery measurement. The batch size is justified by the integration tests and the Go benchmark, which compares commits/round trips per delivery for the single-row and batch paths.

## Critical files
`internal/sender/{coalesce.go (new), intake.go, attempt.go, sender.go}`, `internal/ports/{inbox,deliveries}.go`, `internal/domain` (`Outcome`, `InboxClaimMissing`), `internal/adapters/postgres/{inbox,deliveries}.go` and their tests/bench, `internal/config/config.go`, `config/config.yaml`, `cmd/sender/main.go`, `internal/e2e/e2e_test.go`, decision log.

## Verification
- `make check` green (race tests, lint) before each commit; code-review skill pass per CLAUDE.md.
- `go test -race ./internal/sender ./internal/adapters/postgres`, then `./internal/e2e`.
- Existing resilience/limits tests pass unchanged in assertions, showing retries, switches and deferrals still land in `WAITING_RETRY` and are picked up by the scan.
- Benchmark: batch path vs single-row path per delivery.

## Assumptions to confirm
1. Coalescing at the store boundary (above) is acceptable instead of the issue's literal "claim a batch, send the batch" structure; it delivers the same commit reduction with a smaller change.
2. Defaults `batch_size` 50, `linger` 5 ms, and a raised `prefetch`, tuned by the after-run.
3. No load runs in this issue; they follow separately.
