# Session log: Phase 5 resilience

- **Date:** 2026-10-18 to 2026-10-19
- **Tool:** Claude Code
- **Outcome:** Phase 5 built, issue #9, branch `feat/9-resilience`. The pull request is opened by the engineer's go-ahead and is not merged (the engineer reviews and merges).

This is a summary of the session, not a transcript. The raw log is the session's `.jsonl` file under `~/.claude/projects/-home-manson-Projects-notifier/`, named `c3e9e01d-7737-4372-a0be-c8de5a4383fc.jsonl`.

## Planning

1. Asked to plan Phase 5 of `docs/notification_system_implementation_plan.md`. An exploration found that `max_retries`, `sender.poll_interval` and the `ProviderChecker` existed but the sender used none of them, that `Claim` ignored the row's provider on reclaim so every attempt went to the primary, and that the two new reasons were not in the enum.
2. Points settled in the plan, none needing the engineer's input: the per-provider token bucket stays in Phase 6; the reconciler and the Redis-shared breaker stay out; a "not found" lookup on the last provider with its retries spent ends `FAILED`, since "no" is a usable answer.
3. The plan was approved and saved to `_plans/phase-5-resilience.md`.

## Implementation

Workflow followed: issue #9, branch `feat/9-resilience`, decision log written before code, `make check` before each commit, one `code-review` pass per step, one commit per step. The engineer approved each step before the next began.

1. **Decision log and plan.** The two reasons, the local-only breaker, the not-found rule, the scan owning retries.
2. **Reasons, breaker and config.** `EXPIRED` and `NO_PROVIDER_AVAILABLE`; a per-provider breaker with one half-open probe and an injectable clock; `sender.scan_batch` and `sender.breaker`.
3. **Inbox store.** `Claim` reads the row's provider and leaves a retry that is not yet due; `ClaimDue` uses `SKIP LOCKED`; `Switch` and `Unresolve` are fenced by `claim_count`.
4. **Sender decision flow.** Expiry, breaker, retry with backoff, switch to the next provider, the timeout lookup, and the `UNRESOLVED` end on the last provider.
5. **Retry scan.** A goroutine in the sender that claims due rows and sends them through the same attempt as a queue message; the end-to-end oracle now follows failover.
6. **Janitor.** A stuck row gets the same lookup: accepted, requeued on the same provider, switched, or unresolved.
7. **Docs.** This log, the status line, the plan's checklist and its deviations.

Departures from the written plan are listed at the end of `_plans/phase-5-resilience.md`.

Review findings fixed: a duplicate test name (`TestFailureReasonValid`) was extended instead of added, and two lint findings in the tests were fixed. One review finding was a misreading and was answered with a doc comment: `ProviderOrder` does not look at the `primary` flag because config validation already requires the primary to be first. Two minor findings were left, with reasons in the plan.

## Commits

One commit per step, each after `make check` and a `code-review` pass:

1. `de7cfd9` decisions and plan
2. `24ca331` reasons, breaker and config
3. `1d81846` inbox store
4. `909b35f` sender decision flow
5. `94eb6eb` retry scan and end-to-end failover
6. `d12121e` janitor failover
7. docs (this commit)

## Verification

- Unit tests in `internal/sender` cover each mode with fake providers, a fake checker and a fixed clock: retry then switch then `FAILED`; timeout with the provider holding the message, not holding it, or giving no usable answer, on the primary and on the fallback; an open breaker on each provider with one critical line per outage; expiry; a stale claim; and shutdown during the lookup.
- Store tests against real Postgres cover due-time claims, concurrent scans never taking the same row, the claim cap, and fencing for `Switch` and `Unresolve`.
- `internal/e2e` runs the whole path with every scenario equally likely from a fixed seed and compares each delivery's final state with what the simulator decided at every provider the sender used. All six scenarios are drawn at a primary and at a fallback. It passed three runs in a row.
- The breaker is off in that test, so its outcomes stay predictable; it has its own unit tests.
- `make up` was not run in this session.
