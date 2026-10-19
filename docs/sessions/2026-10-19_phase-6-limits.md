# Session log: Phase 6 limits and backpressure

- **Date:** 2026-10-19
- **Tool:** Claude Code
- **Outcome:** Phase 6 built, issue #11, branch `feat/11-limits`, pull request #12 (opened with the engineer's go-ahead; not merged, the engineer reviews and merges). One review finding is tracked as issue #13.

This is a summary of the session, not a transcript. The raw log is the session's `.jsonl` file under `~/.claude/projects/-home-manson-Projects-notifier/`, named `8b79e1ce-e4fe-4018-bb15-ed3c9e56aefb.jsonl`.

## Planning

1. Asked to plan Phase 6 of `docs/notification_system_implementation_plan.md`. An exploration found Redis already wired (compose, `REDIS_URL`, a client that exposed itself "to the rate limiter") but used only by the health check, no rate-limit config anywhere, and the breaker as the pattern for a new sender component.
2. The engineer asked whether the global limit is a rate limiter. It is: the decision log defines it as deliveries per second. The algorithm was open, and the engineer chose a token bucket in Redis for the global limit. One bucket then serves the Redis limiter, its local fallback and the per-provider limit.
3. Points settled in the plan without the engineer's input: the charge is made in the handler after validation, because only the handler knows the delivery count; a throttled send is deferred on the same provider and not held in a worker.
4. The plan was approved and saved to `_plans/phase-6-limits.md`.

## Implementation

Workflow followed: issue #11, branch `feat/11-limits`, decision log written before code, `make check` before each commit, one `code-review` pass per step. The engineer approved each step before the next began.

1. **Decision log and plan.** The token bucket, the all-or-nothing charge, the burst that must fit the largest request, the Redis script, the fallback, the deferral.
2. **Domain.** `Bucket` and `BucketPolicy` with a fake clock; the review added a free charge for zero or less, a cap on the wait, and a bound on the policy.
3. **Config.** `limits.global` and a `rate_limit` on every provider; the burst must cover the batch limit times the channels.
4. **Redis limiter.** The `Limiter` port and a Lua script that refills and takes in one step on the server clock. The review found an overflowing wait, an unbounded key expiry and a refusal that still wrote; all three were fixed.
5. **Local limiter and fallback.** The fallback has a per-call deadline, a cooldown and a single probe after it, and ignores a caller that gave up.
6. **Phase 5 fixes.** At the engineer's request, the janitor's not-found requeue now spends a retry and a stale breaker success is ignored. The engineer chose to keep failing a delivery at once when every breaker is open.
7. **API enforcement.** The handler charges after validation and answers 429 with `Retry-After`; the Redis client became atomic after the review found a race with the lazy scripter.
8. **Sender token bucket.** `ProviderLimits`, `InboxStore.Defer` and the `deferrals` column; the review moved the token after the breaker and capped the wait at the deadline.
9. **End-to-end cases.** A burst refused and then admitted after `Retry-After`, and a spike drained at the providers' rate.
10. **Follow-ups.** At the engineer's request, the items first left as accepted: the breaker tickets, the janitor's deadline check, a cap on free deferrals, and a scan per channel with 500 rows per channel. Reviews of these found a stuck-probe risk, an index that would walk other channels' rows, and a batch test that dead rows defeated; all three were fixed.
11. **Pull request.** The branch was pushed and PR #12 opened against `master`, with its migrations, known limits and verification listed in the description.
12. **Open review findings.** At the engineer's request, three findings left open in the PR were fixed: a timeout counts against the breaker only if the status lookup does not show the provider took the message; `EXPIRED` stores no failure class from either the sender or the janitor (`domain.FailureNone`, stored as `NULL`); and a lookup with no usable answer no longer logs a nil error. The review of that change argued that a provider slow on every call but taking each message now never trips its breaker; the engineer was told, and the decision log records the cost. A janitor requeue that sets no failure class was left and opened as issue #13.
13. **Docs.** This log, the status line, the plan's checklist and its deviations.

Departures from the written plan are listed at the end of `_plans/phase-6-limits.md`.

## Commits

One commit per step, each after `make check` and a `code-review` pass:

1. `9de711e` decisions and plan
2. `62c03ee` token bucket
3. `f56416a` config
4. `ae3c883` Redis limiter
5. `6b6aad0` local limiter and fallback
6. `8cf0877` Phase 5 fixes
7. `7f2e9dc` API enforcement
8. `b408747` sender token bucket
9. `7ef5704` only the probe ends a limiter outage (a review finding on the end-to-end step)
10. `b3aa7c3` end-to-end cases and docs
11. `51af597`, `395aa66` breaker tickets, then lost-probe replacement and ticketless results (follow-up the engineer asked for)
12. `22ecabc` the janitor honours the deadline
13. `83289d4` per-channel retry scan (500 a channel), `inbox.channel`, and the cap on free deferrals
14. `799f580` follow-ups recorded in the plan and session log
15. `99e99ed` timeouts and the breaker; no class for `EXPIRED`
16. this log (the last commit)

## Verification

- Domain tests move a fake clock through refill, burst, the all-or-nothing charge, a backwards clock, a huge wait and non-finite rates.
- The Redis limiter runs against a real Redis (a new `redistest` helper): the burst, a refused charge taking nothing and writing nothing, refill after the returned wait, four instances sharing one budget of 100 exactly, key expiry, and an unreachable Redis returning an error.
- The fallback is tested with a fake shared limiter (outage logged once, recovery once, cooldown, one probe, a cancelled caller) and with a real Redis limiter pointed at a closed port.
- Handler tests cover 429 and the `Retry-After` rounding, the exact charge, no charge before authentication or validation, a limiter failure, and a burst refused and then admitted.
- Sender tests cover a throttled send being deferred with no retry, class or claim spent, the breaker's probe slot, an open breaker wasting no token, the deadline cap, and a spike of 40 deliveries against 10 calls a second that never exceeds the bucket. Store tests cover `Defer` fencing and the claim cap with and without deferrals.
- `internal/e2e` adds a refused-then-admitted burst through the real handler and a spike drained through the real stack. The second was checked by raising the provider rate to 20,000 a second, which made it fail.
- The breaker tests cover a timeout the provider confirms (the breaker stays closed over four of them), one it denies and one the lookup cannot judge (both open it), the probe slot returned after a confirmed timeout and after shutdown cut the lookup short, and the `NULL` class on `EXPIRED` rows from the sender and the janitor.
- `make up` was not run in this session, and nothing here measures the 500K a second peak (Phase 7).

## Left open

- Issue #13: the janitor's requeue spends a retry but writes no failure class.
- A provider slow on every call that takes each message never trips its breaker; a latency-based trip is not built.
- The provider bucket is per sender, backlogs are re-claimed in rounds, and the global fallback allows up to N times the rate when Redis is down. All are in the decision log.
- The database pool size is not configured; with up to 1,500 sends in flight per sender it is the practical bound. Phase 7 sizes it.
