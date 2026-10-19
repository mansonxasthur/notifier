# Plan #27: Phase 7 Part D, fault-injection runs

## Context

Part C measured healthy and overloaded runs. What is missing (`docs/notification_system_implementation_plan.md`, Phase 7) is evidence for the failure behaviour the decision log promises: retries then failover, breakers, `UNRESOLVED` on a double timeout, the late-failure webhook, the Redis fallback, and no lost deliveries across restarts of the broker, relay, sender and Postgres. Existing tests cover some of these in isolation (broker restart, killed relay, Redis down at the limiter); this runs them on the full stack under load and records the results.

Done when: each run in the table below has a directory in `docs/results/` with raw data, a pass/fail against its stated expectation, and a line in the README; zero lost deliveries in every run, duplicates counted; findings that contradict the decision log are reported, not smoothed over.

## Method (from `_plans/phase-7-verification.md` step 6)

Steady load at **50% of the headline rate: 75 recipients/s = 225 deliveries/s**, 15 s warm-up then 120 s measured, fault injected 30 s into the measured phase, held or released as stated, then drain. Same rig as the headline (`SUT_CPUS=0-1 SIM_CPUS=2-3`), shipped config except where a run says otherwise. `sender.prefetch`/`message_size` as shipped (after #21 and #22).

Per run, record: client-visible errors (status counts in the driver summary), first-attempt p99 and retried p99, final-state counts after drain, lost deliveries (rows still PENDING or without a terminal state after drain; must be 0), duplicate sends (simulator ledger, `GET /scenarios`, counted per delivery and provider), time to recover (first second after the fault when p99 and queue depth are back to the pre-fault level, from `samples.csv`), breaker and queue-depth traces (`samples.csv`), and the log lines that show the behaviour (`log-problems-*.txt`).

## Runs

| Run | Injection | Expected |
| --- | --- | --- |
| provider-down | primary of each channel fails transiently, 100% | retries, switch to the fallback, breaker opens, fallback carries the load, all delivered |
| both-down | both providers of each channel fail transiently | `FAILED` at the last provider; critical log when the last breaker opens |
| provider-slow | primaries answer after `slow_delay` | latency rises, no failures, below 5 s |
| provider-timeout | primaries hang past their timeout | status lookup, then `ACCEPTED` or switch; a second-provider timeout ends `UNRESOLVED` |
| late-failure | `late_failure` | `ACCEPTED` then `FAILED` through the webhook; a bad signature is rejected (one probe request) |
| redis-down | `docker compose stop redis`, restart after 30 s | local fallback limits, intake continues, recovers on restart |
| broker-restart | `docker compose restart rabbitmq` | relay retries unconfirmed publishes; nothing lost; batch messages (`message_size` 100) survive |
| relay-killed | `docker compose kill relay`, start after 30 s | outbox rows reclaimed by the janitor (a relay runs it, so the janitor is down too until restart); nothing lost |
| sender-killed | `docker compose kill sender`, start after 30 s | `PROCESSING` rows recovered by the janitor; at most one send per claim |
| postgres-restart | `docker compose restart postgres` | api returns 5xx during the outage, no partial writes, services reconnect |

## Steps

0. **Housekeeping.** Issue #27, branch `test/27-fault-injection` (done). This plan is copied to `_plans/`; wait for approval.
1. **Decision log first:** record the method and the one config addition below.
2. **Per-provider scenarios (the one code change).** `simprovider.mode` is global, so "primary down, fallback healthy" cannot be configured. Add an optional `simprovider.providers: {email-a: fail_transient, ...}` map to config and `Chooser`: a listed provider uses its own mode (a scenario name or `random`), others use the global one. Config only, no runtime control; validated like `mode`; unit tests in `simprovider_test.go` and `config_test.go`.
3. **Fault runner.** `loadtest/fault.sh <run>` (and `make fault name=<run>`): builds the run's config from `loadtest/configs/<run>.yaml` (a sed overlay on the shipped config, like `run.sh`), calls `loadtest/run.sh` with `-warmup 15s -duration 120s`, and a background injector that sleeps to warm-up + 30 s and executes the run's injection (a `docker compose` command, or nothing for config-only provider runs). `run.sh` gains `FAULT_AT`/`FAULT_CMD`/`RELEASE_AFTER`; the injection and release times are written to `fault.json` in the result directory so recovery time can be computed from `samples.csv`.
4. **Report additions** (`internal/loadtest`): duplicate sends from the simulator ledger, lost count (terminal states vs requested), and a recovery time from the sample series. The driver already records status counts, states and p99.
5. **Run all ten**, one at a time, from a clean stack; any run that does not behave as expected is repeated once to separate flakiness from a finding, and both are kept.
6. **Docs:** a "Fault injection" section in `docs/results/README.md` (table of runs, expectation, result, evidence), tick Part D in the implementation plan and `CLAUDE.md` Status, session log.
7. `make check` and the code-review skill before each commit; PR referencing #27; the engineer merges.

## Risks and notes

- Every run resets the stack (`docker compose down -v`); no dev data survives. About 5 minutes a run, so about an hour for all ten.
- Killing the relay also stops the janitor and the outbox publish; the expected recovery needs the relay back, so the run restarts it after 30 s and measures from there.
- The 175/200 failures after batching (README, "After batching") may make `provider-*` runs noisy; at 225 deliveries/s the rig was clean in earlier runs.
- Each run is done once (twice if it surprises). The README will say so; no claims about run-to-run variation.

## Assumptions to confirm

1. The small config addition in step 2 is acceptable (the Phase 7 plan said no runtime-control code in the simulator; this is config only).
2. Load of 75 recipients/s for all runs, 120 s measured.
3. One PR for all of Part D.
