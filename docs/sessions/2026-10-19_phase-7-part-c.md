# Session log: Phase 7 Part C, the load driver and load runs

- **Date:** 2026-10-19
- **Tool:** Claude Code
- **Outcome:** Part C built on issue #19, branch `feat/19-load-test`. A load driver, a load-test stack, and recorded runs in `docs/results/`. Part D (fault injection) is not done. It continues the session of [Parts A and B](2026-10-19_phase-7-parts-a-b.md); the raw log is the same `.jsonl`.

This is a summary of the session, not a transcript.

## What was built

1. **Driver** (`cmd/loadtest`, `internal/loadtest`): an open-loop sender, a scraper for the services' `/metrics`, and a reader that computes p50, p95 and p99 request-to-`ACCEPTED` from `accepted_at - created_at`, split first attempt and retried, with the setup written beside the results. Unit tests cover the quantile estimate, the request body (valid, Arabic intact, no repeated ids), and the open-loop and skip behaviour.
2. **Stack and scripts:** `docker-compose.loadtest.yml` (built images, published metrics ports, a config per run, CPU pinning), `loadtest/run.sh` (one run from a clean stack), and `loadtest/ramp.sh` (a stepped ramp).
3. **`database.max_conns`** (shipped 20), because the plan said to settle the pool size.

## What happened

1. The first ramp passed at 100 recipients/s and failed at 500. The CPU samples showed the sender mostly idle, which pointed at the pool, so the pool became configurable. A clean A/B at 200 recipients/s then showed no difference between 4 and 30 connections, so the pool was not the limit, and the decision-log text claiming it was had to be corrected.
2. A later test with 30 connections appeared to pass at 200 recipients/s, but the run had dropped its failures from the measured set. The pass criterion was widened: any FAILED or UNRESOLVED delivery or an opened breaker fails a step.
3. A diagnostic with asynchronous commits seemed to point at fsync, and a 5-minute hold at 150 recipients/s failed. The sync-off hold failed too, which showed the fsync reading was wrong, and a hold with the simulator given a second core was clean. **The failures had been the rig's:** the single-core simulator stalled and the providers' 1 s timeout opened breakers.
4. With two cores each, 150 recipients/s (450 deliveries/s) was held for 5 minutes with every delivery within the target. 175 held for 5 minutes began failing deliveries, 200 passed no longer, and 250 queued without bound.
5. The mixed runs showed breaker amplification: a provider misbehaving on 6% of calls gave 15% failed deliveries, and the shipped weights 91%. The over-limit run, after its first version failed to exercise the limit, showed 429 with `Retry-After` and unharmed accepted work.

## Mistakes made and corrected during the session

- Reported a fsync cause for the ceiling before testing it properly; retracted.
- Let a 30-second step stand as a pass. Only 5-minute holds are called sustained now.
- An over-limit run with a burst larger than the overload; rerun with a smaller limit.
- A cleanup that deleted two valid result directories; their runs were repeated.
- Wait loops that matched their own command line and never ended, which left stuck background commands; the run scripts now have timeouts and write `FAILED.txt` on failure.

## Verification

- The driver reported exactly the 300 deliveries of a 10-second smoke run and a p99 that matched a hand query; a slowed provider moved the first-attempt p99 from 0.11 s to 0.63 s.
- `make check` and a `code-review` pass before the commit.

## Left open

- Why Postgres's throughput plateaus; whether the steps above 525 deliveries/s are partly limited by the rig; run-to-run noise.
- Part D: fault injection.
