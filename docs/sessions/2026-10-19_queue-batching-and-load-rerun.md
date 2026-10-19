# Session log: queue batching (#22) and the Phase 7 Part C re-run (#25)

- **Date:** 2026-10-19
- **Tool:** Claude Code
- **Outcome:** #22 merged as PR #24 (queue messages carry up to `relay.message_size` delivery ids). The Part C load runs were repeated on `master` after #21 and #22 (PR #26, `docs/results/README.md`, "After batching"). Part D (fault injection, #27) is planned in `_plans/phase-7-part-d.md`, not built.

This is a summary of the session, not a transcript. See also [queue batching](2026-10-19_queue-batching.md).

## What happened

1. Planned #22 (comma-joined ids so the old body still parses; senders deploy first; no relay linger because the claim already fills batches), built it with real-broker tests, and ran the code review. Four of its eight findings were fixed (republish used the handler's expired context, no back-off on the split path, the memory queue ignored `BatchError`, a style nit); four were left with reasons.
2. Re-ran `hold-150-sim2cores` and the 175 and 200 holds from `master`. At 450 deliveries/s Postgres CPU fell from 72% to 43%, sender CPU from 19% to 7%, first-attempt p99 from 0.64 s to 0.47 s and queue wait p99 from 0.45 s to 0.02 s, nearly all of it from #21. Queue batching was within noise on this rig.
3. At 525 and 600 deliveries/s more deliveries failed than before (breakers open; the simulator is at 102-116% CPU and its webhooks to the api fail). Not isolated; the old "Postgres is the ceiling" reading no longer holds.

## Mistakes made and corrected

- The first one-id run (`after-hold-150-msg1`) kept the new `prefetch` default of 4, so it measured the prefetch, not queue batching. It is kept in the README, labelled, and was repeated with `prefetch` 200 (`MESSAGE_SIZE` and `PREFETCH` options added to `loadtest/run.sh`).
- AMQP messages published per second was not recorded, so it is not reported.

## Open

- Why the breakers open above 450 deliveries/s; 10 deliveries DELIVERED without `accepted_at` in `after-hold-150-msg1-prefetch200`.
- Part D (#27): needs approval of the plan before building.

## Part D (#27)

Built `loadtest/faults.sh` (`make fault name=<run>`), per-provider simulator modes (`simprovider.providers`, config only), `cmd/faultreport` and `internal/loadtest/fault.go` (recovery time, duplicate sends), and ran eleven fault runs (twelve directories; `both-down-run1` is the first of two). Results and the verdict per run are in `docs/results/README.md`, "Fault injection". Nothing lost or duplicated in any run.

Mistakes and corrections: the first launch used `-batch 100` with 75 recipients/s and the driver refused it; the first report counted a half-open fallback breaker as not recovered (it stays half-open without traffic); the first `both-down` run could not show the critical log line because `log-problems` keeps only the top 20 lines, so `critical-lines.txt` was added and the run repeated.

Open: the provider runs inject by restarting the simulator, whose 0.6 s blip opens the fallback breakers too, so their FAILED counts overstate the fault; a runtime switch in the simulator would fix that. `UNRESOLVED` is not cleanly exercised. Each run once.
