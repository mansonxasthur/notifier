# Plan #25: Re-run the Phase 7 load results

Context: results in `docs/results/` predate #21 and #22. Runs, all on the same rig as `hold-150-sim2cores` (`SUT_CPUS=0-1 SIM_CPUS=2-3`, `-batch 100 -warmup 15s -duration 300s`), from `master` (9b12a07):

1. `after-hold-150`: shipped config (sender and queue batching).
2. `after-hold-150-msg1`: same with `relay.message_size: 1`, to separate #22 from #21 (new `MESSAGE_SIZE` env in `loadtest/run.sh`).
3. `after-hold-175`, `after-hold-200`: the edge and one step above.

Then record: messages published/s, broker CPU (docker-stats.csv), queue wait, first-attempt p99, versus the old runs; update `docs/results/README.md` and the decision log findings, and report gains or not. `loadtest/run.sh` resets the stack (authorised by the engineer for this task). Each configuration runs once.
