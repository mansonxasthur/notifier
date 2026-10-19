# Recorded results

Every directory and file here is raw output with the setup that produced it beside it. Nothing in the design document should cite a number that is not in this directory.

## The machine

One desktop, 4 cores (Intel Core i5-4570, 3.2 GHz), 15 GB of RAM, Ubuntu 26.04, Docker 26.1.3, Go 1.26.3. The whole stack runs on it: Postgres 17, RabbitMQ 4, Redis 7, the api, relay and sender, the simulated provider, and the load driver. **That is why it cannot show 500K deliveries a second, and why the figures below are about this machine.** The design's route to the peak is in the decision log.

## Benchmarks

`bench-<date>.txt` is `make bench`: function speed on one machine (validation, batch insert, outbox claim and publish, the bucket, the Redis limiter). It says how fast one call is, not how many the system carries.

## Load runs

`loadtest/run.sh <name> <recipients/s>` makes one run from a clean stack; `loadtest/ramp.sh` makes a stepped ramp. Each run directory holds `summary.md` (read this first), `params.json` (git SHA, flags, hardware), `config.yaml` and `config.diff` (the exact config, and how it differs from the shipped one), `sql.txt` (the queries and their raw answers), `samples.csv` (every metric sample, once a second), `final-*.prom` (the last scrape of each service), `docker-stats.csv` (CPU and memory of each container), `pg-stats.txt` (checkpoints and vacuums), `log-problems-*.txt` (warnings and errors, counted), and `env.txt`.

**Setup common to every run:** 3 channels per recipient, 100 recipients per request, so one recipient/s is 3 deliveries/s; the simulated provider always succeeds unless the run says otherwise; services log warnings only; durable commits (`synchronous_commit=on`); `database.max_conns` 20; the driver is open loop (it sends on schedule whether or not earlier requests have returned). **p99 is request to `ACCEPTED`, exact, from `accepted_at - created_at` in Postgres**, for the measured phase only (the warm-up is left out by recipient id). "First attempt" means the delivery was accepted by its channel's primary provider with no retry spent. The 5 s target applies to it.

**CPU split, which changed what the runs mean.** The simulated provider and the driver are not the system under test, so they are pinned to their own cores. The first runs gave them one core (`SUT_CPUS=0-2 SIM_CPUS=3`). At about 450 deliveries/s that one core could not keep up: the simulator answered slower than the providers' 1 s timeout, breakers opened, and deliveries failed over or failed. **Those runs measured the rig, not the system** (compare `headline-150` with `hold-150-sim2cores`, same load). Later runs use two cores each (`SUT_CPUS=0-1 SIM_CPUS=2-3`), and those are the ones to quote. `params.json` in older runs records `cores` as the driver's CPU affinity, not the machine's.

| Run | Split | Rate (recipients/s = deliveries/s) | Held | First-attempt p50 / p95 / p99 / max (s) | Outcome | Reading |
| --- | --- | --- | --- | --- | --- | --- |
| `ramp-100` | system 3, sim 1 | 100 = 300 | 30 s | 0.18 / 0.28 / 0.49 / 0.54 | all 9,000 delivered | Passes. Too short to trust (see below). |
| `ramp-150` | 3 / 1 | 150 = 450 | 30 s | 0.18 / 0.84 / 1.47 / 2.54 | all delivered | Passes for 30 s; failed when held 5 minutes (`headline-150`). |
| `headline-100` | 3 / 1 | 100 = 300 | 5 min | 0.18 / 0.35 / 0.55 / 1.11 | 90,000 of 90,000 delivered, no retries | Clean at the lower rate, on the rig that failed at 150. |
| `headline-150` | 3 / 1 | 150 = 450 | 5 min | 0.26 / 5.18 / 6.14 / 7.04 | 11,435 failed, 7,691 retried | **Rig-limited**: breakers opened because the single-core simulator stalled. Kept as the contrast for `hold-150-sim2cores`. |
| `diag-syncoff-hold-150` | 3 / 1 | 150 = 450 | 5 min | 0.16 / 2.96 / 4.23 / 4.91 | 17,031 failed | Diagnostic only, `synchronous_commit=off`; **never a result to quote**. Still failed, so commits waiting for the disk were not the cause of `headline-150`; the rig limit was. |
| `hold-150-sim2cores` | system 2, sim 2 | 150 = 450 | 5 min | 0.25 / 0.51 / 0.64 / 1.13 | **135,300 of 135,300 delivered**, no retries, no breaker activity, peak queue depth 54 | **The headline: the highest rate held for 5 minutes with every delivery within the target.** |
| `ramp2c-175` | 2 / 2 | 175 = 525 | 1 min | 0.32 / 0.87 / 1.01 / 1.33 | all delivered | Passes for 1 minute. |
| `hold-175-sim2cores` | 2 / 2 | 175 = 525 | 5 min | 0.89 / 3.25 / 3.67 / 4.35 | 152,762 delivered, **5,038 failed**, 826 retried; queue wait p99 about 4.7 s | Degrades when held: p99 is inside 5 s but deliveries fail. The edge. |
| `ramp2c-200` | 2 / 2 | 200 = 600 | 1 min | 1.56 / 5.37 / 5.76 / 6.24 | all delivered | Over the target. |
| `ramp2c-250` | 2 / 2 | 250 = 750 | 1 min | 8.49 / 17.9 / 18.8 / 19.0 | all delivered | Far over; queue wait grows (p99 about 29 s), peak queue depth 3,658. |
| `mixed-default-150` | 2 / 2 | 150 = 450 | 1 min | 0.23 / 1.15 / 1.32 / 1.45 | **24,860 of 27,300 failed** | The shipped random weights (about 30% of calls misbehave). See "Breaker amplification". |
| `mixed-mild-150` | 2 / 2 | 150 = 450 | 1 min | 0.25 / 0.56 / 1.11 / 1.70 | 23,188 delivered, 4,104 failed, 7 unresolved; 5,435 retried or failed over | A mild mix (94% succeed). Still 15% failed. |
| `overlimit-100` | 3 / 1 | 100 = 300 against a limit of 100/s | 1 min | 0.18 / 0.34 / 0.52 / 0.56 | 14 of 60 requests refused with 429 (`Retry-After` at most 2 s); the 13,800 accepted all delivered | Overload is refused at the door and accepted work keeps its latency. The 9,000-delivery burst lasts about 45 s at this deficit. |

## What the runs show

1. **On this machine the system sustains 450 deliveries/s (150 recipients/s) with first-attempt p99 0.64 s** over 5 minutes and 135,300 deliveries, none failed. At 525/s it holds p99 inside 5 s but starts failing deliveries; at 600/s p99 passes the target; at 750/s queue wait grows without bound. This is with two cores for the system, which includes Postgres.
2. **A short step proves little.** `ramp-150` (30 s) passed while the same rate failed when held. Anything quoted here was held for 5 minutes.
3. **Where the ceiling sits.** Postgres. Its mean CPU is 72% of a core at 450 deliveries/s, 85% at 525, and then stays at about 100% at both 600 and 750 while the load grows by a quarter, whereas the sender stays near 28% and the api near 25% (`docker-stats.csv`, CPU as a percentage of one core). Throughput stops growing where Postgres stops absorbing more, which is not the system's CPU quota (two cores). Every delivery costs several small transactions (accept, outbox claim and publish, inbox claim, completion, webhook event). **Why** Postgres plateaus (WAL commits serialising, or something else) was not isolated: the one diagnostic with commits made asynchronous was confounded by the rig limit. **The simulator also peaks at 180% to 190% of its two cores in the 525 to 750 steps, so those steps may be partly limited by the rig too.** The scale-up routes are in the decision log.
4. **Breaker amplification.** With a provider failing a fraction of calls, each channel's breaker (5 consecutive failures, open for 10 s) opens repeatedly, and when both providers of a channel are open a delivery fails at once (a Phase 5 decision). A provider whose calls misbehave 6% of the time in `mixed-mild-150` (2% fail, 1% time out, 1% fail late, 2% are slow) produced 15% failed deliveries, and the shipped weights (about 30% misbehaving) produced 91%. Why the failures cluster enough to trip a 5-in-a-row breaker so often was not established.
5. **Pool size** did not change the ceiling (4 and 30 connections gave the same p99 at 200 recipients/s on the first rig); 20 is shipped, sized to the services' concurrency. See the decision log.

## What is not shown

- 500K deliveries/s. This is one machine.
- Anything about a real provider: the simulator is faster and simpler than one.
- Run-to-run variation. Each configuration was run once, so it is **not demonstrated** that a rerun of the headline lands within noise.
- That the steps above 525 deliveries/s measure the system alone: the simulator is near its own limit there.
- Fault injection (provider down, Redis down, broker restart, killed relay or sender): Part D.

## Checks of the driver itself

Before the runs, a 10 recipients/s run of 10 s produced exactly 300 measured deliveries and a p99 (0.114 s) that a hand query of the table matched; and the same run with every call slowed by the simulator (500 ms) moved first-attempt p99 from 0.11 s to 0.63 s. Those directories were removed to keep this one to the runs that carry a claim.
