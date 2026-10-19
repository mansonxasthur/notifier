# Phase 7: Verification: implementation plan

## Context

Phases 1-6 are built. The test suite already covers much of the Phase 7 checklist (domain rules, Postgres `ON CONFLICT` / claim / fencing, relay crash and broker-restart recovery, an end-to-end run through the simulator in every scenario with Arabic content). What is missing is the evidence the brief asks for: **no benchmarks, no metrics, no load test, no recorded fault-injection results**. The decision log (lines 268-310) fixes what they must show: p99 request to `ACCEPTED` against 5 s for first-attempt deliveries, reported apart from retried ones, queue wait measured next to it, and a small metrics set (queue wait, queue depth, end-to-end latency, breaker state).

Goal: evidence a reviewer can read without running anything.
Done when: every failure mode and the load test have recorded results with the exact setup, hardware and parameters (implementation plan, Phase 7).

Hardware reality: this machine has 4 cores and 15 GB, and runs Postgres, RabbitMQ, Redis, the simulator, the three services and the load driver together. It cannot show 500K/s. The result is stated as the highest sustained rate at which p99 stays under 5 s, the bottleneck found at the next step up, and the scale-up path (decision log, risks table). No extrapolation presented as measurement.

Not in scope: tracing, dashboards (Prometheus/Grafana stack), a multi-machine run, changing the 5 s target, fixing bottlenecks the load test finds beyond config tuning (those are recorded, and fixes become their own issues).

## Gap audit (what exists vs. the checklist)

| Checklist item | State | Phase 7 work |
| --- | --- | --- |
| Domain unit tests | Done (`state`, `bucket`, `backoff`, `scenario`, `content`, validation) | Audit for missing expiry and error-classification cases only |
| Integration tests, real Postgres/RabbitMQ | Mostly done (`postgres/*_test.go`, `relay_test.go`, `rabbitmq_test.go`) | Add: concurrent senders claiming one row (exactly one wins), a killed sender recovered by the janitor, concurrent relays on one outbox |
| E2E through the API per failure mode | One mixed-scenario run, Arabic included | Add one pinned-scenario case per mode so each is asserted alone; assert Arabic bytes arrive at the provider unchanged |
| Go benchmarks | None | Add (step 2) |
| Load test | None | Add (steps 4-5) |
| Fault injection | Partial tests (broker restart, killed relay, Redis down at the limiter) | Full-stack runs (step 6) |
| Metrics | None | Add (step 3) |

## Steps

### 0. Housekeeping (CLAUDE.md workflow)
- Wait for approval of this plan.
- One GitHub issue and one branch per task, off `master`, in this order, each its own PR: **A** tests and benchmarks (steps 1-2), **B** metrics (step 3), **C** load driver and load results (steps 4-5), **D** fault-injection runs (step 6). C needs B; D needs C. Docs (step 7) go with D.
- `make check` and the `code-review` skill before each commit; PRs not merged by me.

### 1. Close the test gaps (issue A)
- Postgres integration: N goroutines call `Claim` on one inbox row, exactly one succeeds; N relays `Claim` one outbox batch, no row twice (`SKIP LOCKED`).
- Sender kill: a row in `PROCESSING` after a "killed" sender is recovered by the janitor and sent once (mirror of `TestKilledRelayIsRecovered`).
- E2E: one test per scenario with the simulator's mode pinned (config already supports pinned scenarios), asserting final state, provider used, and failure reason. Assert the UTF-8 Arabic text is byte-identical in what the simulator recorded.
- Audit `domain` and `requests` tests for expiry boundary and error classification; add only what is missing.

### 2. Benchmarks (issue A)
- `requests.Validate` over a batch (batch_limit recipients x 3 channels), with `-benchmem`.
- Postgres batch insert of deliveries + outbox (`notifications.Create`) at several batch sizes, against a testcontainer.
- Outbox claim + mark-published batch; `domain.Bucket.Take`; Redis limiter `Allow` (one round trip).
- `make bench` target; raw output committed to `docs/results/bench-<date>.txt` with `go version`, CPU model and core count. Benchmarks are labelled as function speed, not capacity (decision log line 301).

### 3. Metrics (issue B)
Decision to record in the decision log first: `github.com/prometheus/client_golang`, one `/metrics` endpoint on each service's existing HTTP server (relay and sender get a small one beside their health check), no Prometheus server; the load driver scrapes the endpoints and stores the samples. A `ports.Recorder` interface keeps the domain and services free of the library; the adapter lives in `internal/adapters/prometheus`.

| Metric | Type | Where | Labels |
| --- | --- | --- | --- |
| `notifier_queue_wait_seconds` | histogram | sender, on start of work: now minus enqueue time | channel |
| `notifier_accept_latency_seconds` | histogram | sender, at transition to `ACCEPTED`: now minus `deliveries.created_at` | channel, `attempt` = `first` \| `retried` (retries > 0 or provider switched) |
| `notifier_queue_depth` | gauge | relay, passive queue declare each few seconds | channel |
| `notifier_breaker_state` | gauge (0 closed, 1 half-open, 2 open) | sender | provider |
| `notifier_requests_total`, `notifier_rate_limited_total` | counters | api | status |

Open design points:
- **Enqueue time.** Messages carry only `delivery_id` (decision log). Plan: the relay sets an `enqueued_at` message **header**, body unchanged. Alternative: read an `outbox.published_at` column. Header avoids a database read per message.
- **Request-to-ACCEPTED needs a stored timestamp** for the SQL cross-check after a run: add a nullable `deliveries.accepted_at` set in the same conditional update that moves the row to `ACCEPTED` (new migration). This also gives the p99 a source independent of the histograms.
- Histogram buckets chosen around 5 s (0.05 ... 30 s); a p99 from buckets is interpolated, so the driver reports the exact p99 from `accepted_at - created_at` in SQL and uses the histograms for shape only.

Tests: recorder fake in sender/relay tests asserting the labels (`first` vs `retried`), queue-depth gauge against a real broker, `/metrics` served and shut down with the service.

### 4. Load driver (issue C)
- `cmd/loadtest` (small Go driver, no new load tool; vegeta/k6 cannot read `accepted_at` or scrape our metrics, and a Go driver reuses the request types). Flags: target URL, rate (recipients/s), duration, batch size, channels, warm-up, output dir.
- Open-loop arrival: requests are sent on a ticker at the fixed rate whether or not earlier ones returned (closed-loop hides latency). Records HTTP latency, 202/429 counts, `Retry-After`.
- After the run, waits for the queue to drain (or a timeout), then queries Postgres: p50/p95/p99/max of `accepted_at - created_at`, split first-attempt vs retried (`retries = 0` and primary provider), counts by final state; and scrapes `/metrics` every second during the run into a CSV (queue wait, depth, breaker).
- Writes `docs/results/load-<date>-<run>/{params.json,summary.md,samples.csv,sql.txt}`; `params.json` holds the git SHA, config file, hardware, versions, rate, and duration.
- Compose profile `loadtest` with a config that sets `simulation: true`, mode pinned to `succeed` for the headline run, and provider rate limits raised above the test rate so the provider bucket is not the thing measured (a second run keeps it to show the by-design limit).

### 5. Load runs (issue C)
Stepped ramp, each step 60 s after a 15 s warm-up, the stack reset between steps: 100, 500, 1K, 2K, 5K, 10K recipients/s (x3 channels = deliveries/s; the unit is stated in every table). Stop when p99 > 5 s or errors appear. Then:
1. **Headline:** the highest passing step held for 5 minutes. p99 first-attempt vs retried, queue wait, depth.
2. **Mixed:** the same rate with the default random-scenario weights, to show the first-attempt and retried split.
3. **Over the limit:** a rate above `limits.global`, showing 429 + `Retry-After` and that accepted work keeps its latency.
4. **Bottleneck:** one step above the limit of the passing run, with CPU (`docker stats`) and Postgres `pg_stat_activity` / table bloat sampled, naming what saturates first. Check outbox churn (decision log line 139): if it shows, tune autovacuum or delete finished outbox rows, and record the before/after.
Also settle the unconfigured database pool size (left open by Phase 6) with the sender's 1,500 concurrent sends in mind; record the value chosen and why.

### 6. Fault-injection runs (issue D)
Each run: steady load at 50% of the headline rate for 2 minutes, inject the fault at 30 s, hold or release as stated, then record: errors seen by the client, p99 first-attempt, final-state counts after drain, lost or duplicated deliveries (must be zero lost; duplicates counted), time to recover, breaker and queue-depth traces, and the log lines that prove the behaviour. Provider scenarios use a separate config file per run (`loadtest/configs/`), so no runtime-control code is added to the simulator.

| Run | Injection | Expected (from the decision log) |
| --- | --- | --- |
| Provider down | primary `fail_transient` for 100% | retries then switch, breaker opens, fallback carries load |
| Both providers down | both `fail_transient` | `FAILED` at the last provider, critical log line when the last breaker opens |
| Provider slow | primary `slow` | latency rises, no failures; timeout stays below 5 s |
| Provider timing out | primary `timeout` | status lookup, `ACCEPTED` or switch; second provider timeout ends `UNRESOLVED` |
| Late failure webhook | `late_failure` | `ACCEPTED` then `FAILED` via the webhook; bad signature rejected |
| Redis unavailable | `docker compose stop redis` | local fallback limits; intake continues; recovers on restart |
| Broker restarted | `docker compose restart rabbitmq` | relay retries unconfirmed publishes; nothing lost |
| Relay killed | `docker compose kill relay` | outbox rows reclaimed after the janitor timeout; nothing lost |
| Sender killed | `docker compose kill sender` | `PROCESSING` rows requeued by the janitor; at most one send per claim |
| Postgres restarted | `docker compose restart postgres` | requests fail with 5xx during the outage, no partial writes; services reconnect |

A `make fault name=<run>` target drives each one through `cmd/loadtest` plus a shell script; results go to `docs/results/fault-<run>-<date>/`.

### 7. Docs
- Decision log: metrics decision and the `accepted_at` column (before the code), plus the measured limits and what each run changed (after).
- `docs/results/README.md` indexing every run with its setup, so Phase 8's design document links to recorded results instead of restating them.
- Phase 7 checklist ticks in the implementation plan, `CLAUDE.md` Status (Phases 1-7), and `docs/sessions/<date>_phase-7-verification.md`.

## Critical files
`internal/adapters/postgres` (new tests, benchmarks, `accepted_at` write), `migrations/<ts>_add_delivery_accepted_at`, `internal/ports/recorder.go` (new), `internal/adapters/prometheus/` (new), `internal/relay`, `internal/sender` (instrumentation), `internal/httpserver` (`/metrics`), `internal/e2e` (pinned-scenario cases), `cmd/loadtest/` (new), `loadtest/configs/` (new), `Makefile` (`bench`, `fault`), `docker-compose.yml` (loadtest profile), `docs/results/` (new).

## Verification
- `make check` stays green; new tests run under `-race`.
- Metrics tests use a fake recorder for labels and a real broker for queue depth.
- The load driver is checked against a known case first: a tiny run (10 recipients/s, 10 s) whose summary must match a hand count from SQL, and a deliberately slowed provider (`slow`) that must push p99 up in the report.
- Each result directory is reproducible from its `params.json` (SHA, config, command line); rerunning the headline step lands within noise.
- No claim in `docs/results/` without raw data beside it.

## Assumptions to confirm
1. `prometheus/client_golang` plus `/metrics` is acceptable as a new dependency, with no Prometheus server in compose.
2. The enqueue time travels as a message header (body stays `delivery_id` only).
3. A new `deliveries.accepted_at` column is acceptable for measuring request-to-`ACCEPTED`.
4. A custom Go open-loop driver instead of k6/vegeta.
5. "First attempt" means `retries = 0` on the primary provider; anything else is "retried".
6. Four PRs (A-D) rather than one large phase PR.
7. Single-machine results are reported as such; the 500K/s figure is argued by the scale-up path, not claimed.

## Deviations made while building Part B

- **Enqueue time is a custom header (`enqueued_at_ns`, Unix nanoseconds), not the AMQP `timestamp` property**, which has one-second resolution. Redelivered messages are not measured.
- **`ports.Recorder` has three methods** (queue wait, accept latency, queue depth). Breaker state is a collector over `Breakers.States()` registered on the Prometheus adapter, and the request counter is an HTTP wrapper on it, so neither needed a port. `notifier_rate_limited_total` was dropped: `notifier_requests_total{status="429"}` is the same number.
- **A breaker appears in `notifier_breaker_state` after its first use**, since the sender creates breakers lazily.
- **The relay and the sender gained a small HTTP server** (health and `/metrics`) on `http.addr`, as the decision log says. Scrapes of `/metrics` are not counted as requests.
- **`accepted_at` is set by the same statement for the sender and the janitor**; a delivery a webhook finishes first has none. The sender reports accept latency whenever its inbox write applied.
- **Checked on the real stack** (`docker compose up`): the sender's histogram gave 0.1496 s and SQL `accepted_at - created_at` gave 0.1485 s for the same delivery.

## Deviations made while building Part C

- **The CPU split became part of the method.** A first pass with one core for the simulator and the driver showed failures that were the rig's, not the system's; a second pass with two cores each gave clean results at the same load. The plan assumed the simulator was cheap.
- **A step is not a hold.** The stepped ramp's pass criterion was widened (any FAILED or UNRESOLVED delivery, or an opened breaker, fails a step) after a step that passed on p99 alone had thousands of failed deliveries. The headline is the highest rate held 5 minutes, found by holding, not by the ramp.
- **`database.max_conns`** was added (shipped 20) because the plan said to settle the pool size. The runs did not show it to be the limit.
- **The Postgres bottleneck was not isolated.** The plan asked to name what saturates first; the data names Postgres as where throughput stops growing, not why. A diagnostic with asynchronous commits was confounded by the rig limit. The outbox churn check (`pg-stats.txt`): in the 5-minute holds dead tuples stayed under 2% of rows with 6 autovacuums and one checkpoint; in the overloaded 750/s step they reached 9% of `deliveries` and 13% of `inbox` with only 2 autovacuums, so vacuum lags under overload. No tuning was done.
- **Mixed runs** use the default weights and a milder mix, both at 150 recipients/s. They found the breaker amplification above.
- **Fewer rates than planned.** The planned ramp went to 10K recipients/s; the machine fails at 200, so the ramp stopped there. Larger steps would have measured the rig.
- **Not done:** a rerun of the headline to show run-to-run noise, and the plan's separate "second run keeps the provider limit" case.
- **Loadtest stack files:** `docker-compose.loadtest.yml` is a separate file used without the dev override; `loadtest/run.sh` writes a `FAILED.txt` and tears the stack down if a run cannot start, so a run never waits forever.
