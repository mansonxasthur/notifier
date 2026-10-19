# Session log: Phase 7 verification, plan and Parts A and B

- **Date:** 2026-10-19
- **Tool:** Claude Code
- **Outcome:** The Phase 7 plan was approved and saved to `_plans/phase-7-verification.md`. Part A (issue #15, pull request #16) and Part B (issue #17, pull request #18) were built, reviewed and merged by the engineer. Part C (load driver and load runs) was started and stopped at the engineer's request; it is not covered here.

This is a summary of the session, not a transcript. The raw log is the session's `.jsonl` file under `~/.claude/projects/-home-manson-Projects-notifier/`, named `c98420f5-ced1-4150-a242-024cf152872b.jsonl`.

## Planning

1. Asked to plan Phase 7. A look at the repository found that much of the checklist already existed: domain tests, Postgres integration tests for `ON CONFLICT`, the conditional claim and fencing, relay and janitor recovery tests, and an end-to-end run through the simulator in every scenario with Arabic content. What was missing was the evidence the brief asks for: no benchmarks, no metrics, no load test, no recorded fault-injection results.
2. The plan was written to `_plans/phase-7-verification.md` with a gap audit table, four parts (A tests and benchmarks, B metrics, C load driver and runs, D fault injection), the metric definitions, a stepped load ramp, ten fault-injection runs with the behaviour the decision log predicts, and seven assumptions to confirm.
3. The hardware limit was stated up front: a 4-core, 15 GB machine runs the whole stack and the driver, so it cannot show 500K a second. The result is to be the highest sustained rate with p99 under 5 s and the bottleneck above it, with the scale-up path argued, not claimed.
4. The engineer approved the plan as written. Work followed the repository workflow: an issue and a branch per part, the decision log before code, `make check` and a `code-review` pass before each commit, a pull request at the end, no merge.

## Part A: test gaps and benchmarks (issue #15, PR #16)

1. The audit in the plan overstated the gaps. Concurrent sender claims, concurrent relays and stuck-inbox recovery already had tests, so none were added, and the pull request says so.
2. **End-to-end:** each simulator scenario pinned and asserted alone, a confirmed timeout settling at the primary provider without failover, and Arabic text and variables checked as raw UTF-8 bytes at the provider (the simulator's requests are recorded by a wrapper).
3. **Benchmarks:** batch validation, batch insert, outbox claim and publish, the bucket (allowed and denied), and the Redis limiter, with `make bench`. The output is saved in `docs/results/bench-2026-10-19.txt` with the machine and base commit. The shared Postgres and Redis test helpers were widened from `*testing.T` to `testing.TB` for this.
4. **Review findings fixed:** a timeout test that accepted any path to DELIVERED, a silent skip of unlisted scenarios, a strict call count that a retry could break, an ignored body-read error, a missing denied-path benchmark, and a results header that named a commit that did not contain the benchmarks. Left as known: each Postgres benchmark function starts its own container, so the figures are cold-database numbers.

## Part B: metrics (issue #17, PR #18)

1. **Decision log first.** A Metrics section: Prometheus client, `/metrics` on each service, no Prometheus server, a `ports.Recorder` so the services do not depend on the library.
2. **Queue wait** uses an `enqueued_at_ns` message header set by the publisher, not the AMQP `timestamp` property, which has a resolution of one second. The body stays the `delivery_id` alone. Redelivered messages are not measured.
3. **Accept latency** is observed by the sender when its inbox write moves a delivery to `ACCEPTED`, labelled `first` (primary provider, no retry) or `retried`. A migration adds `deliveries.accepted_at`, set in the same statement for the sender and the janitor, so the load driver can compute the exact p99 from SQL.
4. **Queue depth** is read by a new relay sampler through a passive queue declare. **Breaker state** is a collector over `Breakers.States()`, which counts a breaker whose open period has passed as half-open. The API counts requests by status; a scrape is not counted.
5. The relay and the sender gained a small HTTP server (health and `/metrics`) on `http.addr`.
6. **Checked on the real stack** with `docker compose up`: the sender's histogram gave 0.1496 s and SQL `accepted_at - created_at` gave 0.1485 s for the same delivery, and all three services served `/metrics`.
7. **Review findings fixed:** a bounds check before indexing the provider order, `Unwrap` on the status-recording writer, an immediate first depth sample, breaker series for every configured provider from the start, and the CLAUDE.md status. Recorded in the decision log as known limits, not fixed: histograms mix clocks across hosts, queue depth excludes unacknowledged messages, a delivery that waited through rate-limit deferrals still counts as `first`, and `/metrics` is unauthenticated on the service listener. The plan's final section lists where the build differed from the plan (the header, three `Recorder` methods, no `rate_limited` counter).

## Verification

- `make check` passed for both parts before each commit, including `-race` over the real Postgres, RabbitMQ and Redis tests.
- Part B's new tests cover the Prometheus output and bucket edges, the accept-latency labels and the case where nothing is reported for a failure or a stale claim, the breaker states, queue wait and depth against a real broker, the depth sampler, `accepted_at` on completion and on a stale claim, and the schema test's list of nullable columns.

## Left open

- **Part C** (load driver and load runs) and **Part D** (fault injection) are not done. Work on Part C was stopped at the engineer's request and is not part of this log.
- The Phase 7 checklist in `docs/notification_system_implementation_plan.md` and the CLAUDE.md status are not final: the load test and fault-injection items stay unticked until Parts C and D are done.
