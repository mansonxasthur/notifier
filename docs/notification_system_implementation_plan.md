# Notification System: Implementation Plan

Oct 16, 2026 · @Mohamed Ibrahim

## Approach

Build in thin vertical slices, durability first, so each phase leaves a system that runs and can be tested. The order follows the message path: accept, persist, relay, send, then everything that makes sending survive failure.

- **Reviewers read, they do not run.** Every phase ends with code, tests, and a note for the design document, because the evidence has to live in the repository.
- **One codebase, three entry points:** API, relay, and sender. Hexagonal layout with ports for storage, the queue, and providers.
- **Local stack in docker-compose:** PostgreSQL, RabbitMQ, one Redis (breaker state and rate limits), the simulated provider, and the three entry points. No deployment is required by the brief.
- **Core and enhancements.** Core, built and tested: the accept path, the outbox and the relay, publishing with confirms, the inbox and the sender with one provider call per attempt, the timeout loop with failover and its `UNRESOLVED` end, the janitor's two stuck-row sweeps, webhooks, expiry, the global limit, the simulated provider, and the load test. Enhancements, described in the log under future work and built only if time allows: the reconciler sweep for `ACCEPTED` deliveries without a receipt (with the `UNRESOLVED` it produces and the per-channel thresholds), and the breaker's shared Redis state (a local breaker per sender is built).
- **Each decision stays traceable** to the Design Decision Log. Changes to a decision are recorded there first.

## Phase 1: Foundations

Goal: an empty but wired project that builds, starts three entry points, and talks to its dependencies.

- [x] Repository layout with `domain`, `ports`, `adapters`, and `cmd` for `api`, `relay`, and `sender`
- [x] docker-compose with PostgreSQL, RabbitMQ, one Redis (breaker state and rate limits), and the simulated provider service
- [x] Migrations for notifications, deliveries, outbox, and inbox tables (every column NOT NULL except next\_attempt\_at and failure\_reason in the outbox, and next\_attempt\_at, failure\_class, and failure\_reason in the inbox; contact and content as jsonb; outbox keyed by delivery\_id with state, retries, last\_try, next\_attempt\_at, and failure\_reason; inbox keyed by delivery\_id with state, provider, retries, claim\_count, last\_try, next\_attempt\_at, failure\_class, and failure\_reason), including the unique constraint on `(notification_id, recipient_id, channel)` and partial indexes: outbox `next_attempt_at` for the relay (an empty value counts as due now), `last_try` on both tables for the janitor, and inbox `next_attempt_at` for the sender scan
- [x] Configuration loading: per-notification-type expiry values, provider order, per-provider call timeout, whether it supports a status lookup, maximum retries, and backoff with jitter, the batch limit (3,000 recipients), poll intervals for the relay and the sender scan, and janitor thresholds
- [x] `slog` setup with stdout as the default handler
- [x] CI that runs lint and unit tests

Done when all three entry points start, connect, and exit cleanly. Graceful shutdown is mandatory and is built with each entry point.

## Phase 2: Accept path

Goal: the Dispatcher validates, persists durably, and acknowledges, with correct duplicate handling.

- [x] HTTP endpoint for single and batch requests with API key authentication
- [x] One API key, sent in `X-API-KEY` and compared with the configured value; identifying the calling service is out of scope
- [x] Per-recipient validation with reason codes, run before any database write, including a check that every listed channel has content in an accepted shape (text for SMS, inline content or a template ID with variables for email and WhatsApp)
- [x] A request over the recipient limit (3,000, so at most 9,000 deliveries with three channels) is rejected, and inserts run in chunks of about 5,000 rows per statement, all inside one transaction
- [x] In-memory de-duplication of repeated recipients inside one payload (the first occurrence of each recipient and channel wins; repeats count under `duplicates`)
- [x] Batch insert of deliveries with `ON CONFLICT DO NOTHING ... RETURNING`, plus outbox rows for the inserted rows only, in one transaction (see the SQL recommendations below)
- [x] Expiry stamped on each delivery as `expires_at` from the request's optional expires\_at, or else the notification type's default; a deadline in the past is rejected at validation, and any future deadline overrides the default
- [x] The Dispatcher never talks to the broker: the outbox rows written in the same transaction are the only hand-off to the queue
- [x] Response with `accepted`, `duplicates`, and `rejected` (with reasons), and the status codes from the log
- [x] Idempotent retry test: the same request twice creates no new rows, and the same key with a different payload is treated as a duplicate (the first request wins)

**SQL recommendations for the accept path**

- **One transaction per request.** Insert the notification, then the deliveries and outbox rows chunk by chunk, then commit. The default READ COMMITTED isolation is enough.
- **The notification takes two steps.** Insert with `ON CONFLICT DO NOTHING`. If no row comes back, the key already exists, so read it in a second statement. A single statement that combines the insert and the lookup can miss a row that a concurrent request committed a moment earlier.

```sql
INSERT INTO notifications (idempotency_key, type, content)
VALUES ($1, $2, $3)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING id;

-- No row returned means the key already exists:
SELECT id FROM notifications WHERE idempotency_key = $1;
```

- **Deliveries and outbox in one statement per chunk,** with arrays and `unnest`, so the statement always has five parameters however many rows it carries. The outbox rows come from `RETURNING`, so only newly inserted deliveries get one.

```sql
WITH inserted AS (
  INSERT INTO deliveries (notification_id, recipient_id, channel, contact, expires_at, state)
  SELECT $1::bigint, r.recipient_id, r.channel, r.contact, $2::timestamptz, 'PENDING'
  FROM unnest($3::text[], $4::text[], $5::jsonb[]) AS r(recipient_id, channel, contact)
  ORDER BY r.recipient_id, r.channel
  ON CONFLICT (notification_id, recipient_id, channel) DO NOTHING
  RETURNING id
)
INSERT INTO outbox (delivery_id, state, retries, last_try)
SELECT id, 'NEW', 0, now() FROM inserted;
```

- **Counts come from the command tag.** The row count of the outer `INSERT` is the number accepted, and the chunk's rows minus that number are the duplicates.
- **Chunk size.** Up to about 5,000 rows per statement, so a request of 3,000 recipients with three channels (9,000 rows) is two statements in the same transaction. With a plain multi-row `VALUES` list the limit is 65,535 bind parameters, which is about 10,900 rows at six parameters each, so `unnest` avoids the limit and a fourth channel or an extra column cannot break it.
- **Order the rows** by `(recipient_id, channel)` before inserting, so two overlapping requests take their locks in the same order and cannot deadlock.
- **Do not use `COPY`.** It has no `ON CONFLICT`, so it would need a staging table, which is more machinery than this accept path needs.
- **Remove duplicates in memory first.** The plan already does this for repeated recipients inside one payload, which keeps the database check as the safety net.

Done when a 1,000-recipient batch containing one duplicate and one invalid row returns the expected counts, and the database holds exactly the accepted rows.

## Phase 3: Relay, queue and janitor

Goal: the relay and the janitor get every accepted delivery onto the queue at least once, and a crashed relay or sender loses nothing.

- [ ] `Publisher` and `Consumer` ports, with a RabbitMQ adapter and an in-memory adapter used only in tests
- [ ] One durable queue per channel (email, SMS, WhatsApp), with persistent messages that carry only the `delivery_id`
- [ ] Relay claims batches with `FOR UPDATE SKIP LOCKED`, taking only rows whose `next_attempt_at` is empty or has passed, sets `PROCESSING`, adds one to `retries`, and sets `last_try`, with pickup delay kept small by a short poll interval
- [ ] Publisher confirms: mark `PUBLISHED` only after the broker acknowledges, and return a failed publish to `NEW` with a later `next_attempt_at`
- [ ] Fencing token on relay claims (the outbox `retries` counter) and an attempt cap with a dead state that records a `failure_reason`
- [ ] Sender intake: insert or find the inbox row, claim it with a conditional update to `PROCESSING` (adding one to `claim_count`), and acknowledge the message only after that write
- [ ] Janitor sweeps inside the relay: reclaim outbox rows stuck in `PROCESSING`, and reclaim inbox rows stuck in `PROCESSING` (using that provider's timeout plus a margin as the threshold, with the provider check before any resend); the reconciler for `ACCEPTED` deliveries without a receipt is an enhancement
- [ ] One outbox row per recipient per channel, matching its delivery row
- [ ] Tests against real Postgres and RabbitMQ: two relays never take the same row, a killed relay's rows are recovered, a restarted broker loses nothing, and a redelivered message is handled once

Done when killing a relay mid-batch, or restarting the broker, ends with every delivery processed and nothing stuck.

## Phase 4: Sender and simulated provider

Goal: a deliverable message path end to end, with a provider that behaves realistically.

- [ ] `Provider` port and a registry that maps each channel to its providers
- [ ] Simulated provider, run as its own small service in docker-compose, with its modes (succeed, fail permanently, fail transiently, slow, time out, accept then send a late failure webhook) set by configuration, so slow and timed-out calls can be tested against each provider's timeout; it accepts both content forms (inline content, or a template ID with variables)
- [ ] The simulated provider honors the `delivery_id` reference key, echoes it in webhooks, and offers a status lookup by that key (which can be switched off per provider)
- [ ] Sender consumes one channel queue, records the delivery in the inbox (a conditional update to PROCESSING decides which worker sends), and makes one call to one provider
- [ ] Delivery states `PENDING`, `ACCEPTED`, `DELIVERED`, `FAILED`, `UNRESOLVED` with guarded transitions, where `PENDING` may go straight to `FAILED` (and to `DELIVERED` or `FAILED` on an early webhook)
- [ ] Webhook endpoint (served by the API entry point) with signature check, fast acknowledgement, and matching by the `delivery_id` reference key
- [ ] One log line per attempt with provider, failure class, and outcome (no extra table)

Done when a message flows from request to `DELIVERED` through a simulated provider, including the late-webhook path.

## Phase 5: Resilience

Goal: the system keeps its promises when providers misbehave. This phase holds most of the design's difficult decisions.

- [ ] Error classification: message-level errors fail immediately with a reason, and provider-side failures and timeouts retry, with retry state in the inbox (WAITING\_RETRY, retries, next\_attempt\_at) and a sender scan for due rows
- [ ] Per-provider retries from configuration: retry on the same provider with backoff and jitter up to its maximum, then switch to the other provider with `retries` reset, and end the delivery if that one is exhausted (`FAILED` at once for a 5xx, saving the `failure_reason`)
- [ ] Timeout path: check the provider by `delivery_id` when it supports a lookup. Yes means `ACCEPTED`, no or not found retries the same provider, and a check that times out, returns a 5xx, or cannot be made moves to the other provider. The janitor uses the same check for an inbox row stuck in `PROCESSING`, and the check uses the provider's configured timeout
- [ ] Per-provider circuit breaker, local to each sender in this build, with a short cache and a local state machine; an open breaker switches provider without spending retries (the shared Redis state is an enhancement)
- [ ] Every provider's breaker open: `FAILED` with `NO_PROVIDER_AVAILABLE` and one critical log line
- [ ] On the second provider the loop closes: a timeout with no usable answer ends the delivery `UNRESOLVED` and a 5xx ends it `FAILED`, with no return to the first provider
- [ ] Expiry check before every send: deliveries never sent by their deadline become `FAILED` with the reason `EXPIRED`
- [ ] Enhancement, built only if time allows: the reconciler sweep for `ACCEPTED` deliveries past a per-channel threshold, using guarded transitions, with deliveries the provider never confirms ending `UNRESOLVED` at their deadline
- [ ] Out of scope, documented only: events to the preference service for invalid contacts and opt-outs

Done when each simulated failure mode ends in the state the Decision Log describes, with a test per mode.

## Phase 6: Limits and backpressure

Goal: overload is refused cleanly at the door, and bursts that were accepted drain at a pace providers can take.

- [ ] Global rate limit in deliveries per second, enforced in the Dispatcher
- [ ] Limit value configurable and sized from the system's 500K per second peak
- [ ] 429 with `Retry-After` when a limit is exceeded
- [ ] Shared rate-limit counters in Redis, used by every Dispatcher instance, and falling back to a local in-memory limiter when Redis is unreachable
- [ ] Per-provider token bucket in the sender in front of every provider call
- [ ] Burst handling: reject above a limit with 429 and Retry-After; accepted requests are held in the queue and the outbox

Done when a burst above the limit is rejected with 429 and Retry-After, and a spike inside the limit is drained without exceeding any provider's rate.

## Phase 7: Verification

Goal: evidence a reviewer can read without running anything. The brief asks for load test results and failure behavior, so this phase produces numbers and the setup that produced them.

- [ ] Unit tests for domain rules: validation, state transitions, expiry, error classification
- [ ] Integration tests against real Postgres and RabbitMQ (for example with testcontainers) for `ON CONFLICT`, the conditional claim, and crash recovery
- [ ] End-to-end tests through the API with the simulated provider in each failure mode, including Arabic (UTF-8) content from the request to the provider
- [ ] Go benchmarks for hot paths such as batch validation and insert
- [ ] Load test of the full stack (k6, vegeta, or a small Go driver) with p99 latency from request to ACCEPTED against the 5 second target, reported separately for first-attempt and retried deliveries, and with queue wait time measured
- [ ] Fault-injection runs: provider down, slow, timing out, late failure webhook, Redis unavailable, broker restarted, relay or sender killed
- [ ] Metrics for queue wait, queue depth, end-to-end latency, and breaker state, captured during the runs

Done when every failure mode and the load test have recorded results with the exact setup, hardware, and parameters.

## Phase 8: Deliverables

Goal: a repository and a document that explain every decision without needing to run the code.

- [ ] OpenAPI specification in the repository, covering idempotency, the accepted, duplicates, and rejected response, and error codes, with reason codes and failure values written once as an enum in the spec and reused in code
- [ ] Design document with: architecture diagram, key trade-offs (including the Kafka-first, Asynq, and table-queue alternatives, and why none was chosen), difficulties and how they were solved, load test and failure results, and what would be done differently with more time
- [ ] Decisions and trade-offs drawn from the Design Decision Log, with every item the author can defend
- [ ] Assumptions section up front, from the log's scope table
- [ ] AI session records: the Claude Code session logs, and the design discussion summary
- [ ] Final read-through of the code for smells, dead code, and unclear names

Done when a reviewer can follow the design from the document alone and find each claim backed by code or a recorded result.

## Risks and open decisions

The largest risk is proving the 500K per second and 5 second claims with a single-machine setup. The table lists what to settle early.

| Risk or open decision | Why it matters | Plan |
| --- | --- | --- |
| Database write throughput | 500K deliveries per second means 500K or more inserts per second, whatever the batching | Batch inserts, partitioning, sharding by tenant or recipient; state the limits of a single machine honestly, and if outbox churn shows up in the load test, delete finished outbox rows and tune autovacuum |
| RabbitMQ as the queue | One broker node caps throughput well below 500K messages per second | Document the scale-up path (partitioned streams such as Kafka, or sharded queues, behind the ports) |
| Rate limit semantics | Rejecting above the limit drops the bursts the system exists to absorb | Reject above a limit with 429 and Retry-After, hold accepted work in the queue and the outbox, and record the reason in Phase 6 |
| Retry delay mechanism | RabbitMQ has no per-message delay without a plugin | Keep retry timing in the inbox in Phase 5: store next\_attempt\_at and let a sender scan re-attempt when due, with a small backoff and jitter per provider |
| Load test credibility | Reviewers will not run the code, so numbers must carry the argument | Record setup, hardware, parameters, and raw results in the document |
| Priority between types | A password reset should not wait behind a broadcast | Out of scope; listed as a future enhancement |
| Graceful shutdown | Mandatory for all three entry points, but not designed in detail yet | Implement with each entry point |
