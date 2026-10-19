# Plan #22: Queue messages carry batches of delivery ids

## Context

Issue #22. A queue message carries one `delivery_id` today: `Publisher.Publish` sends one AMQP message per id and `ports.Handler` takes one id. At 500K deliveries/s that is 500K msgs/s, more than one broker carries. At 100 ids per message it is ~5K msgs/s, so this removes the need to partition the broker. Phase 7 showed RabbitMQ was not the limit, so this is groundwork, and the load comparison must be reported whether or not it is a gain.

Done when: a message carries up to `relay.message_size` ids of one channel; the consumer hands the whole message to the sender; old single-id messages still work; a poison id cannot loop the batch; the five broker tests in the issue pass; Phase 7 e2e/resilience tests are unchanged in outcome.

Out of scope: sender DB batching (#21, merged), sharded queues, Kafka.

## Design

**Body format.** Decimal ids joined by `,` (`"123"` or `"123,124,125"`), `text/plain`. A single id is byte-identical to the old body, so the new consumer needs no sniffing and old in-flight messages parse unchanged. Text over varint: ~2 KB at 100 ids, debuggable in the management UI. Malformed token => reject without requeue (as today).

**Rollout (decision log).** Deploy senders first, then relays. An old sender cannot parse a multi-id body and would drop it as malformed, so relays must not publish batches until every sender is new. Rollback is the reverse (relays first). Setting `relay.message_size: 1` publishes the old shape and is the kill switch.

**Publisher.** `Publish(ctx, ch, ids)` keeps its signature. It chunks `ids` into messages of up to `message_size`, publishes each with a deferred confirm, and sets `confirmed[i]` per id from its message's confirm: a nack/timeout of a message leaves exactly its ids unconfirmed, and the relay's existing `release` per unconfirmed claim and per-row `MarkPublished` stay unchanged. `NewPublisher(conn, confirmTimeout, messageSize)`. One `enqueued_at_ns` header per message.

**Relay wait time.** The issue asks for "the longest the relay waits to fill a batch". The relay already claims up to `relay.batch_size` (2000) rows and groups them by channel, so a batch fills from the claim and a partial one is published at once. There is nothing to wait for, so I add no linger: added latency is 0 and the bound stays `poll_interval`. Recorded as a deliberate deviation in the decision log (a hold-back timer would only add latency).

**Handler.** `type Handler func(ctx, ids []int64) error`. Acking means the whole message was handled. For partial failure, `ports.BatchError{Failed []int64, Err error}`. Consumer behaviour on error:
- `BatchError` with some ids done: republish only `Failed` as a new message (own confirm-mode channel via an injected `ports.Publisher`, `Consumer.WithRepublisher`), then ack the original. If the republish is not confirmed, nack+requeue the original (after `requeueDelay`). Completed ids never come back unless that fallback fires.
- Any other error: nack+requeue the whole message, as today.

**Redelivery safety.** Whole-message redelivery is safe because `InboxStore.ClaimMany` is idempotent: an id already claimed returns `Duplicate` and is dropped; the republish-then-ack window and crashes mid-batch both end there. A test proves a partially processed batch redelivered does not resend.

**Poison id.** The only per-id permanent failure is `InboxClaimMissing` (no delivery row; impossible under the outbox invariant). Today it requeues forever. In batch mode the intake logs it at Error and drops it (not in `Failed`), so it cannot loop the batch. Transient errors (a claim-batch DB error) put the affected ids in `Failed`; they succeed once the DB is back, so no counter is needed.

**Sender intake.** `Intake.Handler` runs the ids of a message concurrently (a goroutine each, bounded by message size) into the existing claim `Coalescer`, so a 100-id message fills the claim batch at once instead of waiting for `linger`. It waits for all, collects failures into `BatchError`. `send` for won claims is unchanged. Concurrency is now `prefetch x message_size` sends, so lower the shipped `sender.prefetch` default and document it next to `batch_size`.

**Queue wait.** `recordWait` calls `Recorder.QueueWait` once per id in the message (a loop; no interface change), so histograms stay comparable. If the benchmark shows that loop on the profile, add a count argument then.

**Memory queue** (`rabbitmq/memory`): store messages as `[]int64`, chunk by a configurable size, same `Handler`.

## Steps

0. **Housekeeping.** Copy this plan to `_plans/queue-batching.md`; wait for approval. Branch `feat/22-queue-batching` off `master` (#23 merged).
1. **Decision log first** (`docs/notification_system_design_decision_log.md`, "Relay, queue and janitor" and a new short "Queue batching (#22)" section): body format and why, `message_size`, no relay linger and why, redelivery/partial-failure behaviour, poison handling, rollout order, `message_size: 1` kill switch. Update the "Queue names and message" bullet that says one id.
2. **Ports** (`internal/ports/queue.go`): new `Handler`, `BatchError`; update doc comments. Keep `Publisher`.
3. **RabbitMQ publisher** (`internal/adapters/rabbitmq/publisher.go`): chunking and per-message confirm mapping; body encoder. Shared `encodeIDs` / `decodeIDs` in a small `message.go`.
4. **RabbitMQ consumer** (`consumer.go`): decode, per-id wait recording, batch handler call, ack / republish-failed / nack paths above, `WithRepublisher`.
5. **Sender** (`internal/sender/intake.go`): the new handler; keep `WithBatching`/`Close`. `cmd/sender/main.go`: wire a republisher (`rabbitmq.NewPublisher`) into the consumer; stop it on shutdown.
6. **Config** (`internal/config/config.go`, `config/config.yaml`, `config_test.go`): `relay.message_size` (default 100, validated > 0); relay `main.go` passes it to `NewPublisher`; adjust `sender.prefetch` default and comments.
7. **Memory queue and other fixtures**: `memory.go`, `relay_test.go` (`failingPublisher`), `internal/e2e/e2e_test.go` constructor calls.

## Tests (real broker via `rmqtest`, `internal/adapters/rabbitmq/rabbitmq_test.go` and `internal/relay/relay_test.go`)

- Batch round trip: publish 250 ids with `message_size` 100 => 3 messages; handler sees them in batches, all ids once, depth drops to 0.
- Mixed formats: a hand-published old `"7"` body, a new `"1,2,3"` body and a malformed body on one queue; first two handled, malformed dropped, nothing stuck.
- Nack releases the right ids: a publisher with a confirm that nacks/times out one message (fake channel or relay-level `failingPublisher` over the real one) => only that message's ids are released and the rest are `MarkPublished`.
- Consumer stops mid-batch: cancel while the handler is blocked; the handler finishes the whole batch, the message is acked, nothing is redelivered after restart.
- Partial failure: handler returns `BatchError` for 2 of 10 ids => only those 2 reappear (new message), the other 8 are not redelivered; republish failure falls back to requeue and redelivery is dropped as duplicates by the intake (use the real `ClaimMany` in a sender-level test).
- Poison: a batch with a missing delivery id completes, the id is logged and dropped, the rest proceed.
- Broker restart: extend `TestBrokerRestartLosesNothing` (`relay_test.go:399`) to run with `message_size` > 1 and assert every delivery reaches the queue exactly-or-more-than-once and none is lost.
- Unit: `encodeIDs`/`decodeIDs` round trip and malformed inputs; config validation.
- Phase 7 e2e and resilience tests pass unchanged in assertions.

## Load comparison (separate step, needs approval)

`loadtest/run.sh` runs `docker compose down -v`, destroying the dev stack data. Run `hold-150-sim2cores` on `master` and on the branch (batch 1 vs 100 is also a useful third point), record msgs/s published, broker CPU, queue wait, first-attempt p99 in `docs/results/`, and report the outcome whether gain or not. I will not run it until you say so, as in #21's Part C.

## Critical files

`internal/ports/queue.go`, `internal/adapters/rabbitmq/{publisher,consumer,message(new)}.go`, `internal/adapters/rabbitmq/memory/memory.go`, `internal/sender/intake.go`, `internal/relay/relay.go` (unchanged logic; verify), `cmd/{relay,sender}/main.go`, `internal/config/config.go`, `config/config.yaml`, `internal/e2e/e2e_test.go`, decision log, `CLAUDE.md` status line, `docs/sessions/<date>_queue-batching.md`.

## Verification

- `make check` before each commit, then the `code-review` skill; Conventional Commits, no Co-Authored footer.
- `go test -race ./internal/adapters/rabbitmq ./internal/relay ./internal/sender ./internal/e2e`.
- Open a PR referencing #22; the engineer merges.

## Assumptions to confirm

1. No relay-side linger (the claim already fills batches); `relay.message_size` is the only new relay knob.
2. Poison/`Missing` ids are dropped with an Error log in batch mode instead of requeued forever.
3. Partial failure is handled by republishing only the failed ids; whole-message requeue is the fallback.
4. Default `message_size` 100; senders deploy before relays.
5. Load runs are a separate approved step.
