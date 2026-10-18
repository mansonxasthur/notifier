# Session log: Phase 3 relay, queue and janitor

- **Date:** 2026-10-18
- **Tool:** Claude Code
- **Outcome:** Phase 3 built, issue #5, branch `feat/5-relay-queue-janitor`, pull request opened, not merged (the engineer reviews and merges).

This is a summary of the session, not a transcript. The raw log is the session's `.jsonl` file under `~/.claude/projects/`.

## Planning

1. Asked to plan Phase 3 of `docs/notification_system_implementation_plan.md`. An exploration found the Phase 3 config keys, the outbox and inbox tables and the `amqp091-go` dependency already in place, and no ports, publisher, consumer or reconnect logic.
2. The engineer's answers to the first plan:
   - Queues are named by channel only (`email`, `sms`, `whatsapp`), with no prefix.
   - The message holds only the `delivery_id`.
   - `next_attempt_at` is null by default and set only when a retry is scheduled.
3. Further questions, answered with the recommended options: the Phase 3 sender logs and leaves the inbox row `PROCESSING`; a duplicate message is acked and dropped; a claim over `sender.max_claims` ends the inbox row `DEAD` and the delivery `FAILED`.
4. The plan was approved and saved to `_plans/phase-3-relay-queue-janitor.md`.

## Implementation

Workflow followed: issue #5, branch `feat/5-relay-queue-janitor`, decision log written before code, `make check` before each commit, one `code-review` pass over the relay, janitor and sender work.

Departures from the written plan:

- `Consumer` handlers take a delivery ID and return an error (nil acks) instead of a `Message` with `Ack` and `Nack`.
- `Publisher.Publish` returns per-message confirmations even when it also returns an error, so a partial failure keeps the earlier confirmations.
- Broker reconnect is lazy: `Conn.Channel` redials when the connection is closed, so no `Failer` is needed.
- The inbox sweep and the sender intake share one `InboxStore`, extended with `ListStuck`, `Requeue` and `Accept`.

After the first pull request was opened, the engineer asked for three changes, which were made on the same branch:

1. `relay.batch_size` default raised from 500 to 2,000.
2. A configurable `relay.workers` (default 8): each worker runs its own claim-and-publish loop with its own broker channel, and `SKIP LOCKED` keeps workers off each other's rows. This replaced the single loop per relay.
3. `MarkPublished` takes the whole batch and updates all confirmed rows in one fenced statement (arrays of delivery IDs and `retries`), instead of one update per row. Unconfirmed rows are still released one at a time.

The decision log was updated first, tests cover a batch with one stale claim, and a second `code-review` pass raised two findings that needed no change (publishers are not closed under workers that missed the stop deadline; `relay.workers` is validated in config).

Review findings fixed: a failed send after a won claim no longer requeues the message (the redelivery would only be a dropped duplicate); a failing handler waits before the requeue, so an outage is not a hot loop; handlers get a deadline because they no longer see the consumer's cancellation; the relay builds one outbox store.

Review findings left for later phases: the sender is wired to a logging stub until Phase 4; a failed provider check is treated as unknown, which Phase 5 refines per the decision log; channel groups of one batch are published one after another.

A test run showed that a broker outage longer than the relay's total backoff window ends rows `DEAD` by design, so the restart test raises the attempt cap.
