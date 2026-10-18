# Session log: Phase 4 sender and simulated provider

- **Date:** 2026-10-18
- **Tool:** Claude Code
- **Outcome:** Phase 4 built, issue #7, branch `feat/7-sender-simulated-provider`, pull request not yet opened (the engineer reviews and merges).

This is a summary of the session, not a transcript. The raw log is the session's `.jsonl` file under `~/.claude/projects/`.

## Planning

1. Asked to plan Phase 4 of `docs/notification_system_implementation_plan.md`. An exploration found the sender claiming inbox rows and logging, a `ProviderChecker` stub, and no provider port, webhook route, delivery transitions or simulator behaviour.
2. The engineer rejected the first plan with four changes, all adopted:
   - Webhook authentication differs by provider, so requests go to a `WebhookAuthenticator` port with one adapter per scheme (signature header or secret in an environment variable).
   - Simulation is one global mode that can be random, and the simulator registers the scenario it chose for every delivery.
   - Webhook payloads are stored in a `webhook_events` table linked to the delivery.
   - `failure_reason` is a closed enum.
3. The plan's open point, where Phase 4 stops and Phase 5 starts, was decided as: one attempt per claim; a provider failure or timeout is parked in `WAITING_RETRY` with its failure class, and the retry scan, failover, breaker and expiry check stay in Phase 5.
4. The plan was approved and saved to `_plans/phase-4-sender-simulated-provider.md`.

## Implementation

Workflow followed: issue #7, branch `feat/7-sender-simulated-provider`, decision log written before code, `make check` before each commit, one `code-review` pass per step, one commit per step. The engineer approved each step before the next began.

1. **Config and decision log.** Per-provider webhook settings, the `simprovider` block, the `Scenario` enum and the provider failure reasons.
2. **Provider port, HTTP adapter and registry.** Failures are classified as message, provider or timeout. The registry replaced the no-op checker with real status lookups.
3. **State machine, migrations and stores.** Guarded delivery moves, `deliveries.failure_reason`, `webhook_events`, a delivery loader, `WebhookStore.Apply`, and fenced `Complete`, `Fail` and `Park` on the inbox.
4. **Sender.** `Attempt` makes one call per claim and records the outcome.
5. **Simulated provider.** Six scenarios, a seeded random chooser, a registry of decisions, status lookup, and signed webhooks.
6. **Webhook endpoint.** One authenticator per scheme, then store-and-apply in one transaction.
7. **Compose, docs and the end-to-end test.** Webhook secrets in compose, this log, the status line, and a test that runs the whole path.

Departures from the written plan are listed at the end of `_plans/phase-4-sender-simulated-provider.md`.

Review findings fixed, by theme:

- **Secrets:** they print as `[redacted]`, and an empty one is refused at startup.
- **Shutdown:** the outcome write survives cancellation, so a stop between the provider taking a message and the write cannot cause a resend.
- **Retry timing:** the backoff exponent uses retries spent, not claims.
- **Webhooks:** a provider's webhook cannot settle another channel's deliveries; the pre-authentication body cap is small; a 4xx from the API is not retried by the simulator.
- **Simulator ordering:** a slow or timed-out call sends its webhook after the response it delays.

Review findings left, with reasons: the HMAC covers only the body, so a captured webhook can be replayed (a timestamped scheme would get its own authenticator); the simulator's registry and webhook goroutines are unbounded, which suits a simulator; expiry, the retry scan and the 429 `Retry-After` belong to Phase 5.

## After the build

The engineer asked whether a late webhook should be able to settle an `UNRESOLVED` delivery and decided yes: `UNRESOLVED` can be replaced by `DELIVERED` or `FAILED`, never the other way, with the rule in the application and no database constraint. The decision log was changed first, then the move table in `internal/domain`, which the webhook update's guard is built from, with tests in the domain and against Postgres.

## Verification

`internal/e2e` runs the API, relay, broker, sender, simulator and webhooks against real Postgres and RabbitMQ. It submits 60 recipients on SMS and email with every scenario equally likely from a fixed seed, then compares each delivery's final state with what the simulator's registry says it decided: delivered, failed with `BOUNCED` after a late failure webhook, failed with `PROVIDER_REJECTED`, parked after a transient failure, and delivered by webhook after a timeout. It also checks that every webhook was kept and applied once. Changing one expectation makes it fail, so it does check the states.
