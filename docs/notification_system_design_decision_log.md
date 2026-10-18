# Notification System: Design Decision Log

Oct 16, 2026 · @Mohamed Ibrahim

## Scope and assumptions

The assignment leaves several points open, so this log fixes them up front.

| Question | Assumption |
| --- | --- |
| Who calls the Dispatcher? | Trusted internal services, identified by API key. End users never call it; they are recipients. |
| What does 500K/s mean? | Ambiguous: individual requests or notifications. Support single and batch requests, and state the capacity math for both. |
| Who owns contacts and preferences? | A separate service. The notification service only delivers. |
| Who filters by preference? | The producer resolves recipients and their allowed channels before calling. |
| What do API key permissions mean? | The API key is how internal services authorize their access to the notification service. |
| How are idempotency keys scoped? | Producers use UUIDs, and a key is unique across all producers. |
| Who owns message templates? | The provider. The service forwards inline content, or a template ID with variables, and never stores or renders templates. |
| Real providers? | No. A simulated provider, run as its own small service in docker-compose, with its behavior (succeed, fail, slow, time out, late webhook) set by configuration. |
| What counts as delivered for the 5 second target? | Acceptance by the provider. The brief describes the Sender as delivering notifications to the external providers, and the service cannot control delivery by the carrier or to the end user. |

The accepted cost of the contacts decision: contact data is a snapshot taken by the producer, so a number can be stale by send time.

## Architecture overview

&#91;embedded content: architecture · 3 entry points, RabbitMQ as the queue\]

The Dispatcher writes the notification, the deliveries, and the outbox rows in one transaction and only then acknowledges. The relay publishes outbox rows to RabbitMQ, with one queue per channel. The sender consumes the messages and calls the providers, and the janitor sweeps inside the relay heal anything that gets stuck. Three entry points (API, relay, and sender) ship from one Go codebase and scale independently. The API entry point also receives the providers' webhooks.

## Request model and API

One endpoint accepts one notification for 1 to N recipients. Each recipient carries its own contact details and the channels it accepts, and the producer sends a request-level idempotency key.

```json
{
  "idempotency_key": "uuid",
  "type": "match_alert",
  "expires_at": "2026-10-20T18:00:00Z",
  "content": {
    "email": { "template_id": "match_started_v2", "variables": { "team_a": "الهلال", "team_b": "النصر" } },
    "sms": { "text": "مباراة الهلال والنصر تبدأ الساعة 8 مساءً" },
    "whatsapp": { "template_id": "match_started", "variables": { "team_a": "الهلال", "team_b": "النصر" } }
  },
  "recipients": [
    { "recipient_id": "u_1", "channels": { "email": "a@x.com", "sms": "+20..." } },
    { "recipient_id": "u_2", "channels": { "email": "b@x.com" } }
  ]
}
```

- **Channels mean fan-out.** Listing email and SMS sends on both. There is no cross-channel fallback; failover happens between providers inside one channel.
- **Unit of work** is one delivery per recipient and channel. Its identity is `(notification_id, recipient_id, channel)`, where the notification is found by the request's idempotency key, and the delivery's `delivery_id` is the idempotency key used everywhere downstream.
- **Idempotency keys.** Producers use UUIDs, and a key is unique across all producers. A repeat of a key with a different payload is treated as a duplicate: the first request wins, and the payload is not compared.
- **Optional deadline.** `expires_at` is an absolute UTC timestamp, for example the start of a match. If the producer omits it, the notification type's configured expiry applies. A deadline already in the past is rejected, since every delivery would expire on arrival. A future deadline overrides the type's expiry however far away it is. The cost, accepted on purpose, is that a producer typo can keep deliveries in retry and reconciliation for longer.
- **Content per channel.** The producer supplies content for each channel it uses, and the service forwards it without storing or rendering templates. SMS takes plain text. Email and WhatsApp take either inline content or a `template_id` with `variables`, which are shared by the whole request. A recipient listed on a channel that has no content is rejected with a reason code. Content is forwarded as UTF-8, so Arabic text passes through unchanged.
- **Batch limit.** A request carries at most 3,000 recipients, a value kept in configuration (so at most 9,000 delivery rows with three channels, with headroom for more channels), and the insert runs in chunks of about 5,000 rows. A request over the limit is rejected whole with 413, and nothing is written. Each recipient can have several channels, so one request can create several times as many delivery rows.
- **Authentication.** There is one API key, sent in the `X-API-KEY` header and compared with the value loaded from configuration. Identifying the calling service is out of scope, so nothing about the caller is stored.
- **Inline content shape.** Inline email content is `{subject, body}` and inline WhatsApp content is `{body}`. The alternative is `{template_id, variables}` for both.
- **Validation first.** Invalid rows are rejected individually with a reason code, before the database write, so they cannot poison a batch.
- **Duplicates are success.** A row that already exists is skipped, not failed.
- **No ordering guarantee.** Notifications are not delivered in the order they were accepted.

Response to a partly accepted batch:

```json
{
  "accepted": 987,
  "duplicates": 10,
  "rejected": [
    { "recipient_id": "u_42", "reason": "INVALID_RECIPIENT" }
  ]
}
```

| Situation | HTTP status |
| --- | --- |
| At least one row accepted | 202 Accepted |
| Missing or wrong API key | 401 |
| Malformed body | 400 |
| More recipients than the batch limit, or a body over the size cap | 413 |
| Replay of a key where every row already exists | 202, all rows counted as duplicates |
| Every recipient rejected | 422 |
| Database or infrastructure failure, nothing accepted | 5xx, safe to retry with the same key |

Reason codes for a rejected recipient: `INVALID_RECIPIENT` (empty id, an id over 255 bytes or with a control character, or no channels), `INVALID_CONTACT` (not an email address or an E.164 number), `MISSING_CONTENT` (a listed channel has no content), `INVALID_CONTENT` (content not in an accepted shape, or text with a control character other than a line break or tab) and `UNKNOWN_CHANNEL`. One bad channel rejects the whole recipient with the first reason found, so a recipient is never half accepted. A valid recipient repeated inside one payload is not rejected: the first occurrence of each `(recipient_id, channel)` wins and the repeats count under `duplicates`. Every invalid entry is reported as rejected, so a bad repeat is never hidden and a valid repeat of an invalid entry is never dropped; a recipient can therefore appear in both `rejected` and the accepted count.

Some problems reject the request as a whole with 422 and an error code: `INVALID_IDEMPOTENCY_KEY` (not a UUID), `NO_RECIPIENTS`, `UNKNOWN_TYPE` (a type that is not configured) and `EXPIRED_DEADLINE` (`expires_at` is not in the future).

Validation rejections are not retryable. After fixing the data, the producer resends with the same idempotency key: accepted rows are skipped as duplicates and the fixed rows go through.

## Accept path and durability

The Dispatcher persists a request before acknowledging it, using a transactional outbox, so an acknowledged message survives a crash.

On accept, one database transaction writes three kinds of rows:

- **A notification row**, holding the shared content once, with the request's idempotency key as a unique key.
- **Delivery rows**, one per recipient and channel, with a unique constraint on `(notification_id, recipient_id, channel)` and a business state (`PENDING` and onward).
- **Outbox rows**, one per delivery. They are the relay's work list, with an operational state (`NEW`, `PROCESSING`, `PUBLISHED`, and a dead state).

Business state answers "did the user get it?" and operational state, in the outbox and the inbox, answers "has the work been published, and has it been sent?". They are kept separate on purpose.

**Batch insert without batch failure.** A single duplicate must not fail 1,000 rows. The insert uses `ON CONFLICT DO NOTHING ... RETURNING`, and outbox rows are created only for rows that were actually inserted. Valid rows that did not come back were duplicates.

**Why synchronous.** The acknowledgement means the data is durable. If the database fails, the producer receives a 5xx and retries safely. This costs accept latency, which is the deliberate trade-off.

**Publishing after commit.** The Dispatcher never talks to the broker. After the commit, the relay claims outbox rows and publishes them to RabbitMQ with publisher confirms, so accepting requests does not depend on the broker being up.

**Costs to state honestly:**

- Batching reduces HTTP requests and commits, but not rows. 500K deliveries per second still means 500K or more inserts per second, so the database write path is the bottleneck. Mitigations: batch inserts, partitioning, sharding by tenant or recipient.
- One outbox row per recipient per channel, matching its delivery row, doubles the insert volume. The alternative, a single row per batch expanded by the relay, would halve the writes and was not chosen.
- The strongest argument for the database over a durable queue alone is unique-constraint dedup and a queryable status table.

## Data model

&#91;embedded content: data model · 4 tables, one outbox row and one inbox row per delivery\]

Four tables hold what the Dispatcher accepts, the work to publish, and the work to send. Every column is NOT NULL, except `next_attempt_at` and `failure_reason` in the outbox, and `next_attempt_at`, `failure_class`, and `failure_reason` in the inbox, which stay empty until they apply.

- **`notifications`** holds what a whole request shares: the idempotency key (unique), the `type`, and the `content` as JSON, stored once.
- **`deliveries`** holds one row per recipient and channel: a sequential `id`, a foreign key to `notifications`, the `recipient_id`, the `channel`, the `contact` as JSON, the resolved `expires_at`, the business `state`, and created and updated times. The `id` is the `delivery_id` used as the provider reference key.
- **Unique key** on `(notification_id, recipient_id, channel)`. This is the duplicate check for a delivery.
- **Retried requests.** A producer retry with the same idempotency key finds the existing notification through its unique key, and the deliveries insert skips rows that already exist. Both steps run in one transaction.
- **JSON for content and contact.** It leaves room for new channels or context without a migration. The cost is that the database cannot check that an email delivery has an email, so accept-time validation carries that check, and contact data sits inside JSON, which matters for later purging or masking.
- **`expires_at` on each delivery**, so the sender can check the deadline before every send without looking up the notification. It repeats for every delivery of a notification, which is accepted.
- **`updated_at`** records the last state change, which is useful for audit and would let the reconciler enhancement tell how long a delivery has waited in a state.
- **`outbox`** has one row per delivery and is the relay's work list: `delivery_id` (primary key and foreign key), the operational `state` (indexed: `NEW`, `PROCESSING`, `PUBLISHED`, and a dead state), `retries`, `last_try`, `next_attempt_at`, and `failure_reason`. It is a narrow table on purpose, so updates rewrite little data.
- **`outbox.retries` counts claims.** It counts attempts to publish, increments on every claim, and backs the fencing token and the attempt cap. `next_attempt_at` is the earliest time the relay may claim the row. It is empty for a new row, which may be claimed at once, and a failed publish or an expired claim returns the row to `NEW` with a later value, from configured backoff with jitter. `last_try` is set when the row is created and at every claim, and the janitor uses it to find stuck claims. `failure_reason` is empty until a row exceeds its retries, when the reason is saved.
- **`inbox`** has one row per delivery and tracks the sender's work: `delivery_id` (unique key and foreign key, the only constraint), the operational `state` (`PROCESSING`, `WAITING_RETRY`, and terminal states), the current `provider`, `retries`, `claim_count`, `last_try`, `next_attempt_at`, `failure_class`, and `failure_reason`.
- **`provider` and `inbox.retries`.** `provider` is the provider the next attempt uses, and `retries` counts retries on that provider. When the retries exceed that provider's configured maximum, the sender switches `provider` and resets `retries`. Which provider is primary, and each maximum, come from configuration.
- **`claim_count` never resets.** It increments on every claim and backs the inbox's fencing token and overall attempt cap. The final update of an attempt is conditional on the value the worker read, so a slow worker cannot overwrite a newer one.
- **`failure_class` and `failure_reason`.** `failure_class` records how the last attempt failed (message, provider, or timeout), so the next attempt knows whether to check the provider first. `failure_reason` is empty until a job ends in failure, when the reason is saved, for example `NO_PROVIDER_AVAILABLE`.
- **A duplicate is decided by state.** The sender inserts or finds the inbox row and wins a conditional update to `PROCESSING`. The loser drops the message, and a legitimate retry finds the row waiting.
- **Retry timing.** After a transient failure the sender stores `next_attempt_at` in the inbox, from configured backoff plus jitter, so the value is fixed when it is written. Retries never go back through the outbox or RabbitMQ.
- **A crashed sender** leaves a row in `PROCESSING`. The janitor finds rows whose `last_try` is older than that provider's timeout, plus a margin, and recovery checks the provider by `delivery_id` before any resend.
- **Indexes.** Outbox: `next_attempt_at` where `state = 'NEW'`, for the relay. Both tables: `last_try` where `state = 'PROCESSING'`, for the janitor. Inbox: `next_attempt_at` where `state = 'WAITING_RETRY'`, for the sender scan.

**Write cost, and the alternative.** Each delivery costs an insert, a claim update, and a final update on the outbox, the same again on the inbox, and one more update for each failed attempt. This is the accepted trade-off of tracking every step in Postgres. If the load test shows pressure on these tables, the alternative is to keep only live work in them: delete finished rows (the permanent result already lives on `deliveries`), partition by channel, and tune autovacuum for them.

## Relay, queue and janitor

RabbitMQ, with one durable queue per channel, carries the work. The relay publishes from the outbox, the sender consumes, and the janitor sweeps run inside the relay. Each entry point scales independently.

- **Queue.** RabbitMQ with one durable queue per channel and persistent messages. A message carries only the `delivery_id`, and a slow SMS provider cannot block email.
- **Relay claiming.** Each relay selects a batch of `NEW` rows whose `next_attempt_at` is empty or has passed, with a pessimistic lock (`FOR UPDATE SKIP LOCKED`), marks them `PROCESSING`, adds one to `retries`, and sets `last_try`. `SKIP LOCKED` lets several relays work without blocking each other or taking the same row.
- **Publisher confirms.** A row is marked `PUBLISHED` only after the broker acknowledges the message. A failed publish returns the row to `NEW` with a later `next_attempt_at`.
- **Fencing token.** The final update of a claim is conditional on the `retries` value the relay read, so a slow relay whose row was reclaimed updates zero rows instead of overwriting the new owner.
- **Sender intake.** The sender inserts or finds the inbox row for the message, claims it with a conditional update to `PROCESSING`, and acknowledges the message only after that write, so a redelivered message is harmless.
- **Janitor, inside the relay.** Two sweeps are built: one reclaims outbox rows stuck in `PROCESSING`, and one reclaims inbox rows stuck in `PROCESSING`, where recovery checks the provider by `delivery_id` before any resend. A third sweep, the reconciler for deliveries that sit in `ACCEPTED` without a receipt, is an enhancement (see Out of scope and future work).
- **Attempt cap.** A row that keeps failing moves to a dead state for inspection instead of cycling forever, and the failure reason is saved on the row.
- **Alternative considered:** fold the lease into the claim query so every relay reclaims stale rows. A separate janitor sweep was chosen instead.

Details settled for Phase 3:

- **Queue names and message.** The default exchange is used, and each queue is named after its channel only (`email`, `sms`, `whatsapp`). The message body holds only the `delivery_id`, as decimal text.
- **`next_attempt_at`.** It is empty by default, which means due now. It is set only when a retry is scheduled (a failed publish or a janitor reclaim) and is never carried in the message.
- **Release delay.** `delay = min(max, initial * multiplier^(retries-1))`, jittered by plus or minus the configured `jitter` fraction, from the relay's `backoff` settings. One helper serves both the failed-publish release and the janitor reclaim.
- **Attempt cap.** A claim that would take `retries` above `relay.max_attempts` ends the row `DEAD` with the reason `MAX_ATTEMPTS_EXCEEDED`. A janitor reclaim counts against the cap, because it follows a claim.
- **Sender claim.** The first delivery of a message inserts the inbox row directly as `PROCESSING` with `claim_count` 1. If the row exists, only a row in `WAITING_RETRY` can be claimed again (conditional on the `claim_count` read). Any other state means a duplicate message, which is acknowledged and dropped. A `claim_count` above `sender.max_claims` ends the inbox row `DEAD` and the delivery `FAILED` with the reason `MAX_CLAIMS_EXCEEDED`.
- **Provider check before Phase 4.** The inbox sweep calls a `ProviderChecker` port. Until providers exist its adapter answers "unknown", so a stuck row goes back to `WAITING_RETRY`.
- **Relay workers and batch confirmation.** A relay runs a configurable number of workers (`relay.workers`, default 8), each with its own claim-and-publish loop and its own broker channel. `SKIP LOCKED` keeps workers, like separate relays, off each other's rows. The default batch is 2,000 rows (`relay.batch_size`). After a batch is published, every confirmed row is marked `PUBLISHED` in one fenced update (an array of delivery IDs and their `retries`), not one update per row. Unconfirmed rows are still released one by one, because that path is the exception.
- **Broker restarts.** The connection component reconnects on its own and re-declares the queues, so neither the relay nor the sender needs a restart after a broker restart.

The relay and the sender are at-least-once. A crash between publishing and marking `PUBLISHED` causes a republish, and a crash between the provider accepting a message and the sender recording it can cause a resend. That is why the inbox makes redelivery harmless and why recovery checks the provider by `delivery_id` first.

The timeout trades against the 5 second target: a relay or sender crash costs at least the timeout for the affected rows. This is accepted because crashes are rare, and the timeout must exceed the worst-case publish or provider call.

## Sender, retries, circuit breaker and failover

The sender consumes one queue per channel, each with its own consumer pool, so a slow SMS provider cannot block email.

**Error classes decide the action.**

| Error class | Action |
| --- | --- |
| Message-level (bad number, rejected content) | Mark the delivery `FAILED` with a reason and stop, no retry |
| Provider-side failure (5xx, refused, throttled) | Retry on the same provider up to its configured maximum, with backoff and jitter. Then switch to the other provider with `retries` reset. If that one is exhausted too, the delivery goes from `PENDING` to `FAILED` at once |
| Timeout (outcome unknown) | Check the provider by `delivery_id`. If it has the message, `ACCEPTED`. If it says no or not found, retry that provider. If the check times out, returns a 5xx, or the provider has no lookup, move to the other provider. On the second provider, a timeout with no usable answer ends the delivery as `UNRESOLVED`, and a 5xx as `FAILED` |
| Deadline reached before a send | `FAILED` with the reason `EXPIRED` |
| Breaker open for the current provider | Switch provider without spending retries |
| Breaker open for every provider | `FAILED` with `NO_PROVIDER_AVAILABLE`, and one critical log line per outage, not per delivery |

**Idempotency.** The sender records the delivery in an inbox row keyed by `delivery_id`, and passes the same ID to the provider as its idempotency or reference key. A webhook carries the same ID back, so it finds its delivery directly. The inbox cannot protect the external call itself: a crash between the provider accepting and our recording it can still cause a resend. Exactly-once does not exist at that boundary, and the document says so.

**Circuit breaker.** One breaker per provider. When it opens, new messages route to another provider for the same channel, skipping doomed retries. Breaker state lives in Redis so all instances agree, and each sender falls back to a local in-memory breaker if Redis is unreachable. The cost of the fallback is slower fleet-wide detection. The shared Redis state is an enhancement: the build for this assignment keeps the breaker local to each sender (see Out of scope and future work).

Refinements: a short local cache of breaker state (around one second) to keep Redis off the hot path, an atomic probe slot for the half-open state, and a few milliseconds of Redis timeout.

**Failover and duplicates.** Failover is safe after a clear failure or an open breaker. After a timeout it can produce a duplicate, because the first provider may have sent the message, so the sender checks that provider first (see the next paragraph) and moves on only when the check cannot settle it. The system aims at at-least-once delivery: exactly-once does not exist at this boundary, so a possible duplicate is accepted over a lost message. The loop runs once per delivery, and after moving to the second provider it never returns to the first.

**After a timeout.** The sender asks the same provider whether it received the earlier request, using the `delivery_id`, if that provider supports a status lookup. If the answer is yes, the delivery is `ACCEPTED`. If the answer is no or not found, the sender retries that provider, up to its maximum. If the check itself times out, returns a 5xx, or cannot be made, the sender moves to the other provider, accepting that a duplicate is possible. The second provider runs the same loop, but it closes it: a timeout with no usable answer ends the delivery as `UNRESOLVED`, and a 5xx ends it as `FAILED`. If both providers end up sending, the first terminal webhook wins and the guarded transitions ignore the later one.

**Retries and provider limit.** Retry timing lives in the inbox, not in the broker. After a transient failure the sender moves its inbox row to `WAITING_RETRY`, adds one to `retries`, records `last_try`, and stores `next_attempt_at` using exponential backoff with jitter from configuration. A sender scan picks up rows whose `next_attempt_at` has passed and sends again. Retries never go back through the outbox or RabbitMQ, survive a broker restart, and need no delay plugin. The cost is extra database writes for each retry and a scan for due rows. A per-provider rate limiter sits in front of the provider call so a fresh spike does not flood it.

**Provider configuration.** Each provider, in any channel, has its own entry in configuration: its channel, whether it is the primary or the fallback, its call timeout, whether it supports a status lookup, its maximum retries, its backoff and jitter, and its rate limit. The timeout is what turns a slow call into an ambiguous outcome, so it stays well below the 5 second target. The janitor's stuck threshold for an inbox row follows the timeout of the provider on that row, plus a margin, and the status lookup uses the same timeout. A timeout that is too short counts healthy calls as ambiguous, and one that is too long holds a worker while the provider struggles, so the load test is where each value is tuned.

Details settled for Phase 4:

- **Phase 4 and Phase 5 boundary.** Phase 4 makes one call to the channel's primary provider per claim. A success moves the delivery to `ACCEPTED` and the inbox row to `DONE`. A message-level error (a provider 4xx) ends the delivery `FAILED` with the reason `PROVIDER_REJECTED`. A provider failure or timeout parks the inbox row in `WAITING_RETRY` with its `failure_class` and nothing picks it up until Phase 5 adds the retry scan, backoff, failover, breaker, expiry check and timeout lookup. This changes none of the decisions above; it only orders their delivery.
- **Webhook authentication differs by provider.** Providers sign webhooks in different ways, such as an HMAC signature header or a shared secret in a header. The API therefore passes each webhook request to a `WebhookAuthenticator` port, resolved by the provider named in the route (`POST /v1/webhooks/{provider}`). Each provider's configuration names its scheme (`hmac_signature` or `shared_secret_header`), the header that carries the signature or secret, and the environment variable that holds the secret (`secret_env`). Secrets never live in the YAML file. A provider with a new scheme needs a new adapter and no change to the handler.
- **Webhook events are stored.** Every webhook that passes authentication is written to a `webhook_events` table: `delivery_id`, provider, event type, the raw payload as jsonb, the receive time, and whether it moved the delivery (`applied`). Duplicate, reordered and ignored events are kept too, so the history of what a provider told us survives the guarded transitions. The event row and the delivery transition are written in one transaction.
- **Failure reasons are a closed enum.** `failure_reason` on a delivery holds only values from the enum in `internal/domain`. Phase 4 adds `PROVIDER_REJECTED`, `PROVIDER_FAILED`, `BOUNCED` and `INVALID_DESTINATION`. A reason string from a webhook that the enum does not know maps to `PROVIDER_FAILED`, and the original text stays in the stored payload. `deliveries` gains a nullable `failure_reason` column for this.
- **Simulated provider modes are global.** One global mode applies to every provider call: `random` draws a scenario per request from configured weights (with an optional seed for reproducible runs), or a single named scenario is pinned. The scenarios are `succeed`, `fail_permanent`, `fail_transient`, `slow`, `timeout` and `late_failure`. The simulator registers the scenario it chose for each `delivery_id`, exposes the registry, and replays the same scenario if the same `delivery_id` is sent again, so a test or a load run can check that the final state of every delivery matches what the simulator decided.
- **Provider calls and classification.** A provider is called at `POST <base>/send` and looked up at `GET <base>/status/<delivery_id>`, where the base is the provider's own `url`, or `<simulation_url>/providers/<name>` in simulation, because one simulator plays every provider. A 2xx means the provider took the message, and a lookup answers 200 (it has it), 404 (it does not), or 501 (no lookup). Only 400, 413, 415 and 422 are message-level failures. Every other failure, including 401, 403, 404, 408, 429 and 5xx, is a provider-side failure, because it describes the provider, its credentials or its load, and failover can still help. A deadline of the caller that expires mid-call is a timeout, since the outcome is unknown. A cancelled caller (shutdown) is not classified, and the row stays `PROCESSING` for the janitor.
- **Recording an outcome.** The inbox write that records an attempt's outcome is fenced by the claim and does not depend on the caller's cancellation, so a shutdown between the provider taking a message and the write cannot cause a resend. A message-level failure ends the inbox row `DEAD` and the delivery `FAILED` with `PROVIDER_REJECTED`. A provider failure or timeout parks the row in `WAITING_RETRY`, adds one to `retries`, and sets `next_attempt_at` from the provider's backoff, whose exponent is the retries already spent, not the claim count that the janitor's reclaims also raise.
- **No send for a finished delivery.** A webhook can finish a delivery before the sender's attempt. The sender then closes the inbox row without a call, because sending would only produce a duplicate.
- **Delivery moves.** A timeout on the last provider ends a `PENDING` delivery `UNRESOLVED` without it ever being `ACCEPTED`, so `PENDING` may move to `UNRESOLVED` as well as to `ACCEPTED`, `DELIVERED` and `FAILED`.
- **Webhooks are bound to a channel.** A provider's webhook applies only to a delivery on the channel that provider serves, so one provider's secret cannot settle another channel's deliveries. An unknown delivery or a channel mismatch answers 404, an event that changes nothing answers 200, and a store failure answers 500 so the provider retries. The body is read, up to 64 KiB, before it is authenticated, because a signature covers its exact bytes. Binding to the exact provider is not done, because after a failover both providers can legitimately send and the first terminal webhook wins. The HMAC covers the body only, so a captured webhook can be replayed. The state guard keeps terminal states terminal, and a provider whose scheme carries a timestamp gets its own authenticator.
- **Simulator scenarios.** A call that times out, and one that ends in a late failure, still leave the provider holding the message, so a status lookup finds it. A slow or timed-out call sends its webhook after the response it delays, so the webhook never races the caller's own timeout handling. The simulator keeps its registry in memory for the life of the process.

## Delivery lifecycle

&#91;embedded content: delivery states · 5 states, forward moves only\]

A delivery only moves forward: `PENDING`, then `ACCEPTED` once the provider has the message, then `DELIVERED`, `FAILED`, or `UNRESOLVED`. It can also go from `PENDING` straight to `FAILED`. The deliveries table holds only these business states, and the operational detail lives in the outbox and the inbox.

- **ACCEPTED is not DELIVERED.** The provider taking a message is not the user receiving it. A late failure webhook moves ACCEPTED to FAILED with a reason, and the service does not retry, since the provider may have partly processed it. Retrying is a business rule left to the producer.
- **Failure comes at once.** A message-level error, a failure on both providers, every breaker open (`NO_PROVIDER_AVAILABLE`), or a deadline reached before a send (`EXPIRED`) moves PENDING straight to FAILED, with the reason saved.
- **UNRESOLVED means we never learned the outcome.** It ends a delivery whose last provider timed out with no usable answer. With the reconciler enhancement, it also ends a delivery that is still ACCEPTED without a receipt when its deadline passes.
- **Guarded transitions.** Webhooks can be duplicated or reordered, and one can arrive before our own ACCEPTED write commits. Each transition is conditional on the current state, terminal states stay terminal, and a webhook carries the `delivery_id` we sent as the reference key, so it finds its delivery directly. A delivery still in PENDING may move straight to DELIVERED or FAILED when an early webhook arrives. If both providers sent the message, the first terminal webhook wins and the later one is ignored.
- **Reconciler (an enhancement).** One of the janitor's sweeps would look at deliveries that sit in ACCEPTED past a threshold, ask the provider for the current state, and apply the same guarded transitions. It is not part of the build for this assignment, and without it a delivery with no receipt simply stays ACCEPTED.
- **Per-channel threshold.** SMS and WhatsApp usually give delivery receipts, while email may not, so the staleness threshold and even whether reconciliation applies would be configured per channel. This belongs to the same enhancement.
- **Expiry.** A producer may send an optional `expires_at` timestamp, such as the start of a match. Without one, the notification type's expiry from domain configuration applies, with the values in config rather than code. The deadline is stamped on each delivery at accept time and checked before every send. A delivery that was never sent by its deadline becomes FAILED with the reason EXPIRED. With the reconciler enhancement, one still ACCEPTED without a receipt at its deadline becomes UNRESOLVED, because we never learned whether it was delivered.

## Rate limiting and backpressure

The limiter protects the system from overload, not from legitimate bursts. Rate limiting is middleware in the Dispatcher, applied as one global limit.

- **Global limit.** One limit on total intake, because 500K per second is the system-wide peak and the database behind it is the shared bottleneck.
- **Counted in deliveries, not requests.** One batch can carry thousands of recipients, so request-based limits are easy to bypass.
- **No per-service quota.** A per-service quota was considered and not chosen. The limit protects shared capacity, so a single global limit is enough. The trade-off is that one producer can use up the whole limit.
- **Burst handling.** Reject with 429 and `Retry-After` when a limit is exceeded. Requests that were already accepted are held in the queue and the outbox.
- **Shared counters.** Counters live in Redis and are shared by every Dispatcher instance, because the database behind them is the shared bottleneck whatever the instance count. If Redis is unreachable, the limiter falls back to a local in-memory limiter on each instance, so limiting becomes approximate instead of absent.
- **Provider limiter.** A separate per-provider token bucket in the sender protects providers from floods.

## Latency target

The 5 second target is a claim about first attempts, not about every delivery. Retries and recovery trade speed for never losing an accepted message.

- **Target.** p99 from request to `ACCEPTED` under 5 seconds for first-attempt deliveries, within the global limit.
- **Why `ACCEPTED`.** Real delivery depends on the provider and the carrier, which the service does not control. The brief describes the Sender as delivering to the external providers, so delivery for this target means provider acceptance.
- **Outside the target.** Retried deliveries (backoff adds delay by design), recovery after a relay or sender crash (up to the janitor timeout), and deliveries that wait for the reconciler in `ACCEPTED` without a receipt. These are measured and reported separately.
- **Main risk on the happy path.** Relay pickup delay, which a short poll interval keeps small, and queue wait, which stays small only while sender concurrency for each channel keeps up with the arrival rate.
- **Why polling, not `LISTEN/NOTIFY`.** Postgres notifications are lost if the relay is disconnected, and a retry that becomes due as time passes fires no signal, so polling would still be needed. A short poll interval is simpler and covers both cases.
- **Above the limit.** Requests are rejected with 429 and `Retry-After`, so the target holds only within the global limit.

**What bounds the first attempt.** Three rates decide whether first attempts finish within 5 seconds: how fast the Dispatcher accepts deliveries, how fast they are placed on the queue and taken off it, and how fast each provider accepts calls. The target holds while the arrival rate stays inside the global limit and the provider limits. If a provider's limit is below the arrival rate, 5 seconds cannot be reached for that channel, by design.

**Queue wait time**, from the moment a delivery is queued to the moment the sender starts on it, is measured next to end-to-end latency in the load test. It shows directly whether the drain rate keeps up with the arrival rate. A first attempt that fails does not count as within 5 seconds. It counts as a retry or a fallback, which is outside the claim.

## Foundations (Phase 1)

Choices made while scaffolding the project. They fill gaps in the sections above and change none of them.

- **Libraries.** `pgx/v5` (`pgxpool`) for Postgres, `rabbitmq/amqp091-go`, `redis/go-redis/v9`, `yaml.v3` for configuration, `golang-migrate` for migrations (the CLI in compose, the library in tests), and `testcontainers-go` for integration tests.
- **Column types.** `bigint generated always as identity` for ids, `text`, `jsonb`, and `timestamptz`. There are no CHECK constraints: the inbox's only constraint is its key, and state values are enforced by typed enums in `internal/domain`.
- **State names.** The log leaves some open. Outbox: `NEW`, `PROCESSING`, `PUBLISHED`, `DEAD`. Inbox: `PROCESSING`, `WAITING_RETRY`, `DONE`, `DEAD`. Deliveries: `PENDING`, `ACCEPTED`, `DELIVERED`, `FAILED`, `UNRESOLVED`.
- **Configuration.** A YAML file (unknown keys are an error) with environment overrides for connection strings and the listen address. Connection strings never live in the file.
- **Providers.** Each channel lists its providers in order. Exactly one has `primary: true` and the others are fallbacks, tried in list order. `max_retries` defaults to 1 for every provider, and stays configurable.
- **Simulation flag.** A global `simulation` setting routes every provider call to the simulated provider service when true, and to the providers' real URLs when false. Phase 1 loads and validates it, and Phase 4 acts on it.
- **Verification percentile.** Simulation and load-test checks gate on p95 from request to `ACCEPTED`. The p99 figure against the 5 second target is still recorded and reported, and the target above is unchanged.
- **Operations.** Migrations run as a one-shot compose service before the entry points start. Logs are JSON on stdout by default, with text available through `LOG_FORMAT=text`.

## Observability, testing, structure and documentation

**Structure.** Hexagonal architecture in Go: domain and ports at the center, with Postgres, RabbitMQ, Redis, HTTP and the simulated providers as adapters. Ports give flexibility at wiring time. Choosing a provider at runtime needs a registry plus the breaker-driven routing described above. Layers stay light to avoid over-abstraction.

**Queue.** RabbitMQ behind `Publisher` and `Consumer` ports. An in-memory queue exists only as a test double, because a restart would lose every queued message and break the durability promise. A single broker will not do 500K messages per second, and the document names partitioned streams such as Kafka, or sharded queues, as the way past that.

**Graceful shutdown.** Mandatory for the API, the relay, and the sender, and built along with them during implementation.

**Testing.** Unit tests, end-to-end tests, and Go benchmarks. Benchmarks measure function speed, so they do not replace a load test, which needs:

- A load test against the full stack with p99 end-to-end latency compared to the 5 second target. Results are recorded after implementation, with the exact setup and numbers.
- Fault injection in the simulated provider (fail, slow, timeout, late webhook), with results per scenario.
- Tests for duplicate batches, janitor recovery, a restarted broker, and concurrent relays and senders against a real Postgres and RabbitMQ, since mocks cannot prove `SKIP LOCKED`, `ON CONFLICT`, or broker behavior.

**Observability.** `slog` with pluggable handlers and stdout as the default. Logs go in the right places: accept results (accepted, duplicates, rejected), publish failures, janitor actions, each attempt with its provider and failure class, provider switches, breaker changes, and every final state, always with `delivery_id` and never with phone numbers or emails. One critical log line is written when the last breaker for a channel opens, and it is the alert. Tracing is described in the document without implementation. A small metrics set (queue wait, queue depth, end-to-end latency, breaker state) backs the load test results.

**PII.** Delivery rows carry contact data. Purging or masking it once a delivery reaches a terminal state is a future enhancement and out of scope here.

**API documentation.** The source of truth is an OpenAPI spec in the repository, drafted with an AI agent and covering idempotency semantics, the accepted, duplicates and rejected response, and the error codes, written once as an enum in the spec and reused in code so the documents and the code cannot drift (for example INVALID\_RECIPIENT and NO\_PROVIDER\_AVAILABLE, and the failure classes message, provider, and timeout).

## Alternative considered: Kafka-first ingestion

The alternative was to let producers write straight to Kafka and rely only on an inbox at the consumer for de-duplication, with no HTTP Dispatcher, outbox, or relay. It was considered and not chosen. The current design (HTTP Dispatcher, transactional outbox, RabbitMQ queue) was kept, and Kafka remains the documented scale-up path.

**What Kafka-first would have gained**

- **Throughput.** Partitions scale ingress well beyond a synchronous Postgres write per delivery and the claim updates behind it.
- **Fewer components.** No outbox table, claim leases, fencing token, or janitor.
- **Natural backpressure.** Consumer lag replaces 429 responses.

**Why it was not chosen**

- **It drops the contract in the brief.** The brief describes an HTTP Dispatcher that validates keys and requests, rate-limits, and accepts fast. Writing straight to Kafka loses synchronous validation and the 202 and 429 behavior, and a bad message is found only later, at the consumer.
- **Producers lose per-recipient feedback.** Duplicates and rejections would be detected asynchronously, so the accepted, duplicates, and rejected response could not exist.
- **Retries are harder.** Kafka has no delayed delivery, so retries need retry topics or a database table anyway, and a slow provider blocks a partition unless topics are split per channel.
- **The outbox problem moves, it does not vanish.** The sender still updates state and publishes follow-ups, and the provider call is non-transactional either way.
- **More to build and explain.** Kafka in docker-compose, partition design, and offset handling.

**Cost of the choice.** A synchronous Postgres write per delivery caps ingress throughput, so the document states that 500K per second cannot be proven on one machine and names the Kafka-based path as the way to scale beyond it. A middle path exists: an HTTP Dispatcher that writes validated requests to Kafka instead of Postgres. It keeps validation and rate limiting but loses synchronous duplicate feedback.

## Alternatives considered for the queue: Asynq and a table queue

Two other designs were considered for the queue and dropped in favor of RabbitMQ with an outbox and an inbox.

**Why they were dropped**

- **Asynq.** It looked simple, but the red flags added up. Its retry scan runs every 5 seconds by default, which works against quick retries. A task's payload cannot change between retries, so the retry metadata has to live in the database anyway. A finished task's ID is free again, so duplicates are not caught for long. It has no bulk enqueue. And Redis becomes a second system of record that can disagree with Postgres.
- **A table as the queue.** It removes the broker, but every delivery then costs an insert, a claim update, and a result update on one hot table. Dead tuples, index churn, and vacuum pressure grow with the spike.

**What RabbitMQ costs**

- **A second durability boundary.** The outbox and the relay keep Postgres and the broker consistent, and the inbox keeps a redelivered message harmless. That is more parts than a single queue library.
- **Pickup delay.** The relay polls the outbox, so a first attempt waits for the next poll. A short poll interval keeps it small.
- **A throughput ceiling.** One RabbitMQ node cannot carry 500K messages per second, and the document names Kafka or sharded queues as the way past that.
- **Retries stay in Postgres.** RabbitMQ has no per-message delay without a plugin, so retry timing lives in the inbox, with a sender scan for due rows.

## Out of scope and future work

Nothing remains open. These seven items were considered and left out of this assignment on purpose. Each is a candidate for a later version.

- **Priority between notification types.** A password-reset SMS should not wait behind a broadcast to millions of users. It would need separate queues or priorities per type.
- **Data lifecycle.** Purging or masking contact data once a delivery reaches a terminal state, and archiving old outbox, inbox, and delivery rows.
- **Partial-failure view for producers.** Deriving one overall status for a notification from its per-channel deliveries, so a producer can see at a glance that the email arrived and the SMS did not.
- **Events to the preference service.** Providers can report opt-outs and invalid contacts. The notification service could publish these as events, through the outbox, for the service that owns preferences. This is documented but not implemented.
- **Re-arming a failed delivery.** A failed delivery is final for its idempotency key, so a resend needs a new key. A way to re-arm one on request would be a later addition.
- **Reconciler for missing receipts.** A janitor sweep that finds deliveries sitting in `ACCEPTED` past a per-channel threshold, asks the provider for the current state, and applies the guarded transitions, ending as `UNRESOLVED` at the deadline if nothing arrives. Per-channel thresholds belong to it, and it would need an index on deliveries.updated\_at for ACCEPTED rows. Without it, a delivery with no receipt stays `ACCEPTED`.
- **Shared breaker state.** Circuit breaker state shared through Redis, so every sender agrees at once. The build keeps a local in-memory breaker per sender, at the cost of slower fleet-wide detection.
