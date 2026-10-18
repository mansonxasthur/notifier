# Session log: Phase 2 accept path

- **Date:** 2026-10-18
- **Tool:** Claude Code (session `f5159861-0179-42af-ae61-570ec2aded59`)
- **Outcome:** Phase 2 built, issue #3, branch `feat/3-accept-path`, pull request opened, not merged (the engineer reviews and merges).

This is a summary of the session, not a transcript. The raw log is the session's `.jsonl` file under `~/.claude/projects/`.

## Planning

1. Asked to plan Phase 2 of `docs/notification_system_implementation_plan.md`, noting the batch limit had been reduced to 3,000 to leave room for more channels.
2. An exploration of the Phase 1 code found that `config/config.yaml` and the decision log still said 10,000. It also showed that the log leaves open the API key mechanism, the over-limit status code, the reason codes and the inline content shape.
3. Questions put to the engineer, and the answers:
   - API keys: one key, sent in `X-API-KEY` and compared with the configured value. Identifying the calling service is out of scope, so this deviates from the plan's "service identity derived from the key".
   - Port naming: the store method is `Create` returning `CreateResult` (not `Accept`, which describes the Dispatcher's outcome, nor `Save`). It sets the ID on the notification it is given.
4. Defaults I proposed, which the engineer approved with the plan: 413 for a request over the recipient limit, `{subject, body}` for inline email and `{body}` for inline WhatsApp, and a replay answering 202 with every row counted as a duplicate.
5. The plan was approved and saved to `_plans/phase-2-accept-path.md`.

## Implementation

Workflow followed: issue #3, branch `feat/3-accept-path`, decision log written before code, `make check` and a `code-review` pass before each commit.

| Commit | Content |
| --- | --- |
| `ab4439e` | Batch limit 3,000; `accept.chunk_size`, `accept.max_body_bytes` and `API_KEY` in config; Phase 2 decisions in the log |
| `5d8faf3` | Domain types and request validation |
| `761c18f` | `NotificationStore.Create`, the Postgres adapter, the shared `pgtest` helper, the 255-byte `recipient_id` cap |
| `4524374` | `POST /v1/notifications` and its wiring in `cmd/api` |
| `8cb8dd3` | Request types and validation moved to `httpserver/requests` |
| later | Fix for repeated recipients; `slices.Backward` in `stopAll`; Phase 2 boxes ticked; session logs |

Departures from the written plan, all recorded in the decision log:
- One API key and no service identity.
- A valid recipient repeated inside one payload is counted under `duplicates` and is not rejected, so there is no `DUPLICATE_RECIPIENT` reason. Invalid entries are always reported as rejected. The engineer kept the in-memory de-duplication even though the unique constraint also covers it.
- A past `expires_at`, an unconfigured `type`, a missing recipient list and a non-UUID key reject the whole request with 422 and an error code.
- `expires_at` is passed to the insert as an array, so the store does not assume every delivery shares one deadline.
- One bad channel rejects the whole recipient, so a recipient is never half accepted.
- On the engineer's instruction, the JSON request types, the reject reasons, `Policy` and `Validate` live in `internal/httpserver/requests`. The domain keeps `Notification`, `Delivery`, `Content` (duplicated, without tags) and the per-channel rules `Content.ValidFor` and `Channel.ValidContact`. The Postgres adapter owns the stored content shape.

Review findings that were fixed:
- Config and compose: the dev `API_KEY` is set on the `api` service only; the `config.yaml` header says `api` requires it.
- Validation: only the content of channels with an accepted delivery is stored; only the canonical 36-character UUID is accepted; emails need a dotted domain and no quoted local part; a `recipient_id` is capped at 255 bytes and refuses control characters; content text refuses control characters other than line breaks and tabs, because Postgres rejects NUL and the insert would otherwise fail the whole batch with a 500.
- Store: `Create` returns an error for a chunk size below 1 instead of panicking in `slices.Chunk`; the SQL `ORDER BY` uses the `"C"` collation so it agrees with Go's byte-order sort and overlapping requests lock rows in the same order.
- Test helper: `pgtest` finds the repository root by walking up to `go.mod`, so it works under `-trimpath`.
- Handler: `RegisterNotifications` returns an error for an empty API key instead of answering 401 to everyone.
- Repeated recipients, found by the final review: an earlier fix counted a bad repeat of an accepted recipient as a duplicate, which hid the failure, and dropped a valid repeat of a rejected recipient. Invalid entries are now always reported and only valid rows are de-duplicated.
- Validation order: the order that decides a rejection reason is documented in `validateRecipient` and tested.

Review findings that were deferred, or declined with a reason:
- A resend under an existing idempotency key keeps the first request's content and type, and still inserts any new deliveries. This is the logged "first request wins" rule.
- A retry whose explicit `expires_at` has since passed gets 422 `EXPIRED_DEADLINE` and not the duplicate result. The deadline check stays in request validation and the idempotency check stays in the store, as the engineer confirmed.
- A request where every recipient is rejected is not stored, so its idempotency key is not consumed.
- Rate limiting and 429 (Phase 6).
- One Postgres container per test function; a shared container is a later optimisation.
- Per-row allocation when building the insert arrays.
- Validation rules now sit in the HTTP layer, so a future non-HTTP entry point would reuse `requests` or copy it.
- The stored content shape is unexported in the Postgres adapter, so the sender needs an exported decoder in Phase 4.
- `Content` is declared three times (`requests`, `domain`, the adapter); a round-trip test could guard against drift.

## Verification

- `make check` passes: format, vet, lint and `go test -race ./...`, including the Postgres integration tests.
- Integration tests against a real Postgres: a replay creates no rows; the same key with a different payload keeps the first content; a resend adds only the new deliveries, each with an outbox row; 9,000 deliveries run as two statements in one transaction; a failure in a later chunk rolls everything back; eight overlapping concurrent requests neither deadlock nor double insert.
- The acceptance test through the real handler and store: a 1,000-recipient batch with one repeated recipient and one invalid contact returns 998 accepted, 1 duplicate and 1 rejected, and the database holds exactly 998 deliveries and 998 outbox rows. The same request again returns 0 accepted and 999 duplicates and adds no rows.
- `cmd/api` imports nothing from the RabbitMQ adapter.
- Live check on the compose stack: no key gives 401; the first request gives 202 with `accepted: 2` and `u_bad` rejected as `INVALID_CONTACT`; the replay gives 202 with `duplicates: 2`; Postgres holds 2 deliveries, 2 outbox rows and 1 notification.

## Questions from the engineer

**Why is it called `accept`?**
The name came from the phase title and the `accepted` response field. It describes the Dispatcher's outcome, not what a store does. The method became `Create` returning `CreateResult`, with the HTTP layer mapping `Created` to `accepted`.

**Should `recipient_id` be a UUID?**
No. It is the producer's own identifier, free text in the log's examples, and it is only part of the delivery identity. It is capped at 255 bytes.

**What is the endpoint?**
`POST /v1/notifications`. The log says "one endpoint" without a path, so the path is a choice made in this phase.

**Which deadline fails on a late retry?**
The request's optional `expires_at`, once it is in the past. Idempotency stays in the store and deadline validation stays at the request level.

**Why check for repeated recipients in the request when the unique constraint already skips them?**
The plan lists the in-memory pass, and it makes the first occurrence win and avoids reporting one recipient as both accepted and rejected. It is not needed for correctness. The engineer chose to keep it.

## Open items for review

- Confirm the Phase 2 additions to the decision log: reason codes, request-level 422 error codes, the 413 cases, the inline content shapes and the single API key.
- Decide whether a resend under an existing key should be refused when its content differs, instead of keeping the first request's content.
- Next step: a Phase 3 plan (relay, queue and janitor) in `_plans/`, which needs approval before code.
