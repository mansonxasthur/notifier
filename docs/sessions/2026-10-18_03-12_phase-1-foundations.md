# Session log: Phase 1 foundations

- **Date:** 2026-10-18
- **Tool:** Claude Code (session `2f87e5c2-e27c-4bbf-813e-53e1e8ff5202`)
- **Outcome:** Phase 1 built, issue #1, pull request #2, CI green, not merged (the engineer reviews and merges).

This is a summary of the session, not a transcript. The raw log is the session's `.jsonl` file under `~/.claude/projects/`.

## Planning

1. Asked to plan Phase 1 of `docs/notification_system_implementation_plan.md`. Read the plan, the decision log, the Makefile, `go.mod` and the `go-architecutre` skill. The repo held only docs and empty directories.
2. The skill assumes a multi-module monolith, Tilt and Kubernetes. The project is one bounded context on docker-compose, so the plan uses a flat hexagonal layout instead.
3. Questions put to the engineer, and the answers:
   - Config: YAML file plus environment overrides.
   - Simulated provider: a minimal stub service in Phase 1.
   - CI: GitHub Actions.
4. The first plan was rejected with changes:
   - Mark one provider as primary; the rest are fallbacks.
   - `max_retries` is 1 for each provider.
   - Add a `simulation` flag, and test simulations against p95.
5. Follow-up questions and answers:
   - `simulation` is a global flag that routes provider calls to the simulator.
   - p95 is the gate for simulation and load-test checks; p99 against the 5 second target is still recorded.
6. The revised plan was approved and saved to `_plans/phase-1-foundations.md`.

## Implementation

Workflow followed: issue #1, branch `feat/1-foundations`, decision log written before code, `make check` and a `code-review` pass before each commit.

| Commit | Content |
| --- | --- |
| `d490e72` | Plan and decision-log "Foundations" section |
| `a7f6457` | Tooling, domain enums, logging, config |
| `dc75b1f` | Lifecycle helper, adapters, entry points |
| `cd4a758` | Migrations and testcontainers schema test |
| later | Dockerfiles and compose; CI and docs update |

Review findings that were fixed:
- Config: the primary provider must be first; multi-document YAML is rejected; NaN values are rejected; `Require` takes named dependencies.
- Lifecycle: a runtime failure of a component (such as the HTTP listener dying) now ends the process; `Retry` runs at least once.
- Adapters: nil guards on `Ping`; the AMQP dial is cancellable; closing an already-closed connection is not a shutdown error.
- HTTP: `/readyz` logs which dependency failed.
- Compose: a fixed RabbitMQ hostname so its volume survives recreates; the dev override no longer repeats the build and command per service.

Review findings that were deferred:
- Janitor and timeout cross-validation.
- Worst-case latency maths including retries and failover.
- RabbitMQ reconnect (Phase 3).
- `FailureClass` from the OpenAPI enum (Phase 8).

## Verification

- `make check` passes locally.
- Production compose stack: all services healthy, `/readyz` and `/healthz` return 200, schema matches the migrations.
- `docker compose stop`: `api`, `relay`, `sender` and `simprovider` exit 0, with components stopped in reverse order.
- Unreachable Postgres: `api` retries about 9 seconds, then exits 1.
- Dev stack (air) starts and stops with exit 0.
- GitHub Actions `check` job passed on PR #2.

## Questions from the engineer

**Why `tcp` instead of `net/http` for the server?**
The server is `net/http`. Only the listener is created with `net.Listen("tcp", ...)`, which is the required network type. Binding in `Start` and serving in a goroutine makes a bad port fail startup with exit 1, and `Start` still returns as the lifecycle requires. `srv.Shutdown` still drains requests. The alternative, `go srv.ListenAndServe()`, would only report a port conflict after startup. The engineer accepted the current approach.

## Open items for review

- Confirm the state names recorded in the decision log: outbox `DEAD`; inbox `DONE` and `DEAD`.
- Next step: a Phase 2 plan (accept path) in `_plans/`, which needs approval before code.
