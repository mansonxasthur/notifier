# Session: sender batching (issue #21)

## What was built
- A size-or-linger `Coalescer` (`internal/sender/coalesce.go`): callers submit one item and wait for the batch holding it.
- Batch store calls: `InboxStore.ClaimMany`, `InboxStore.Settle` (one pipelined pgx batch, one statement per outcome kind, one commit) and `DeliveryStore.LoadMany`. Every row keeps its `claim_count` fence and guarded transition; a stale claim reports false for its row only.
- `Intake` claims, and `Attempt` loads and settles, through coalescers. `Attempt.Send` is unchanged in shape; the queue handler and consumer stay per message.
- Config `sender.batch_size` (50), `sender.linger` (5ms), and `sender.prefetch` raised from 50 to 200 so a batch can fill.

## Decisions
- Coalescing at the store boundary instead of restructuring the sender around explicit batches; recorded in the decision log.
- `NewAttempt` and `NewIntake` default to a batch of one; `WithBatching` turns batching on, so the existing tests exercise the batch SQL with single items.
- `Sender.Stop` closes the coalescers after in-flight messages finish, so pending outcomes are written.

## Result
- `BenchmarkSenderInbox` (one connection, local container): 1,226 deliveries/s one at a time against 10,323 in batches of 50, for claim + load + complete.
- Load runs (Part C) were left out on purpose; the commits-per-delivery measurement in the load results is not done.
