# Session: queue batching (#22)

Plan: `_plans/queue-batching.md`. Decision log: "Queue batching".

## Built
- Message body is comma-joined decimal ids (`rabbitmq/message.go`); one id equals the old body.
- `Publisher` splits ids into messages of `relay.message_size` (default 100) and maps each message's confirm to its ids.
- `ports.Handler` takes `[]int64`; `ports.BatchError` names the failed ids; the consumer republishes those (`WithRepublisher`) and acks the original, falling back to a whole-message requeue.
- `Intake.Handler` runs a message's ids together into the claim coalescer; a missing delivery is logged and dropped.
- `sender.prefetch` default lowered to 4 (messages, not ids).
- No relay linger: the claim already fills batches (recorded in the decision log).

## Tests
Real broker: batch round trip, mixed formats, partial failure republish, stop mid-batch, broker restart with `message_size` 7. Relay: nacked message releases only its rows. Sender: poison id, partial failure and redelivery.

## Not done
Load comparison against `hold-150-sim2cores` (needs `loadtest/run.sh`, which resets the stack).
