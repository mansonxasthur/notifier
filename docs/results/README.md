# Recorded results

Every file here is raw output with the setup that produced it beside it. Nothing in the design document should cite a number that is not in this directory.

| File | What | Measures |
| --- | --- | --- |
| `bench-<date>.txt` | `make bench` | Function speed on one machine (validation, batch insert, outbox claim and publish, bucket, Redis limiter). Not capacity: it says how fast one call is, not how many the system carries. |

Load-test and fault-injection results are added by the later parts of Phase 7.
