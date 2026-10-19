# both-down

Run `r1792412391`, started 2026-10-19T15:19:51+03:00. Setup is in `params.json`; queries and raw answers in `sql.txt`; every metric sample in `samples.csv`.

## Load

- 75 recipients/s x 3 channels = **225 deliveries/s** target, in requests of 75 recipients (1 requests/s), for 2m0s after a 15s warm-up.
- Requests scheduled 120; answered map[202:120]; transport errors 0; **skipped by the driver 0** (in-flight limit 2000); latest request left 1ms late.
- Deliveries the API accepted: 27000 (of 27000 requested by every scheduled request). HTTP latency of answered requests: p50 33ms, p99 58ms, max 59ms.

## Request to ACCEPTED (from `accepted_at - created_at`, exact)

| Deliveries | Count | p50 | p95 | p99 | max | p99 vs 5 s |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| first attempt (primary provider, no retry) | 19705 | 0.074 s | 0.125 s | 0.233 s | 0.248 s | within |
| retried or failed over | 198 | 0.236 s | 0.314 s | 0.314 s | 0.314 s | within |

## Outcome

- Stored 27000 deliveries (27000 expected from accepted requests); 19903 have `accepted_at`.
  - DELIVERED: 19903
  - FAILED: 7097
- After the last request the system took 500ms to settle (drained).

## Metrics (scraped every 1s, 0 scrapes failed)

- Queue wait, sms (10125 messages including the warm-up; estimated from buckets, so shape only): p50 0.003 s, p99 0.009 s.
- Queue wait, email (10125 messages including the warm-up; estimated from buckets, so shape only): p50 0.003 s, p99 0.009 s.
- Queue wait, whatsapp (10125 messages including the warm-up; estimated from buckets, so shape only): p50 0.003 s, p99 0.010 s.
- Peak queue depth: 0 (notifier_queue_depth{channel=email}).
- Highest breaker state seen: 2 (notifier_breaker_state{provider=email-a}); 0 closed, 1 half-open, 2 open.
