# headline-100

Run `r1792381031`, started 2026-10-19T06:37:11+03:00. Setup is in `params.json`; queries and raw answers in `sql.txt`; every metric sample in `samples.csv`.

## Load

- 100 recipients/s x 3 channels = **300 deliveries/s** target, in requests of 100 recipients (1 requests/s), for 5m0s after a 15s warm-up.
- Requests scheduled 300; answered map[202:300]; transport errors 0; **skipped by the driver 0** (in-flight limit 2000); latest request left 3ms late.
- Deliveries the API accepted: 90000 (target 90000). HTTP latency of answered requests: p50 40ms, p99 81ms, max 93ms.

## Request to ACCEPTED (from `accepted_at - created_at`, exact)

| Deliveries | Count | p50 | p95 | p99 | max | p99 vs 5 s |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| first attempt (primary provider, no retry) | 90000 | 0.175 s | 0.351 s | 0.546 s | 1.111 s | within |
| retried or failed over | 0 | – | – | – | – | – |

## Outcome

- Stored 90000 deliveries (90000 expected from accepted requests); 90000 have `accepted_at`.
  - DELIVERED: 90000
- After the last request the system took 600ms to settle (drained).

## Metrics (scraped every 1s, 0 scrapes failed)

- Queue wait, sms (31500 messages including the warm-up; estimated from buckets, so shape only): p50 0.024 s, p99 0.368 s.
- Queue wait, email (31500 messages including the warm-up; estimated from buckets, so shape only): p50 0.022 s, p99 0.369 s.
- Queue wait, whatsapp (31500 messages including the warm-up; estimated from buckets, so shape only): p50 0.024 s, p99 0.387 s.
- Peak queue depth: 50 (notifier_queue_depth{channel=sms}).
- Highest breaker state seen: 0 (notifier_breaker_state{provider=email-a}); 0 closed, 1 half-open, 2 open.
