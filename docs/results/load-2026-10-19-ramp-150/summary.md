# ramp-150

Run `r1792379169`, started 2026-10-19T06:06:09+03:00. Setup is in `params.json`; queries and raw answers in `sql.txt`; every metric sample in `samples.csv`.

## Load

- 150 recipients/s x 3 channels = **450 deliveries/s** target, in requests of 100 recipients (1 requests/s), for 30s after a 10s warm-up.
- Requests scheduled 46; answered map[202:46]; transport errors 0; **skipped by the driver 0** (in-flight limit 2000); latest request left 2ms late.
- Deliveries the API accepted: 13800 (target 13800). HTTP latency of answered requests: p50 40ms, p99 66ms, max 66ms.

## Request to ACCEPTED (from `accepted_at - created_at`, exact)

| Deliveries | Count | p50 | p95 | p99 | max | p99 vs 5 s |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| first attempt (primary provider, no retry) | 13658 | 0.181 s | 0.835 s | 1.466 s | 2.537 s | within |
| retried or failed over | 121 | 2.566 s | 3.109 s | 3.110 s | 3.113 s | within |

## Outcome

- Stored 13800 deliveries (13800 expected from accepted requests); 13779 have `accepted_at`.
  - DELIVERED: 13800
- After the last request the system took 3.6s to settle (drained).

## Metrics (scraped every 1s, 0 scrapes failed)

- Queue wait, sms (6200 messages including the warm-up; estimated from buckets, so shape only): p50 0.036 s, p99 1.190 s.
- Queue wait, email (6200 messages including the warm-up; estimated from buckets, so shape only): p50 0.029 s, p99 0.833 s.
- Queue wait, whatsapp (6200 messages including the warm-up; estimated from buckets, so shape only): p50 0.034 s, p99 1.337 s.
- Peak queue depth: 130 (notifier_queue_depth{channel=whatsapp}).
- Highest breaker state seen: 2 (notifier_breaker_state{provider=sms-a}); 0 closed, 1 half-open, 2 open.
