# relay-killed

Run `r1792411867`, started 2026-10-19T15:11:07+03:00. Setup is in `params.json`; queries and raw answers in `sql.txt`; every metric sample in `samples.csv`.

## Load

- 75 recipients/s x 3 channels = **225 deliveries/s** target, in requests of 75 recipients (1 requests/s), for 2m0s after a 15s warm-up.
- Requests scheduled 120; answered map[202:120]; transport errors 0; **skipped by the driver 0** (in-flight limit 2000); latest request left 3ms late.
- Deliveries the API accepted: 27000 (of 27000 requested by every scheduled request). HTTP latency of answered requests: p50 35ms, p99 2.106s, max 2.125s.

## Request to ACCEPTED (from `accepted_at - created_at`, exact)

| Deliveries | Count | p50 | p95 | p99 | max | p99 vs 5 s |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| first attempt (primary provider, no retry) | 27000 | 0.108 s | 26.782 s | 31.312 s | 32.196 s | **over** |
| retried or failed over | 0 | – | – | – | – | – |

## Outcome

- Stored 27000 deliveries (27000 expected from accepted requests); 27000 have `accepted_at`.
  - DELIVERED: 27000
- After the last request the system took 500ms to settle (drained).

## Metrics (scraped every 1s, 31 scrapes failed)

- Queue wait, sms (10125 messages including the warm-up; estimated from buckets, so shape only): p50 0.003 s, p99 2.384 s.
- Queue wait, email (10125 messages including the warm-up; estimated from buckets, so shape only): p50 0.003 s, p99 2.384 s.
- Queue wait, whatsapp (10125 messages including the warm-up; estimated from buckets, so shape only): p50 0.003 s, p99 2.374 s.
- Peak queue depth: 5 (notifier_queue_depth{channel=email}).
- Highest breaker state seen: 0 (notifier_breaker_state{provider=email-a}); 0 closed, 1 half-open, 2 open.
