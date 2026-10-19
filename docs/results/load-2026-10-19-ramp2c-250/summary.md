# ramp2c-250

Run `r1792382337`, started 2026-10-19T06:58:57+03:00. Setup is in `params.json`; queries and raw answers in `sql.txt`; every metric sample in `samples.csv`.

## Load

- 250 recipients/s x 3 channels = **750 deliveries/s** target, in requests of 100 recipients (2 requests/s), for 1m0s after a 10s warm-up.
- Requests scheduled 150; answered map[202:150]; transport errors 0; **skipped by the driver 0** (in-flight limit 2000); latest request left 2ms late.
- Deliveries the API accepted: 45000 (target 45000). HTTP latency of answered requests: p50 184ms, p99 866ms, max 1.001s.

## Request to ACCEPTED (from `accepted_at - created_at`, exact)

| Deliveries | Count | p50 | p95 | p99 | max | p99 vs 5 s |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| first attempt (primary provider, no retry) | 44940 | 8.490 s | 17.904 s | 18.762 s | 19.027 s | **over** |
| retried or failed over | 0 | – | – | – | – | – |

## Outcome

- Stored 45000 deliveries (45000 expected from accepted requests); 44940 have `accepted_at`.
  - DELIVERED: 45000
- After the last request the system took 19.4s to settle (drained).

## Metrics (scraped every 1s, 0 scrapes failed)

- Queue wait, sms (17500 messages including the warm-up; estimated from buckets, so shape only): p50 5.813 s, p99 29.442 s.
- Queue wait, email (17500 messages including the warm-up; estimated from buckets, so shape only): p50 5.923 s, p99 29.449 s.
- Queue wait, whatsapp (17500 messages including the warm-up; estimated from buckets, so shape only): p50 5.765 s, p99 29.435 s.
- Peak queue depth: 3658 (notifier_queue_depth{channel=email}).
- Highest breaker state seen: 0 (notifier_breaker_state{provider=email-a}); 0 closed, 1 half-open, 2 open.
