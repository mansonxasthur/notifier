# headline-150

Run `r1792379588`, started 2026-10-19T06:13:08+03:00. Setup is in `params.json`; queries and raw answers in `sql.txt`; every metric sample in `samples.csv`.

## Load

- 150 recipients/s x 3 channels = **450 deliveries/s** target, in requests of 100 recipients (1 requests/s), for 5m0s after a 15s warm-up.
- Requests scheduled 451; answered map[202:451]; transport errors 0; **skipped by the driver 0** (in-flight limit 2000); latest request left 80ms late.
- Deliveries the API accepted: 135300 (target 135300). HTTP latency of answered requests: p50 40ms, p99 211ms, max 267ms.

## Request to ACCEPTED (from `accepted_at - created_at`, exact)

| Deliveries | Count | p50 | p95 | p99 | max | p99 vs 5 s |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| first attempt (primary provider, no retry) | 114261 | 0.256 s | 5.184 s | 6.142 s | 7.039 s | **over** |
| retried or failed over | 7691 | 1.230 s | 4.858 s | 5.300 s | 6.024 s | **over** |

## Outcome

- Stored 135300 deliveries (135300 expected from accepted requests); 121952 have `accepted_at`.
  - DELIVERED: 123865
  - FAILED: 11435
- After the last request the system took 2.2s to settle (drained).

## Metrics (scraped every 1s, 0 scrapes failed)

- Queue wait, sms (47400 messages including the warm-up; estimated from buckets, so shape only): p50 0.080 s, p99 8.395 s.
- Queue wait, email (47400 messages including the warm-up; estimated from buckets, so shape only): p50 0.071 s, p99 8.396 s.
- Queue wait, whatsapp (47400 messages including the warm-up; estimated from buckets, so shape only): p50 0.086 s, p99 8.538 s.
- Peak queue depth: 875 (notifier_queue_depth{channel=whatsapp}).
- Highest breaker state seen: 2 (notifier_breaker_state{provider=email-a}); 0 closed, 1 half-open, 2 open.
