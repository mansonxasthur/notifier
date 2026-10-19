# sender-killed

Run `r1792412016`, started 2026-10-19T15:13:36+03:00. Setup is in `params.json`; queries and raw answers in `sql.txt`; every metric sample in `samples.csv`.

## Load

- 75 recipients/s x 3 channels = **225 deliveries/s** target, in requests of 75 recipients (1 requests/s), for 2m0s after a 15s warm-up.
- Requests scheduled 120; answered map[202:120]; transport errors 0; **skipped by the driver 0** (in-flight limit 2000); latest request left 2ms late.
- Deliveries the API accepted: 27000 (of 27000 requested by every scheduled request). HTTP latency of answered requests: p50 35ms, p99 2.341s, max 2.513s.

## Request to ACCEPTED (from `accepted_at - created_at`, exact)

| Deliveries | Count | p50 | p95 | p99 | max | p99 vs 5 s |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| first attempt (primary provider, no retry) | 27000 | 0.111 s | 26.598 s | 31.379 s | 32.426 s | **over** |
| retried or failed over | 0 | – | – | – | – | – |

## Outcome

- Stored 27000 deliveries (27000 expected from accepted requests); 27000 have `accepted_at`.
  - DELIVERED: 27000
- After the last request the system took 500ms to settle (drained).

## Metrics (scraped every 1s, 31 scrapes failed)

- Queue wait, sms (6675 messages including the warm-up; estimated from buckets, so shape only): p50 0.004 s, p99 above the last bucket (30 s).
- Queue wait, email (6675 messages including the warm-up; estimated from buckets, so shape only): p50 0.004 s, p99 above the last bucket (30 s).
- Queue wait, whatsapp (6675 messages including the warm-up; estimated from buckets, so shape only): p50 0.004 s, p99 above the last bucket (30 s).
- Peak queue depth: 69 (notifier_queue_depth{channel=whatsapp}).
- Highest breaker state seen: 0 (notifier_breaker_state{provider=email-a}); 0 closed, 1 half-open, 2 open.
