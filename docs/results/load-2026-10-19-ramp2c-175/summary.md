# ramp2c-175

Run `r1792383204`, started 2026-10-19T07:13:24+03:00. Setup is in `params.json`; queries and raw answers in `sql.txt`; every metric sample in `samples.csv`.

## Load

- 175 recipients/s x 3 channels = **525 deliveries/s** target, in requests of 100 recipients (1 requests/s), for 1m0s after a 10s warm-up.
- Requests scheduled 106; answered map[202:106]; transport errors 0; **skipped by the driver 0** (in-flight limit 2000); latest request left 3ms late.
- Deliveries the API accepted: 31800 (target 31800). HTTP latency of answered requests: p50 47ms, p99 331ms, max 491ms.

## Request to ACCEPTED (from `accepted_at - created_at`, exact)

| Deliveries | Count | p50 | p95 | p99 | max | p99 vs 5 s |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| first attempt (primary provider, no retry) | 31800 | 0.318 s | 0.870 s | 1.006 s | 1.326 s | within |
| retried or failed over | 0 | – | – | – | – | – |

## Outcome

- Stored 31800 deliveries (31800 expected from accepted requests); 31800 have `accepted_at`.
  - DELIVERED: 31800
- After the last request the system took 1.6s to settle (drained).

## Metrics (scraped every 1s, 0 scrapes failed)

- Queue wait, sms (12400 messages including the warm-up; estimated from buckets, so shape only): p50 0.073 s, p99 0.832 s.
- Queue wait, email (12400 messages including the warm-up; estimated from buckets, so shape only): p50 0.063 s, p99 0.776 s.
- Queue wait, whatsapp (12400 messages including the warm-up; estimated from buckets, so shape only): p50 0.080 s, p99 0.856 s.
- Peak queue depth: 150 (notifier_queue_depth{channel=email}).
- Highest breaker state seen: 0 (notifier_breaker_state{provider=email-a}); 0 closed, 1 half-open, 2 open.
