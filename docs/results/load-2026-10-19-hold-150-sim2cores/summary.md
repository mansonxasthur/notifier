# hold-150-sim2cores

Run `r1792381985`, started 2026-10-19T06:53:05+03:00. Setup is in `params.json`; queries and raw answers in `sql.txt`; every metric sample in `samples.csv`.

## Load

- 150 recipients/s x 3 channels = **450 deliveries/s** target, in requests of 100 recipients (1 requests/s), for 5m0s after a 15s warm-up.
- Requests scheduled 451; answered map[202:451]; transport errors 0; **skipped by the driver 0** (in-flight limit 2000); latest request left 12ms late.
- Deliveries the API accepted: 135300 (target 135300). HTTP latency of answered requests: p50 38ms, p99 169ms, max 378ms.

## Request to ACCEPTED (from `accepted_at - created_at`, exact)

| Deliveries | Count | p50 | p95 | p99 | max | p99 vs 5 s |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| first attempt (primary provider, no retry) | 135300 | 0.248 s | 0.513 s | 0.644 s | 1.130 s | within |
| retried or failed over | 0 | – | – | – | – | – |

## Outcome

- Stored 135300 deliveries (135300 expected from accepted requests); 135300 have `accepted_at`.
  - DELIVERED: 135300
- After the last request the system took 700ms to settle (drained).

## Metrics (scraped every 1s, 0 scrapes failed)

- Queue wait, sms (47400 messages including the warm-up; estimated from buckets, so shape only): p50 0.043 s, p99 0.454 s.
- Queue wait, email (47400 messages including the warm-up; estimated from buckets, so shape only): p50 0.028 s, p99 0.438 s.
- Queue wait, whatsapp (47400 messages including the warm-up; estimated from buckets, so shape only): p50 0.044 s, p99 0.459 s.
- Peak queue depth: 54 (notifier_queue_depth{channel=sms}).
- Highest breaker state seen: 0 (notifier_breaker_state{provider=email-a}); 0 closed, 1 half-open, 2 open.
