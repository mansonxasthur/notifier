# after-hold-200

Run `r1792389523`, started 2026-10-19T08:58:43+03:00. Setup is in `params.json`; queries and raw answers in `sql.txt`; every metric sample in `samples.csv`.

## Load

- 200 recipients/s x 3 channels = **600 deliveries/s** target, in requests of 100 recipients (2 requests/s), for 5m0s after a 15s warm-up.
- Requests scheduled 600; answered map[202:600]; transport errors 0; **skipped by the driver 0** (in-flight limit 2000); latest request left 93ms late.
- Deliveries the API accepted: 180000 (of 180000 requested by every scheduled request). HTTP latency of answered requests: p50 41ms, p99 492ms, max 1.018s.

## Request to ACCEPTED (from `accepted_at - created_at`, exact)

| Deliveries | Count | p50 | p95 | p99 | max | p99 vs 5 s |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| first attempt (primary provider, no retry) | 101343 | 0.178 s | 0.876 s | 1.319 s | 2.142 s | within |
| retried or failed over | 26659 | 0.520 s | 1.896 s | 2.886 s | 4.527 s | within |

## Outcome

- Stored 180000 deliveries (180000 expected from accepted requests); 128002 have `accepted_at`.
  - ACCEPTED: 26
  - DELIVERED: 131267
  - FAILED: 48707
- After the last request the system took 700ms to settle (drained).

## Metrics (scraped every 1s, 0 scrapes failed)

- Queue wait, sms (63000 messages including the warm-up; estimated from buckets, so shape only): p50 0.003 s, p99 0.664 s.
- Queue wait, email (63000 messages including the warm-up; estimated from buckets, so shape only): p50 0.003 s, p99 0.380 s.
- Queue wait, whatsapp (63000 messages including the warm-up; estimated from buckets, so shape only): p50 0.003 s, p99 0.563 s.
- Peak queue depth: 7 (notifier_queue_depth{channel=sms}).
- Highest breaker state seen: 2 (notifier_breaker_state{provider=email-a}); 0 closed, 1 half-open, 2 open.
