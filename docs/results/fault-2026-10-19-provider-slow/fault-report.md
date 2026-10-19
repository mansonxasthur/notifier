# Fault report

- Injected `simprovider.providers={sms-a: slow, email-a: slow, whatsapp-a: slow}` at t=0 (took 0.8 s to apply).
- Released (`restored the simulator's config`) at +31 s (took 0.6 s).
- Queue depth (deepest channel): 0 before the fault (20 s), peak 0 (no queue built).
- Breakers: most open at once 0; seconds with at least one open 0.
- **Recovered** at +31 s: 0 s after the fault was lifted.

## Trace (changes only; providers listed have a breaker open or half-open)

```
   -5 s  depth    0  
```

## Duplicates (simulator ledger, 3 snapshot(s))

- Deliveries the simulator saw: 30150.
- Held by more than one provider: 0.
- Sends beyond the first to a provider that already held the message: 0.
