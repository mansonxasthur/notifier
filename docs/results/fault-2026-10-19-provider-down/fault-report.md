# Fault report

- Injected `simprovider.providers={sms-a: fail_transient, email-a: fail_transient, whatsapp-a: fail_transient}` at t=0 (took 0.6 s to apply).
- Released (`restored the simulator's config`) at +31 s (took 0.6 s).
- Queue depth (deepest channel): 0 before the fault (20 s), peak 0 (no queue built).
- Breakers: most open at once 5; seconds with at least one open 39.
- **Recovered** at +42 s: 10 s after the fault was lifted.

## Trace (changes only; providers listed have a breaker open or half-open)

```
   -4 s  depth    0  
   +2 s  depth    0  email-a sms-a whatsapp-a
  +32 s  depth    0  email-a email-b sms-a whatsapp-a whatsapp-b
  +34 s  depth    0  email-b whatsapp-b
```

## Duplicates (simulator ledger, 3 snapshot(s))

- Deliveries the simulator saw: 29753.
- Held by more than one provider: 0.
- Sends beyond the first to a provider that already held the message: 0.
