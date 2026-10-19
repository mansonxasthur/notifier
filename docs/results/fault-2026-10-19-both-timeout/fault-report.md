# Fault report

- Injected `simprovider.providers={sms-a: timeout, sms-b: timeout, email-a: timeout, email-b: timeout, whatsapp-a: timeout, whatsapp-b: timeout}` at t=0 (took 0.6 s to apply).
- Released (`restored the simulator's config`) at +31 s (took 1.3 s).
- Queue depth (deepest channel): 0 before the fault (20 s), peak 0 (no queue built).
- Breakers: most open at once 6; seconds with at least one open 10.
- **Recovered** at +42 s: 10 s after the fault was lifted.

## Trace (changes only; providers listed have a breaker open or half-open)

```
   -4 s  depth    0  
  +32 s  depth    0  email-a email-b sms-a sms-b whatsapp-a whatsapp-b
  +42 s  depth    0  whatsapp-b
```

## Duplicates (simulator ledger, 3 snapshot(s))

- Deliveries the simulator saw: 27828.
- Held by more than one provider: 0.
- Sends beyond the first to a provider that already held the message: 0.
