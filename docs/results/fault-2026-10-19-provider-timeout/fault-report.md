# Fault report

- Injected `simprovider.providers={sms-a: timeout, email-a: timeout, whatsapp-a: timeout}` at t=0 (took 1.7 s to apply).
- Released (`restored the simulator's config`) at +32 s (took 0.6 s).
- Queue depth (deepest channel): 0 before the fault (20 s), peak 0 (no queue built).
- Breakers: most open at once 6; seconds with at least one open 20.
- **Recovered** at +43 s: 11 s after the fault was lifted.

## Trace (changes only; providers listed have a breaker open or half-open)

```
   -5 s  depth    0  
   +1 s  depth    0  email-a email-b sms-a sms-b whatsapp-a whatsapp-b
  +12 s  depth    0  email-a sms-a whatsapp-a
  +44 s  depth    0  
```

## Duplicates (simulator ledger, 3 snapshot(s))

- Deliveries the simulator saw: 27528.
- Held by more than one provider: 0.
- Sends beyond the first to a provider that already held the message: 0.
