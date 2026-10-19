# Fault report

- Injected `simprovider.providers={sms-a: fail_transient, sms-b: fail_transient, email-a: fail_transient, email-b: fail_transient, whatsapp-a: fail_transient, whatsapp-b: fail_transient}` at t=0 (took 1.8 s to apply).
- Released (`restored the simulator's config`) at +32 s (took 0.7 s).
- Queue depth (deepest channel): 0 before the fault (20 s), peak 0 (no queue built).
- Breakers: most open at once 6; seconds with at least one open 33.
- **Recovered** at +35 s: 2 s after the fault was lifted.

## Trace (changes only; providers listed have a breaker open or half-open)

```
   -4 s  depth    0  
   +2 s  depth    0  email-a sms-a whatsapp-a whatsapp-b
   +3 s  depth    0  email-a email-b sms-a sms-b whatsapp-a whatsapp-b
  +33 s  depth    0  email-b sms-b
```

## Duplicates (simulator ledger, 3 snapshot(s))

- Deliveries the simulator saw: 23047.
- Held by more than one provider: 0.
- Sends beyond the first to a provider that already held the message: 0.
