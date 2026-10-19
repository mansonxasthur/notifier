# Fault report

- Injected `simprovider.providers={sms-a: fail_transient, sms-b: fail_transient, email-a: fail_transient, email-b: fail_transient, whatsapp-a: fail_transient, whatsapp-b: fail_transient}` at t=0 (took 0.7 s to apply).
- Released (`restored the simulator's config`) at +31 s (took 0.5 s).
- Queue depth (deepest channel): 0 before the fault (20 s), peak 0 (no queue built).
- Breakers: most open at once 6; seconds with at least one open 32.
- **Recovered** at +32 s: 1 s after the fault was lifted.

## Trace (changes only; providers listed have a breaker open or half-open)

```
   -5 s  depth    0  
   +0 s  depth    0  email-a email-b sms-a sms-b whatsapp-a whatsapp-b
  +31 s  depth    0  email-a sms-a whatsapp-a
  +32 s  depth    0  
```

## Duplicates (simulator ledger, 3 snapshot(s))

- Deliveries the simulator saw: 23290.
- Held by more than one provider: 0.
- Sends beyond the first to a provider that already held the message: 0.
