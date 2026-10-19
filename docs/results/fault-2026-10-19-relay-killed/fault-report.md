# Fault report

- Injected `kill relay` at t=0 (took 0.3 s to apply).
- Released (`start relay`) at +30 s (took 1.6 s).
- Queue depth (deepest channel): 0 before the fault (20 s), peak 5 (at +34 s).
- Breakers: most open at once 0; seconds with at least one open 0.
- **Recovered** at +33 s: 1 s after the fault was lifted.

## Trace (changes only; providers listed have a breaker open or half-open)

```
   -4 s  depth    0  
  +34 s  depth    5  
  +36 s  depth    0  
```

## Duplicates (simulator ledger, 1 snapshot(s))

- Deliveries the simulator saw: 30375.
- Held by more than one provider: 0.
- Sends beyond the first to a provider that already held the message: 0.
