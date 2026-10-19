# Fault report

- Injected `restart postgres` at t=0 (took 1.0 s to apply).
- Queue depth (deepest channel): 0 before the fault (20 s), peak 1 (at +58 s).
- Breakers: most open at once 0; seconds with at least one open 0.
- **Recovered** at +2 s: 1 s after the fault was lifted.

## Trace (changes only; providers listed have a breaker open or half-open)

```
   -4 s  depth    0  
  +58 s  depth    1  
  +60 s  depth    0  
```

## Duplicates (simulator ledger, 1 snapshot(s))

- Deliveries the simulator saw: 30375.
- Held by more than one provider: 0.
- Sends beyond the first to a provider that already held the message: 0.
