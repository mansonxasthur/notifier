# Fault report

- Injected `kill sender` at t=0 (took -0.4 s to apply).
- Released (`start sender`) at +30 s (took 1.7 s).
- Queue depth (deepest channel): 0 before the fault (20 s), peak 69 (at +32 s).
- Breakers: most open at once 0; seconds with at least one open 0.
- **Recovered** at +35 s: 4 s after the fault was lifted.

## Trace (changes only; providers listed have a breaker open or half-open)

```
   -5 s  depth    0  
   +1 s  depth    3  
   +3 s  depth    4  
   +5 s  depth    7  
   +7 s  depth    9  
   +9 s  depth   11  
  +11 s  depth   16  
  +13 s  depth   25  
  +15 s  depth   27  
  +17 s  depth   33  
  +19 s  depth   38  
  +21 s  depth   47  
  +23 s  depth   48  
  +25 s  depth   55  
  +27 s  depth   57  
  +29 s  depth   61  
  +32 s  depth   69  
  +33 s  depth   51  
  +35 s  depth    0  
```

## Duplicates (simulator ledger, 1 snapshot(s))

- Deliveries the simulator saw: 30375.
- Held by more than one provider: 0.
- Sends beyond the first to a provider that already held the message: 0.
