#!/usr/bin/env bash
# Stepped ramp: each step from a clean stack, warm-up then measured load, stopping at the
# first step that fails: first-attempt p99 over 5 s, the driver skipping requests, transport
# errors, or deliveries still PENDING after the drain. Steps are recipients per second; each
# recipient has three channels. (Retried deliveries are reported but do not fail a step: they
# are outside the 5 s claim by design.)
#   PREFIX=ramp names the result directories load-<date>-<PREFIX>-<rate>.
#   STEPS="100 200" WARMUP=10s DURATION=30s loadtest/ramp.sh
set -uo pipefail
cd "$(dirname "$0")/.."
steps=${STEPS:-"100 200 400 800 1600 3200"}
for rate in $steps; do
  loadtest/run.sh "${PREFIX:-ramp}-$rate" "$rate" -batch 100 -warmup "${WARMUP:-10s}" -duration "${DURATION:-30s}" -drain-timeout "${DRAIN:-60s}" || { echo "step $rate: driver failed"; exit 1; }
  s="docs/results/load-$(date +%F)-${PREFIX:-ramp}-$rate/summary.md"
  # The provider always succeeds in a ramp, so a FAILED or UNRESOLVED delivery or an opened
  # breaker means the stack or the rig was overloaded, even when the p99 looks fine.
  [ -f "$s" ] || { echo "step $rate: no summary at $s"; exit 1; }
  if grep -E 'first attempt' "$s" | grep -q '\*\*over\*\*' \
     || grep -qE 'skipped by the driver \*\*[1-9]|still PENDING|transport errors [1-9]|^  - (FAILED|UNRESOLVED):|breaker state seen: [12]' "$s"; then
    echo "step $rate failed: see $s"; exit 0
  fi
  echo "step $rate passed"
done
