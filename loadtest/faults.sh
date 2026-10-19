#!/usr/bin/env bash
# One fault-injection run from a clean stack: steady load at half the headline rate, one fault
# injected 30 s into the measured phase, and the results under docs/results/fault-<date>-<run>/.
#
#   loadtest/faults.sh <run>        (or: make fault name=<run>)
#   loadtest/faults.sh list
#
# Runs: provider-down both-down provider-slow provider-timeout both-timeout late-failure redis-down
#       broker-restart relay-killed sender-killed postgres-restart
# Same rig as the headline run (SUT_CPUS=0-1 SIM_CPUS=2-3). See the decision log, "Fault-injection
# runs", and _plans/phase-7-part-d.md for what each run should show.
set -euo pipefail

run=${1:?usage: faults.sh <run> | list}
primaries="{sms-a: %s, email-a: %s, whatsapp-a: %s}"
all="{sms-a: %s, sms-b: %s, email-a: %s, email-b: %s, whatsapp-a: %s, whatsapp-b: %s}"

unset SIM_FAULT_PROVIDERS FAULT_CMD RELEASE_CMD RELEASE_AFTER
case "$run" in
  list) sed -n '/^# Runs:/,/^# Same rig/p' "$0" | sed '$d'; exit 0 ;;
  provider-down)    SIM_FAULT_PROVIDERS=$(printf "$primaries" fail_transient fail_transient fail_transient); RELEASE_AFTER=30 ;;
  both-down)        SIM_FAULT_PROVIDERS=$(printf "$all" fail_transient fail_transient fail_transient fail_transient fail_transient fail_transient); RELEASE_AFTER=30 ;;
  provider-slow)    SIM_FAULT_PROVIDERS=$(printf "$primaries" slow slow slow); RELEASE_AFTER=30 ;;
  provider-timeout) SIM_FAULT_PROVIDERS=$(printf "$primaries" timeout timeout timeout); RELEASE_AFTER=30 ;;
  both-timeout)     SIM_FAULT_PROVIDERS=$(printf "$all" timeout timeout timeout timeout timeout timeout); RELEASE_AFTER=30 ;;
  late-failure)     SIM_FAULT_PROVIDERS=$(printf "$primaries" late_failure late_failure late_failure); RELEASE_AFTER=30 ;;
  redis-down)       FAULT_CMD="stop redis"; RELEASE_CMD="start redis"; RELEASE_AFTER=30 ;;
  broker-restart)   FAULT_CMD="restart rabbitmq" ;;
  relay-killed)     FAULT_CMD="kill relay"; RELEASE_CMD="start relay"; RELEASE_AFTER=30 ;;
  sender-killed)    FAULT_CMD="kill sender"; RELEASE_CMD="start sender"; RELEASE_AFTER=30 ;;
  postgres-restart) FAULT_CMD="restart postgres" ;;
  *) echo "unknown run $run; try: $0 list" >&2; exit 2 ;;
esac
export SIM_FAULT_PROVIDERS="${SIM_FAULT_PROVIDERS:-}" FAULT_CMD="${FAULT_CMD:-}" RELEASE_CMD="${RELEASE_CMD:-}" RELEASE_AFTER="${RELEASE_AFTER:-}"

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
warmup=15
export OUT_PREFIX=fault SUT_CPUS=${SUT_CPUS:-0-1} SIM_CPUS=${SIM_CPUS:-2-3} KEEP_STACK=1
export FAULT_AT=$((warmup + 30))
out="$root/docs/results/fault-$(date +%F)-$run"
compose=(docker compose -f docker-compose.yml -f docker-compose.loadtest.yml)

# 75 recipients/s = 225 deliveries/s, half the headline rate (150 recipients/s), one request of 75 a second.
status=0
loadtest/run.sh "$run" 75 -batch 75 -warmup "${warmup}s" -duration 120s -drain-timeout 180s || status=$?

if [ -d "$out" ] && [ -f "$out/fault.json" ]; then
  # When the deliveries that ended FAILED or UNRESOLVED did so, in 5 s steps from the injection:
  # a failure at the moment of an injection or release may be the harness (a simulator restart),
  # not the fault.
  inject_ms=$(sed -E 's/.*"inject_unix_ms":([0-9]+).*/\1/' "$out/fault.json")
  LOAD_CONFIG="$out/config.yaml" "${compose[@]}" exec -T postgres psql -U notifier -P pager=off -c \
    "SELECT floor((extract(epoch FROM updated_at) * 1000 - ${inject_ms}) / 5000) * 5 AS secs_from_injection, state, failure_reason, count(*)
     FROM deliveries WHERE state IN ('FAILED', 'UNRESOLVED') GROUP BY 1, 2, 3 ORDER BY 1, 2, 3" \
    > "$out/failures-by-time.txt" 2>&1 || true
  # The line the sender logs once per outage when the last breaker of a channel opens.
  LOAD_CONFIG="$out/config.yaml" "${compose[@]}" logs --no-color sender 2>&1 | grep -i 'no provider available' | cut -c1-300 > "$out/critical-lines.txt" || true
  go run ./cmd/faultreport "$out" || echo "fault report failed" >&2
  if [ "$run" = late-failure ]; then
    # A webhook with a bad signature must be refused with 401.
    code=$(curl -s -o /dev/null -w '%{http_code}' -X POST -H 'X-Signature: 00' -H 'Content-Type: application/json' \
      -d '{"delivery_id":1,"status":"failed"}' localhost:8080/v1/webhooks/sms-a || true)
    echo "bad-signature webhook to sms-a: HTTP $code (want 401)" | tee "$out/webhook-probe.txt"
  fi
else
  echo "no fault.json in $out: the injector did not finish" >&2
fi
LOAD_CONFIG="$out/config.yaml" "${compose[@]}" down -v >/dev/null 2>&1 || true
exit "$status"
