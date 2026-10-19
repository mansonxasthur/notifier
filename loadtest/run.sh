#!/usr/bin/env bash
# One load run from a clean stack, with its results under docs/results/.
#
#   loadtest/run.sh <name> <recipients-per-second> [driver flags...]
#
# Environment (defaults in brackets):
#   SIM_MODE [succeed]       simprovider.mode: succeed, random, slow, ...
#   SIM_WEIGHTS [shipped]    the simprovider.weights flow map, used when SIM_MODE=random
#   GLOBAL_RATE [500000]     limits.global.rate, deliveries per second
#   GLOBAL_BURST [1000000]   limits.global.burst
#   DB_MAX_CONNS [20]        database.max_conns of every service
#   SUT_CPUS [0-2], SIM_CPUS [3]  cores for the system under test, and for the simulator and driver
#   MESSAGE_SIZE [shipped]   relay.message_size, delivery ids per queue message
#   PREFETCH [shipped]       sender.prefetch, unacknowledged messages per queue
#   PG_SYNC_COMMIT [on]      off only to diagnose; see docker-compose.loadtest.yml
#   KEEP_STACK [0]           1 leaves the stack up afterwards
# The config is the shipped one with those lines changed; the result is saved as config.yaml
# in the results directory, so the run can be repeated exactly.
set -euo pipefail

name=${1:?usage: run.sh <name> <recipients-per-second> [driver flags]}
rate=${2:?usage: run.sh <name> <recipients-per-second> [driver flags]}
shift 2

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
out="$root/docs/results/load-$(date +%F)-$name"
mkdir -p "$out"

sim_mode=${SIM_MODE:-succeed}
grate=${GLOBAL_RATE:-500000}
gburst=${GLOBAL_BURST:-1000000}

sed -e "s/^  mode: random$/  mode: ${sim_mode}/" \
    -e "s/^  max_conns: [0-9]*$/  max_conns: ${DB_MAX_CONNS:-20}/" \
    -e "s/global: {rate: 500000, burst: 1000000}/global: {rate: ${grate}, burst: ${gburst}}/" \
    config/config.yaml > "$out/config.yaml"
# A setting that did not change the file would record the run under the wrong name.
grep -q "^  mode: ${sim_mode}$" "$out/config.yaml" || { echo "simprovider.mode was not set to ${sim_mode}: is config/config.yaml formatted as run.sh expects?" >&2; exit 1; }
grep -q "global: {rate: ${grate}, burst: ${gburst}}" "$out/config.yaml" || { echo "limits.global was not set: is config/config.yaml formatted as run.sh expects?" >&2; exit 1; }
grep -q "^  max_conns: ${DB_MAX_CONNS:-20}$" "$out/config.yaml" || { echo "database.max_conns was not set: is config/config.yaml formatted as run.sh expects?" >&2; exit 1; }
if [ -n "${MESSAGE_SIZE:-}" ]; then
  sed -i -e "s/^  message_size: [0-9]*$/  message_size: ${MESSAGE_SIZE}/" "$out/config.yaml"
  grep -q "^  message_size: ${MESSAGE_SIZE}$" "$out/config.yaml" || { echo "relay.message_size was not set" >&2; exit 1; }
fi
if [ -n "${PREFETCH:-}" ]; then
  sed -i -e "s/^  prefetch: [0-9]*$/  prefetch: ${PREFETCH}/" "$out/config.yaml"
  grep -q "^  prefetch: ${PREFETCH}$" "$out/config.yaml" || { echo "sender.prefetch was not set" >&2; exit 1; }
fi
if [ -n "${SIM_WEIGHTS:-}" ]; then
  sed -i -e "s/^  weights: {.*}$/  weights: ${SIM_WEIGHTS}/" "$out/config.yaml"
fi
diff <(cat config/config.yaml) "$out/config.yaml" > "$out/config.diff" || true

compose=(docker compose -f docker-compose.yml -f docker-compose.loadtest.yml)
export LOAD_CONFIG="$out/config.yaml"
export PG_SYNC_COMMIT=${PG_SYNC_COMMIT:-on}

log() { echo "[$(date +%T)] >> $*"; }

# fail records why a run stopped, so a run that does not finish still leaves something to read.
fail() {
  echo "$*" | tee "$out/FAILED.txt" >&2
  {
    echo; echo "--- docker compose ps"; "${compose[@]}" ps -a 2>&1 | cut -c1-200
    for s in api relay sender simprovider migrate; do
      echo; echo "--- last log lines of $s"; "${compose[@]}" logs --no-color --tail 15 "$s" 2>&1 | cut -c1-250
    done
  } >> "$out/FAILED.txt" 2>&1
  [ "${KEEP_STACK:-0}" = 1 ] || "${compose[@]}" down -v >/dev/null 2>&1 || true
  exit 1
}

# ready waits up to $2 seconds for the command $3... to succeed.
ready() {
  local what=$1 limit=$2; shift 2
  local start=$SECONDS
  until "$@" >/dev/null 2>&1; do
    [ $((SECONDS - start)) -lt "$limit" ] || fail "timed out after ${limit}s waiting for $what"
    sleep 1
  done
}

log "resetting the stack"
"${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
log "building images (the first build downloads Go modules and takes minutes; log: build.log)"
"${compose[@]}" build > "$out/build.log" 2>&1 || { tail -20 "$out/build.log"; fail "image build failed"; }
log "starting the stack"
"${compose[@]}" up -d > "$out/up.log" 2>&1 || { tail -20 "$out/up.log"; fail "docker compose up failed"; }
ready "the api (localhost:8080/readyz)" 180 curl -sf localhost:8080/readyz
ready "the relay (localhost:8082/healthz)" 60 curl -sf localhost:8082/healthz
ready "the sender (localhost:8083/healthz)" 60 curl -sf localhost:8083/healthz
ready "the simulated provider (localhost:8081/scenarios)" 60 curl -sf localhost:8081/scenarios
log "stack is up"

{
  echo "date: $(date -Is)"
  echo "kernel: $(uname -srm)"
  echo "cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | xargs) x $(nproc)"
  echo "memory: $(grep MemTotal /proc/meminfo | xargs)"
  echo "docker: $(docker version --format '{{.Server.Version}}')"
  "${compose[@]}" images --format '{{.Service}} {{.Repository}}:{{.Tag}} {{.ID}}' 2>/dev/null || true
} > "$out/env.txt"

log "sampling container CPU and memory"
(
  echo "time,container,cpu_pct,mem"
  while true; do
    docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' | sed "s/^/$(date +%s),/"
  done
) > "$out/docker-stats.csv" 2>/dev/null &
sampler=$!
trap 'kill $sampler 2>/dev/null || true; pkill -P $sampler 2>/dev/null || true' EXIT

log "building the driver"
go build -o "$out/.loadtest" ./cmd/loadtest || fail "driver build failed"
log "driving ${rate} recipients/s; the driver prints its phases, and the summary follows when it ends"
DATABASE_URL='postgres://notifier:notifier@localhost:5432/notifier?sslmode=disable' \
  taskset -c "${SIM_CPUS:-3}" "$out/.loadtest" -rate "$rate" -out "$out" -label "$name" \
  -target api=http://localhost:8080/metrics -target relay=http://localhost:8082/metrics -target sender=http://localhost:8083/metrics \
  "$@" | tee "$out/driver.log" | tail -40
driver_status=${PIPESTATUS[0]}
rm -f "$out/.loadtest"
[ "$driver_status" -eq 0 ] || fail "the driver exited with status $driver_status (see driver.log)"

kill $sampler 2>/dev/null || true
# What the database was doing: checkpoints, and dead tuples and vacuums per table.
timeout 20 "${compose[@]}" exec -T postgres psql -U notifier -P pager=off -c \
  "SELECT * FROM pg_stat_checkpointer" -c \
  "SELECT relname, n_live_tup, n_dead_tup, autovacuum_count, autoanalyze_count FROM pg_stat_user_tables ORDER BY relname" \
  > "$out/pg-stats.txt" 2>&1 || true
for s in api relay sender simprovider; do
  "${compose[@]}" logs --no-color "$s" 2>&1 | sed -E 's/"time":"[^"]*",//; s/"delivery_id":[0-9]+,?//' | grep -E '"level":"(WARN|ERROR)"|panic' | cut -c1-300 | sort | uniq -c | sort -rn | head -20 > "$out/log-problems-$s.txt" || true
done

if [ "${KEEP_STACK:-0}" != 1 ]; then "${compose[@]}" down -v >/dev/null 2>&1; fi
log "results in docs/results/$(basename "$out")"
