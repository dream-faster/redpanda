#!/bin/bash
# Start the slim broker, exercise the Kafka path, restart it on the same data
# directory and check the log survived.
#
#   smoke.sh <redpanda-binary> <rpk-binary> [workdir]
set -euo pipefail

RP="$(realpath "$1")"
RPK="$(realpath "$2")"
WORK="${3:-$(mktemp -d)}"
DATA="$WORK/data"
LOG="$WORK/redpanda.log"
CFG="$WORK/redpanda.yaml"
BROKERS="127.0.0.1:9092"
mkdir -p "$DATA"

cat >"$CFG" <<YAML
redpanda:
  data_directory: $DATA
  node_id: 0
  developer_mode: true
  rpc_server:
    address: 127.0.0.1
    port: 33145
  kafka_api:
    - address: 127.0.0.1
      port: 9092
  admin:
    - address: 127.0.0.1
      port: 9644
  seed_servers: []
YAML

rpk() { "$RPK" -X brokers="$BROKERS" "$@"; }

PID=""
start_broker() {
  "$RP" --redpanda-cfg "$CFG" --default-log-level=info --smp 1 --memory 1G \
    --reserve-memory 0M --overprovisioned --reactor-backend=epoll \
    --unsafe-bypass-fsync=1 >>"$LOG" 2>&1 &
  PID=$!
  for _ in $(seq 1 120); do
    if curl -fsS http://127.0.0.1:9644/v1/status/ready >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "$PID" 2>/dev/null; then
      echo "broker exited during startup"
      return 1
    fi
    sleep 1
  done
  echo "broker did not become ready"
  return 1
}

stop_broker() {
  if [[ -n $PID ]] && kill -0 "$PID" 2>/dev/null; then
    kill -TERM "$PID"
    for _ in $(seq 1 60); do
      kill -0 "$PID" 2>/dev/null || break
      sleep 1
    done
    kill -KILL "$PID" 2>/dev/null || true
  fi
  PID=""
}

fail() {
  echo "SMOKE FAILED: $1"
  echo "=== last 200 lines of broker log ==="
  tail -n 200 "$LOG" || true
  stop_broker
  exit 1
}
trap 'stop_broker' EXIT

expect_lines() {
  local want="$1" got
  got=$(cat)
  [[ $got == "$want" ]] || fail "expected '$want' got '$got'"
}

echo "== first boot"
start_broker || fail "first boot"

rpk topic create plain -p 3 -r 1 || fail "create plain"
rpk topic create compacted -p 1 -r 1 -c cleanup.policy=compact -c segment.bytes=1048576 ||
  fail "create compacted"
rpk topic create retained -p 1 -r 1 -c retention.ms=86400000 -c retention.bytes=104857600 ||
  fail "create retained"
rpk topic alter-config plain --set retention.ms=3600000 || fail "alter-config"

for i in $(seq 1 100); do echo "msg-$i"; done | rpk topic produce plain -p 0 >/dev/null ||
  fail "produce plain"
printf 'k1:v1\nk2:v2\n' | rpk topic produce compacted -f '%k:%v\n' >/dev/null ||
  fail "produce compacted"

rpk topic consume plain -p 0 -o start -n 100 -f '%v\n' | wc -l |
  tr -d ' ' | expect_lines 100
rpk topic consume plain -o start -n 100 -g smoke-group -f '%v\n' >/dev/null ||
  fail "consume with group"
rpk group describe smoke-group >/dev/null || fail "describe group"
rpk cluster health | grep -q 'Healthy:.*true' || fail "cluster not healthy"
rpk topic describe plain -c | grep -q 'retention.ms *3600000' ||
  fail "alter-config not visible"

echo "== restart on the same data directory"
stop_broker
start_broker || fail "restart"

rpk topic consume plain -p 0 -o start -n 100 -f '%v\n' | wc -l |
  tr -d ' ' | expect_lines 100
rpk topic list | grep -q compacted || fail "topics lost across restart"
rpk topic describe plain -c | grep -q 'retention.ms *3600000' ||
  fail "topic config lost across restart"
rpk group describe smoke-group | grep -q 'TOTAL-LAG *0' ||
  fail "group offsets lost across restart"

echo "== appends after restart"
for i in $(seq 101 120); do echo "msg-$i"; done | rpk topic produce plain -p 0 >/dev/null ||
  fail "produce after restart"
rpk topic consume plain -p 0 -o start -n 120 -f '%v\n' | wc -l |
  tr -d ' ' | expect_lines 120

stop_broker
echo "SMOKE OK"
