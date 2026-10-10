#!/bin/bash
# Rolling upgrade of a three node stock cluster to the slim broker.
#
#   upgrade_test.sh <stock-image> <slim-image>
#
# Brings up three stock nodes, writes data and cluster state, then replaces
# the nodes one by one with the slim image on the same data volumes while
# producing and consuming through the cluster. Fails if any step breaks, or
# if data written before the upgrade is not readable afterwards.
set -euo pipefail

STOCK="$1"
SLIM="$2"
NET="rp-upgrade-net"
NODES=(0 1 2)
STOCK_RPK="rpk"
SLIM_RPK="${SLIM_RPK:-/opt/redpanda/bin/rpk}"

name() { echo "rp-$1"; }

start_node() {
  local id="$1" image="$2" role="$3" extra=()
  # The slim image runs as 65532, stock as 101: let the new image take over the
  # volume's owner so the data directory stays writable.
  extra+=(--user "$(stat -c %u "$WORK/data$id")")
  docker run -d --name "$(name "$id")" --hostname "$(name "$id")" --network "$NET" \
    --label "slim-role=$role" \
    -v "$WORK/data$id:/var/lib/redpanda/data" "${extra[@]}" "$image" \
    redpanda start --node-id "$id" \
    --kafka-addr "0.0.0.0:9092" --advertise-kafka-addr "$(name "$id"):9092" \
    --rpc-addr "0.0.0.0:33145" --advertise-rpc-addr "$(name "$id"):33145" \
    --seeds "$(name 0):33145" --smp 1 --memory 1G --reserve-memory 0M \
    --overprovisioned --unsafe-bypass-fsync=1 >/dev/null
}

rpk_bin() { # image of container $1
  if [[ "$(docker inspect -f '{{index .Config.Labels "slim-role"}}' "$(name "$1")")" == "slim" ]]; then
    echo "$SLIM_RPK"
  else
    echo "$STOCK_RPK"
  fi
}

# rpk <node> args...   (runs rpk inside the node's own container)
rpk() {
  local n="$1"
  shift
  docker exec "$(name "$n")" "$(rpk_bin "$n")" -X brokers="$(name "$n"):9092" "$@"
}

# rpk_in <node> args...   (reads stdin)
rpk_in() {
  local n="$1"
  shift
  docker exec -i "$(name "$n")" "$(rpk_bin "$n")" -X brokers="$(name "$n"):9092" "$@"
}

wait_ready() {
  local id="$1"
  for _ in $(seq 1 120); do
    if docker exec "$(name "$id")" "$(rpk_bin "$id")" cluster health \
      -X brokers="$(name "$id"):9092" 2>/dev/null | grep -q 'Healthy:.*true'; then
      return 0
    fi
    if [[ "$(docker inspect -f '{{.State.Running}}' "$(name "$id")")" != "true" ]]; then
      echo "node $id exited"
      docker logs --tail 200 "$(name "$id")" || true
      return 1
    fi
    sleep 2
  done
  echo "node $id never became healthy"
  docker logs --tail 200 "$(name "$id")" || true
  return 1
}

fail() {
  for id in "${NODES[@]}"; do
    echo "=== node $id log tail"
    docker logs --tail 40 "$(name "$id")" 2>&1 || true
  done
  echo "UPGRADE TEST FAILED: $1"
  exit 1
}

count() { # count <node> <topic> <partition> -> messages readable
  rpk_in "$1" topic consume "$2" -p "$3" -o :end -f '%v\n' 2>/dev/null |
    wc -l | tr -d ' '
}

WORK=$(mktemp -d)
cleanup() {
  for id in "${NODES[@]}"; do docker rm -f "$(name "$id")" >/dev/null 2>&1 || true; done
  docker network rm "$NET" >/dev/null 2>&1 || true
  # data was written by container users
  docker run --rm -v "$WORK:/w" busybox rm -rf /w/data0 /w/data1 /w/data2 >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker network create "$NET" >/dev/null
for id in "${NODES[@]}"; do
  mkdir -p "$WORK/data$id"
  # stock images run as uid 101
  chown 101:101 "$WORK/data$id" 2>/dev/null || sudo chown 101:101 "$WORK/data$id"
done

echo "== starting three stock nodes: $STOCK"
for id in "${NODES[@]}"; do start_node "$id" "$STOCK" stock; done
for id in "${NODES[@]}"; do wait_ready "$id" || fail "stock node $id"; done

echo "== writing state on stock"
# Snapshot the controller quickly so both the snapshot and the log tail are
# exercised by the upgrade.
rpk 0 cluster config set controller_snapshot_max_age_sec 5 >/dev/null || true
rpk 0 topic create plain -p 6 -r 3 >/dev/null || fail "create plain"
rpk 0 topic create compacted -p 3 -r 3 -c cleanup.policy=compact >/dev/null ||
  fail "create compacted"
rpk 0 topic create retained -p 1 -r 3 -c retention.ms=86400000 -c retention.bytes=1073741824 \
  -c segment.bytes=1048576 -c write.caching=true >/dev/null || fail "create retained"
rpk 0 topic alter-config plain --set min.cleanable.dirty.ratio=0.3 >/dev/null || fail "alter"
for p in 0 1 2 3 4 5; do
  for i in $(seq 1 200); do echo "p$p-$i"; done | rpk_in 0 topic produce plain -p "$p" >/dev/null ||
    fail "produce plain $p"
done
printf 'a:1\nb:2\na:3\n' | rpk_in 0 topic produce compacted -f '%k:%v\n' -p 0 >/dev/null ||
  fail "produce compacted"
rpk 0 topic consume plain -p 0 -o start -n 50 -g upgrade-group -f '%v\n' >/dev/null ||
  fail "group commit"
rpk 0 security user create upgrader -p secret >/dev/null 2>&1 || true
sleep 15
rpk 0 topic create after-snapshot -p 3 -r 3 >/dev/null || fail "create after-snapshot"
for i in $(seq 1 50); do echo "s-$i"; done | rpk_in 0 topic produce after-snapshot -p 0 >/dev/null

echo "== rolling upgrade to slim: $SLIM"
for id in 2 1 0; do
  echo "-- upgrading node $id"
  docker stop -t 60 "$(name "$id")" >/dev/null
  docker rm "$(name "$id")" >/dev/null
  start_node "$id" "$SLIM" slim
  wait_ready "$id" || fail "slim node $id did not rejoin"

  # Mixed cluster traffic through the upgraded node and a node still on stock.
  other=$(((id + 1) % 3))
  [[ $id == 0 ]] && other=1
  topic="mixed-$id"
  rpk "$id" topic create "$topic" -p 3 -r 3 >/dev/null || fail "create $topic via slim node $id"
  for i in $(seq 1 30); do echo "m-$i"; done | rpk_in "$id" topic produce "$topic" -p 0 >/dev/null ||
    fail "produce via node $id"
  got=$(count "$other" "$topic" 0)
  [[ $got == 30 ]] || fail "node $other read $got/30 from $topic"
  rpk "$id" cluster health | grep -q 'Healthy:.*true' || fail "unhealthy after node $id"
done

echo "== verifying pre-upgrade data on slim"
for p in 0 1 2 3 4 5; do
  got=$(count 0 plain "$p")
  [[ $got == 200 ]] || fail "plain/$p has $got/200 messages"
done
got=$(count 1 after-snapshot 0)
[[ $got == 50 ]] || fail "after-snapshot has $got/50"
rpk 0 topic describe plain -c | grep -q 'min.cleanable.dirty.ratio *0.3' ||
  fail "topic config lost"
rpk 0 topic describe retained -c | grep -q 'retention.ms *86400000' || fail "retention lost"
rpk 2 group describe upgrade-group | grep -q 'upgrade-group' || fail "group lost"
rpk 1 topic list | grep -q compacted || fail "compacted topic lost"
rpk 0 cluster config get controller_snapshot_max_age_sec | grep -q '^5$' ||
  fail "cluster config lost"

echo "== restart the slim cluster once more"
for id in 0 1 2; do docker restart -t 60 "$(name "$id")" >/dev/null; done
for id in 0 1 2; do wait_ready "$id" || fail "slim restart $id"; done
got=$(count 2 plain 3)
[[ $got == 200 ]] || fail "plain/3 has $got/200 after slim restart"

echo "UPGRADE TEST OK"
