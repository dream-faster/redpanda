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
name() { echo "rp-$1"; }

# The user and rpk path are properties of the image, not of its role in this
# test: an upstream stock image runs as 101 with rpk on PATH, while anything
# built from this repo runs as 65532 with rpk only at an absolute path. The
# baseline can be either -- it is the deployed dedup build by default -- so
# ask the image instead of assuming.
image_uid() { # image_uid <image> <fallback>
  local u
  u=$(docker image inspect -f '{{.Config.User}}' "$1" 2>/dev/null | cut -d: -f1)
  if [[ $u =~ ^[0-9]+$ ]]; then echo "$u"; else echo "$2"; fi
}

image_rpk() { # image_rpk <image>
  if docker run --rm --entrypoint /opt/redpanda/bin/rpk "$1" version >/dev/null 2>&1; then
    echo /opt/redpanda/bin/rpk
  else
    echo rpk
  fi
}

start_node() {
  local id="$1" image="$2" role="$3" extra=()
  # A rollout has to hand the data directory to the incoming image's user.
  local uid
  if [[ $role == "slim" ]]; then
    uid="$SLIM_UID"
  else
    uid="$STOCK_UID"
  fi
  docker run --rm --user 0 -v "$WORK/data$id:/d" busybox chown -R "$uid:$uid" /d
  extra+=(--user "$uid:$uid")
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

# Health settles a few seconds after topics are created or nodes rejoin.
wait_healthy() {
  local id="$1"
  for _ in $(seq 1 60); do
    if rpk "$id" cluster health 2>/dev/null | grep -q 'Healthy:.*true'; then
      return 0
    fi
    sleep 2
  done
  echo "--- cluster health as seen from node $id"
  rpk "$id" cluster health || true
  return 1
}

# expect_config <node> <topic> <config> <value>
expect_config() {
  local out
  out=$(rpk "$1" topic describe "$2" -c 2>&1) || true
  if ! grep -Eq "^$3[[:space:]]+$4([[:space:]]|\$)" <<<"$out"; then
    echo "--- topic describe -c $2 as seen from node $1 (wanted $3 = $4)"
    echo "$out"
    return 1
  fi
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

STOCK_UID=$(image_uid "$STOCK" 101)
SLIM_UID="${SLIM_UID:-$(image_uid "$SLIM" 65532)}"
STOCK_RPK="${STOCK_RPK:-$(image_rpk "$STOCK")}"
SLIM_RPK="${SLIM_RPK:-/opt/redpanda/bin/rpk}"
echo "baseline uid=$STOCK_UID rpk=$STOCK_RPK; slim uid=$SLIM_UID rpk=$SLIM_RPK"

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
  chown "$STOCK_UID:$STOCK_UID" "$WORK/data$id" 2>/dev/null ||
    sudo chown "$STOCK_UID:$STOCK_UID" "$WORK/data$id"
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
rpk 0 topic consume plain -o start -n 50 -g upgrade-group -f '%v\n' >/dev/null ||
  fail "group commit"
rpk 0 security user create upgrader -p secret >/dev/null 2>&1 || true
sleep 15
rpk 0 topic create after-snapshot -p 3 -r 3 >/dev/null || fail "create after-snapshot"
for i in $(seq 1 50); do echo "s-$i"; done | rpk_in 0 topic produce after-snapshot -p 0 >/dev/null

# Dedup index state only exists when the node being replaced already runs
# dedup, so probe for the property instead of assuming it: upstream stock
# rejects it, a predecessor built from this branch accepts it. Pass that
# image as the baseline (docker-build's stock_image input) to exercise this.
#
# --acks=1 is deliberate. rpk's default --acks=-1 keeps franz-go's idempotent
# producer, and the dedup filter bypasses idempotent and transactional produce
# by design, so duplicates would sail through and the leg would assert nothing.
# Only --acks=0 and --acks=1 disable idempotency, and --acks=0 gives no ack to
# order the counts against.
DEDUP=false
if rpk 0 topic create deduped -p 1 -r 3 \
  -c redpanda.dedup.window.ms=3600000 >/dev/null 2>&1; then
  DEDUP=true
  printf 'k1:v1\nk2:v2\nk1:v3\n' |
    rpk_in 0 topic produce deduped --acks=1 -f '%k:%v\n' -p 0 >/dev/null ||
    fail "produce deduped"
  sleep 5
  got=$(count 0 deduped 0)
  [[ $got == 2 ]] || fail "dedup did not suppress before the upgrade: $got/2"
else
  echo "-- baseline has no dedup property; skipping the dedup leg"
fi

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
  wait_healthy "$id" || fail "unhealthy after node $id"
done

echo "== verifying pre-upgrade data on slim"
for p in 0 1 2 3 4 5; do
  got=$(count 0 plain "$p")
  [[ $got == 200 ]] || fail "plain/$p has $got/200 messages"
done
got=$(count 1 after-snapshot 0)
[[ $got == 50 ]] || fail "after-snapshot has $got/50"
expect_config 0 plain min.cleanable.dirty.ratio 0.3 || fail "topic config lost"
if [[ $DEDUP == true ]]; then
  echo "-- dedup state across the upgrade"
  expect_config 0 deduped redpanda.dedup.window.ms 3600000 || fail "dedup config lost"
  # A key first seen before the roll must still be suppressed. The index is
  # log-derived, so this passes only if the dedup snapshot and the replayed
  # log both survived the node replacements -- the disruption this leg exists
  # to catch.
  printf 'k1:v4\n' |
    rpk_in 0 topic produce deduped --acks=1 -f '%k:%v\n' -p 0 >/dev/null ||
    fail "produce deduped after the upgrade"
  sleep 5
  got=$(count 0 deduped 0)
  [[ $got == 2 ]] || fail "pre-upgrade dedup state lost: $got/2"
  # And the filter still indexes identities it sees for the first time now.
  printf 'k3:v5\nk3:v6\n' |
    rpk_in 0 topic produce deduped --acks=1 -f '%k:%v\n' -p 0 >/dev/null ||
    fail "produce a new dedup identity"
  sleep 5
  got=$(count 0 deduped 0)
  [[ $got == 3 ]] || fail "dedup stopped working after the upgrade: $got/3"
fi
expect_config 0 retained retention.ms 86400000 || fail "retention lost"
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
