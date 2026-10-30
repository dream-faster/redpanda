#!/bin/bash
# Start or stop a 3-broker Redpanda cluster in Docker, one core per broker.
#
#   [PROFILE=profile.yaml] [RPMEM=2G] [CMEM=2560m] ./cluster.sh up [image]
#   ./cluster.sh down
#
# PROFILE is a cluster-config bootstrap file (see profiles/; defaults.yaml if
# unset). RPMEM is the
# memory given to Redpanda (--memory), CMEM the container's cgroup limit.
# The host must resolve rp0..rp2 for load.py; `./cluster.sh hosts` prints the
# /etc/hosts lines.
set -euo pipefail
d=$(cd "$(dirname "$0")" && pwd)
cmd=${1:?up|down|hosts}
img=${2:-redpandadata/redpanda:v26.2.6}
work=${WORK:-$d/.work}

if [ "$cmd" = down ]; then
  docker rm -f -v rp0 rp1 rp2 >/dev/null 2>&1 || true
  exit 0
fi
if [ "$cmd" = hosts ]; then
  for i in 0 1 2; do
    echo "$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' rp$i) rp$i"
  done
  exit 0
fi

mkdir -p "$work"
docker network create rpnet >/dev/null 2>&1 || true
for i in 0 1 2; do
  # SMALL_DISK=1 (the default) swaps in a 4 KiB fallocation step so that a
  # test machine's disk isn't what runs out first; memory is unaffected.
  grep -v '^segment_fallocation_step:' "${PROFILE:-$d/profiles/defaults.yaml}" \
    >"$work/bootstrap-rp$i.yaml"
  if [ "${SMALL_DISK:-1}" = 1 ]; then
    echo "segment_fallocation_step: 4096" >>"$work/bootstrap-rp$i.yaml"
  fi
  docker run -d --name rp$i --hostname rp$i --network rpnet \
    --memory "${CMEM:-2560m}" --ulimit nofile=20000:20000 \
    -v "$work/bootstrap-rp$i.yaml:/etc/redpanda/.bootstrap.yaml" \
    "$img" redpanda start --node-id $i --smp 1 --memory "${RPMEM:-2G}" \
    --reserve-memory 0M --overprovisioned --check=false \
    --kafka-addr internal://0.0.0.0:9092 --advertise-kafka-addr internal://rp$i:9092 \
    --rpc-addr 0.0.0.0:33145 --advertise-rpc-addr rp$i:33145 \
    --seeds rp0:33145,rp1:33145,rp2:33145 >/dev/null
done
for _ in $(seq 60); do
  docker exec rp0 rpk cluster health 2>/dev/null | grep -q "Healthy:.*true" && exit 0
  sleep 2
done
echo "cluster did not become healthy" >&2
exit 1
