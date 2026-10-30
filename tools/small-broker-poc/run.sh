#!/bin/bash
# One experiment on a fresh cluster: create N topics, run load, restart a
# broker under load, and report memory at each step.
#
#   PROFILE=profiles/two-gib.yaml ./run.sh N [SECONDS] [MB_PER_SEC]
set -uo pipefail
d=$(cd "$(dirname "$0")" && pwd)
n=${1:?N}
secs=${2:-60}
mbps=${3:-10}

"$d/cluster.sh" down
"$d/cluster.sh" up
# Let the first housekeeping pass run: it allocates the compaction key map.
sleep 40
echo "== idle, no topics"
"$d/mem.sh"
"$d/mktopics.sh" "$n" | tail -1
sleep 20
echo "== idle, $n topics"
"$d/mem.sh"
python3 "$d/load.py" "$n" "$secs" "$mbps" 2>/dev/null
echo "== after ${secs}s at ${mbps}MB/s"
"$d/mem.sh"

python3 "$d/load.py" "$n" 40 "$mbps" 1 >/dev/null 2>&1 &
sleep 8
docker restart rp2 >/dev/null
t0=$SECONDS
until docker exec rp0 rpk cluster health 2>/dev/null | grep -q "Healthy:.*true" &&
  [ $((SECONDS - t0)) -gt 10 ]; do
  sleep 2
done
echo "== rp2 restarted under load, cluster healthy after $((SECONDS - t0))s"
wait
"$d/mem.sh" | grep rp2
echo "allocation failures or asserts on rp2: $(docker logs rp2 2>&1 | grep -cE 'bad_alloc|Assert failure')"
