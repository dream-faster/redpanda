#!/bin/bash
# Per-broker memory from Seastar's own counters, in MiB:
#   allocated     - everything allocated, including reclaimable batch cache
#   hard          - allocated minus what Seastar could reclaim under pressure
#   peak_hard     - the most `hard` has been since the broker started
set -uo pipefail
for i in 0 1 2; do
  docker exec rp$i curl -s localhost:9644/metrics | awk -v b=rp$i '
    /^vectorized_memory_total_memory\{/ {t = $2}
    /^vectorized_memory_allocated_memory\{/ {a = $2}
    /^vectorized_memory_available_memory\{/ {v = $2}
    /^vectorized_memory_available_memory_low_water_mark\{/ {l = $2}
    END {printf "%s allocated=%d hard=%d peak_hard=%d\n", b, a/2^20, (t-v)/2^20, (t-l)/2^20}'
done
