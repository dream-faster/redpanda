#!/bin/bash
# Create N single-partition RF=3 topics t0000..t(N-1), 50 per request.
#   ./mktopics.sh N
set -uo pipefail
n=${1:?N}
ok=0
fail=0
for ((s = 0; s < n; s += 50)); do
  e=$((s + 49 < n - 1 ? s + 49 : n - 1))
  out=$(docker exec rp0 rpk topic create $(seq -f "t%04g" $s $e) -r 3 -p 1 2>&1)
  ok=$((ok + $(grep -c " OK" <<<"$out")))
  fail=$((fail + $(grep -vc -e " OK" -e "^TOPIC" <<<"$out")))
  grep -v -e " OK" -e "^TOPIC" <<<"$out" | head -1
done
echo "created ok=$ok failed=$fail"
