#!/usr/bin/env python3
"""Compare bench results: compare.py BASE NEW [mode]  (names as given to bench.py --name)."""
import json, os, statistics, sys
R = os.environ.get("RESULTS", "/home/user/work/results")
base, new = sys.argv[1], sys.argv[2]
mode = sys.argv[3] if len(sys.argv) > 3 else "obj"
def load(n):
    return json.load(open(f"{R}/{n}.{mode}.json"))
b, n = load(base), load(new)
common = [t for t in b if b[t] is not None and n.get(t) is not None]
failed = [t for t in b if b[t] is not None and n.get(t) is None]
tb, tn = sum(b[t] for t in common), sum(n[t] for t in common)
ratios = [n[t] / b[t] for t in common]
print(f"{base} -> {new} [{mode}]  TUs compared={len(common)} (broken by change: {len(failed)})")
print(f"  total CPU  {tb:8.1f}s -> {tn:8.1f}s   saved {tb-tn:7.1f}s  ({100*(tb-tn)/tb:5.1f}%)")
print(f"  per-TU ratio: median {statistics.median(ratios):.3f}  p10 {sorted(ratios)[len(ratios)//10]:.3f}  p90 {sorted(ratios)[9*len(ratios)//10]:.3f}")
if failed: print("  broken:", ", ".join(failed[:8]))
