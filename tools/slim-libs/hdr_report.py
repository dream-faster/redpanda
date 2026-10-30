#!/usr/bin/env python3
"""Per-library ranking of headers by (a) self parse time, (b) inclusive time, summed over TUs.
Usage: hdr_report.py NAME [lib ...]"""
import collections, json, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from profile_tu import lib_of
RESULTS = os.environ.get("RESULTS", "/home/user/work/results")
name = sys.argv[1]
libs = sys.argv[2:] or ["seastar", "boost", "absl", "fmt", "other3p", "libc++", "unknown"]
r = json.load(open(f"{RESULTS}/{name}.profile.json"))
def short(p):
    for pre in ("/home/user/work/src/", "/usr/lib/llvm-22/bin/../include/c++/v1/", "/usr/lib/llvm-22/lib/clang/22/include/"):
        if p.startswith(pre): return p[len(pre):]
    return p.replace("/home/user/redpanda/src/v/", "rp:")
selfs = collections.defaultdict(collections.Counter); incl = collections.defaultdict(collections.Counter); cnt = collections.defaultdict(collections.Counter)
for tu, v in r.items():
    if v.get("rc") != 0: continue
    for p, x in v["hdr_self"].items():
        selfs[lib_of(p if p.startswith("/") else "/home/user/redpanda/" + p)][p] += x
    for p, x in v["hdr_incl"].items():
        l = lib_of(p if p.startswith("/") else "/home/user/redpanda/" + p)
        incl[l][p] += x; cnt[l][p] += 1
for l in libs:
    print(f"\n=== {l}: top headers by SELF time (s summed over TUs) ===")
    for p, x in selfs[l].most_common(12): print(f"  {x:7.1f}  {short(p)}")
    print(f"=== {l}: top headers by INCLUSIVE time (nested includes counted once at first inclusion per TU) ===")
    for p, x in incl[l].most_common(12): print(f"  {x:7.1f}  in {cnt[l][p]:3d} TUs  {short(p)}")
