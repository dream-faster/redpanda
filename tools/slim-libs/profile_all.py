#!/usr/bin/env python3
"""Run profile_tu over the corpus in parallel and aggregate per-library time.

Usage: profile_all.py --name NAME [-j 4] [-- extra clang flags]
Writes $RESULTS/NAME.profile.json (per TU) and prints the aggregate.
"""
import argparse, collections, json, os, subprocess, sys
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.environ.get("RESULTS", "/home/user/work/results")


def one(tu, extra):
    p = subprocess.run([sys.executable, os.path.join(HERE, "profile_tu.py"), tu] + extra, capture_output=True, text=True)
    try:
        return tu, json.loads(p.stdout)
    except Exception:
        return tu, {"rc": -1, "err": p.stderr[-500:]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True)
    ap.add_argument("--corpus", default=os.path.join(HERE, "corpus.txt"))
    ap.add_argument("-j", type=int, default=4)
    ap.add_argument("extra", nargs="*")
    a = ap.parse_args()
    tus = [l.strip() for l in open(a.corpus) if l.strip()]
    with ThreadPoolExecutor(a.j) as ex:
        res = dict(ex.map(lambda t: one(t, a.extra), tus))
    os.makedirs(RESULTS, exist_ok=True)
    json.dump(res, open(os.path.join(RESULTS, f"{a.name}.profile.json"), "w"))
    aggregate(res)


def aggregate(res):
    ok = {k: v for k, v in res.items() if v.get("rc") == 0}
    print(f"{len(ok)}/{len(res)} TUs profiled")
    tot = collections.Counter()
    for v in ok.values():
        for k, x in v["totals"].items():
            tot[k] += x
    print("totals (s, sum over TUs):", {k: round(x, 1) for k, x in tot.items()})
    for key in ("source_self_by_lib", "inst_toplevel_by_lib", "pp_bytes_by_lib"):
        c = collections.Counter()
        for v in ok.values():
            for k, x in v[key].items():
                c[k] += x
        t = sum(c.values())
        print(f"\n{key} (sum over TUs):")
        for k, x in c.most_common():
            print(f"  {k:16s} {x:14.1f} {100*x/t:5.1f}%")
    h = collections.Counter()
    for v in ok.values():
        for p, x in v["top_headers"]:
            h[p] += x
    print("\nheaviest headers (self parse time summed over TUs, top 25):")
    for p, x in h.most_common(25):
        print(f"  {x:8.1f}s  {p.replace('/home/user/work/src/', '').replace('/usr/lib/llvm-22/bin/../include/c++/v1/', '<c++>/')}")


if __name__ == "__main__":
    main()
