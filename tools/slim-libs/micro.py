#!/usr/bin/env python3
"""Compile each micro/m_*.cc (one header + a tiny use) best-of-N and print CPU seconds.

Shows what a single library header costs a TU, independent of Redpanda code.
Usage: micro.py [--reps 3] [--env K=V ...] [-- extra clang flags]"""
import argparse, glob, os, subprocess
HARNESS = os.environ.get("HARNESS", "/home/user/work/harness")
ap = argparse.ArgumentParser()
ap.add_argument("--reps", type=int, default=3)
ap.add_argument("--env", action="append", default=[])
ap.add_argument("--only", default="")
ap.add_argument("extra", nargs="*")
a = ap.parse_args()
env = os.environ.copy()
for kv in a.env:
    k, v = kv.split("=", 1); env[k] = v
out = subprocess.check_output(["bash", "-c", f'source {HARNESS}/flags.sh; echo "$CXX"; echo "$FLAGS" | tr "\\n" " "'], text=True, env=env).splitlines()
cmd0 = [out[0]] + out[1].split() + a.extra
here = os.path.join(os.path.dirname(os.path.abspath(__file__)), "micro")
for f in sorted(glob.glob(os.path.join(here, "m_*.cc"))):
    if a.only and a.only not in f:
        continue
    best = 1e9
    for _ in range(a.reps):
        p = subprocess.Popen(cmd0 + ["-c", f, "-o", "/dev/null"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        _, st, ru = os.wait4(p.pid, 0)
        if st == 0:
            best = min(best, ru.ru_utime + ru.ru_stime)
    print(f"{os.path.basename(f):22s} {best if best < 1e8 else float('nan'):7.2f}s")
