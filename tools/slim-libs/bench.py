#!/usr/bin/env python3
"""Compile a corpus of TUs and record per-TU CPU time (user+sys) via rusage.

Usage: bench.py --name NAME [--mode syntax|obj] [--corpus FILE] [-j N]
                [--env KEY=VAL ...] [-- extra clang flags]
Results go to $RESULTS/NAME.<mode>.json ({tu: cpu_seconds}); failures are
recorded with null so a configuration that breaks TUs is visible.

CPU time (not wall) is used because the box has 4 cores and TUs run in
parallel; rusage is insensitive to scheduling.
"""
import argparse
import json
import os
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor

HARNESS = os.environ.get("HARNESS", "/home/user/work/harness")
RESULTS = os.environ.get("RESULTS", "/home/user/work/results")


def flags(env):
    out = subprocess.check_output(
        ["bash", "-c", f'source {HARNESS}/flags.sh; echo "$CXX"; echo "$FLAGS" | tr "\\n" " "'],
        text=True, env=env).splitlines()
    return out[0], out[1].split()


def compile_one(cxx, fl, extra, mode, tu, logdir):
    obj = tempfile.mktemp(suffix=".o")
    cmd = [cxx] + fl + extra + (["-fsyntax-only"] if mode == "syntax" else ["-c", "-o", obj]) + [tu]
    with open(os.path.join(logdir, tu.replace("/", "_") + ".log"), "w") as lf:
        p = subprocess.Popen(cmd, cwd="/home/user/redpanda", stdout=lf, stderr=subprocess.STDOUT)
        _, status, ru = os.wait4(p.pid, 0)
    if os.path.exists(obj):
        os.unlink(obj)
    rc = os.waitstatus_to_exitcode(status)
    return tu, (ru.ru_utime + ru.ru_stime) if rc == 0 else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True)
    ap.add_argument("--mode", default="obj", choices=["syntax", "obj"])
    ap.add_argument("--corpus", default="/home/user/redpanda/tools/slim-libs/corpus.txt")
    ap.add_argument("-j", type=int, default=4)
    ap.add_argument("--env", action="append", default=[])
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("extra", nargs="*")
    a = ap.parse_args()
    env = os.environ.copy()
    for kv in a.env:
        k, v = kv.split("=", 1)
        env[k] = v
    cxx, fl = flags(env)
    tus = [l.strip() for l in open(a.corpus) if l.strip()]
    if a.limit:
        tus = tus[: a.limit]
    os.makedirs(RESULTS, exist_ok=True)
    logdir = os.path.join(RESULTS, f"{a.name}.{a.mode}.logs")
    os.makedirs(logdir, exist_ok=True)
    with ThreadPoolExecutor(a.j) as ex:
        res = dict(ex.map(lambda t: compile_one(cxx, fl, a.extra, a.mode, t, logdir), tus))
    json.dump(res, open(os.path.join(RESULTS, f"{a.name}.{a.mode}.json"), "w"), indent=1)
    ok = [v for v in res.values() if v is not None]
    print(f"{a.name} [{a.mode}] tus={len(res)} ok={len(ok)} failed={len(res)-len(ok)} cpu_total={sum(ok):.1f}s mean={sum(ok)/max(1,len(ok)):.2f}s")


if __name__ == "__main__":
    main()
