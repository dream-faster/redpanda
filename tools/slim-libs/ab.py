#!/usr/bin/env python3
"""Paired A/B compile-time comparison.

Each TU is compiled with arm A and arm B back to back (order alternates per
TU) by the same worker, so machine drift cancels in the per-TU ratio. CPU time
comes from rusage. Prints totals, mean/median ratio and a bootstrap 95% CI.

Usage: ab.py --name NAME [--mode syntax|obj] [--corpus F] [-j 4] [--reps 1]
             [--a-env K=V ...] [--a-flag FLAG ...] [--b-env K=V ...] [--b-flag FLAG ...]
Arm A defaults to the untouched baseline (no env, no flags).
"""
import argparse, json, os, random, statistics, subprocess, tempfile
from concurrent.futures import ThreadPoolExecutor

HARNESS = os.environ.get("HARNESS", "/home/user/work/harness")
RESULTS = os.environ.get("RESULTS", "/home/user/work/results")


def arm_cmd(envkv, flags):
    env = os.environ.copy()
    for kv in envkv:
        k, v = kv.split("=", 1)
        env[k] = v
    out = subprocess.check_output(["bash", "-c", f'source {HARNESS}/flags.sh; echo "$CXX"; echo "$FLAGS" | tr "\\n" " "'], text=True, env=env).splitlines()
    return [out[0]] + out[1].split() + flags


def run(cmd, mode, tu):
    obj = tempfile.mktemp(suffix=".o")
    full = cmd + (["-fsyntax-only"] if mode == "syntax" else ["-c", "-o", obj]) + [tu]
    p = subprocess.Popen(full, cwd="/home/user/redpanda", stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    err = p.stderr.read()
    _, status, ru = os.wait4(p.pid, 0)
    if os.path.exists(obj):
        os.unlink(obj)
    rc = os.waitstatus_to_exitcode(status)
    return ((ru.ru_utime + ru.ru_stime) if rc == 0 else None), err.decode(errors="replace")[:600]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True)
    ap.add_argument("--mode", default="obj", choices=["syntax", "obj"])
    ap.add_argument("--corpus", default="/home/user/redpanda/tools/slim-libs/corpus.txt")
    ap.add_argument("-j", type=int, default=4)
    ap.add_argument("--reps", type=int, default=1)
    ap.add_argument("--a-env", action="append", default=[])
    ap.add_argument("--b-env", action="append", default=[])
    ap.add_argument("--a-flag", action="append", default=[])
    ap.add_argument("--b-flag", action="append", default=[])
    a = ap.parse_args()
    A, B = arm_cmd(a.a_env, a.a_flag), arm_cmd(a.b_env, a.b_flag)
    tus = [l.strip() for l in open(a.corpus) if l.strip()]
    jobs = [(i, r, t) for r in range(a.reps) for i, t in enumerate(tus)]

    def work(job):
        i, r, tu = job
        first_a = (i + r) % 2 == 0
        if first_a:
            ta, ea = run(A, a.mode, tu); tb, eb = run(B, a.mode, tu)
        else:
            tb, eb = run(B, a.mode, tu); ta, ea = run(A, a.mode, tu)
        return tu, ta, tb, ea, eb

    with ThreadPoolExecutor(a.j) as ex:
        rows = list(ex.map(work, jobs))
    per = {}
    broken = {}
    for tu, ta, tb, ea, eb in rows:
        if ta is None:
            continue
        if tb is None:
            broken[tu] = eb
            continue
        per.setdefault(tu, []).append((ta, tb))
    ta_tot = sum(statistics.mean(x[0] for x in v) for v in per.values())
    tb_tot = sum(statistics.mean(x[1] for x in v) for v in per.values())
    ratios = [statistics.mean(x[1] for x in v) / statistics.mean(x[0] for x in v) for v in per.values()]
    random.seed(1)
    boots = []
    keys = list(per)
    for _ in range(2000):
        s = [random.choice(keys) for _ in keys]
        boots.append(sum(statistics.mean(x[1] for x in per[k]) for k in s) / sum(statistics.mean(x[0] for x in per[k]) for k in s))
    boots.sort()
    res = {"name": a.name, "mode": a.mode, "tus": len(per), "broken": list(broken), "A_total": ta_tot, "B_total": tb_tot,
           "saved_pct": 100 * (ta_tot - tb_tot) / ta_tot, "ci95_pct": [100 * (1 - boots[1975]), 100 * (1 - boots[25])],
           "median_ratio": statistics.median(ratios), "per_tu": {k: [statistics.mean(x[0] for x in v), statistics.mean(x[1] for x in v)] for k, v in per.items()},
           "args": {k: getattr(a, k) for k in ("a_env", "b_env", "a_flag", "b_flag", "reps", "mode")}}
    os.makedirs(RESULTS, exist_ok=True)
    json.dump(res, open(f"{RESULTS}/ab_{a.name}.{a.mode}.json", "w"), indent=1)
    print(f"{a.name:28s} [{a.mode:6s}] TUs={len(per):3d} broken={len(broken):2d}  A={ta_tot:7.1f}s B={tb_tot:7.1f}s  saved {res['saved_pct']:5.1f}%  CI95 [{res['ci95_pct'][0]:5.1f}, {res['ci95_pct'][1]:5.1f}]  median ratio {res['median_ratio']:.3f}")
    for k, e in list(broken.items())[:3]:
        print("   broken:", k, e.strip().splitlines()[0] if e.strip() else "")


if __name__ == "__main__":
    main()
