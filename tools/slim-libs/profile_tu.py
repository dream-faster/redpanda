#!/usr/bin/env python3
"""Compile one TU with -ftime-trace and attribute time to third-party libraries.

Usage: profile_tu.py <src.cc> [extra clang flags...]
Env:   HARNESS (dir holding flags.sh), OPT, OUT (json result path)

Prints/writes JSON: wall seconds, trace totals (frontend/backend), self time per
library for header parsing ("Source" events), instantiation time per library
(top-level Instantiate* events attributed by entity-name prefix) and the number
of bytes of preprocessed output per library (from -E line markers).
"""
import json
import os
import re
import subprocess
import sys
import tempfile
import time

HARNESS = os.environ.get("HARNESS", "/home/user/work/harness")

LIBS = [
    ("seastar", re.compile(r"/seastar/(include|src)/|/gen/seastar/")),
    ("boost", re.compile(r"/boost[^/]*/(flat/)?boost/|/boost/")),
    ("fmt", re.compile(r"/fmt/|/fmt/include/")),
    ("absl", re.compile(r"/abseil-cpp/absl/|/absl/")),
    ("protobuf", re.compile(r"/protobuf/|google/protobuf")),
    ("libc++", re.compile(r"/c\+\+/v1/|/llvm-[0-9]+/include/c\+\+")),
    ("libc/system", re.compile(r"^/usr/(include|lib/gcc)")),
    ("other3p", re.compile(r"/work/src/|/work/gen/(?!rp)")),
    ("redpanda", re.compile(r"/redpanda/src/v/|/work/gen/")),
]


def lib_of(path: str) -> str:
    for name, rx in LIBS:
        if rx.search(path):
            return name
    return "unknown"


INST_PREFIXES = [
    ("seastar", re.compile(r"^(seastar::|ss::|\(anonymous namespace\)::seastar)")),
    ("boost", re.compile(r"^boost::")),
    ("fmt", re.compile(r"^(fmt::|fmt_)")),
    ("absl", re.compile(r"^absl::")),
    ("protobuf", re.compile(r"^google::protobuf")),
    ("std", re.compile(r"^std::|^__")),
]


def inst_lib(detail: str) -> str:
    for name, rx in INST_PREFIXES:
        if rx.search(detail):
            return name
    return "redpanda/other"


def run(src, extra, mode="c"):
    sys.path.insert(0, HARNESS)
    env = os.environ.copy()
    cmd = f'source {HARNESS}/flags.sh; echo "$CXX"; echo "$FLAGS" | tr "\\n" " "'
    out = subprocess.check_output(["bash", "-c", cmd], text=True, env=env).splitlines()
    cxx, flags = out[0], out[1].split()
    tmp = tempfile.mkdtemp(prefix="prof")
    obj = os.path.join(tmp, "a.o")
    full = [cxx] + flags + extra + ["-ftime-trace", "-ftime-trace-granularity=200", "-c", src, "-o", obj]
    t0 = time.time()
    p = subprocess.run(full, cwd="/home/user/redpanda", capture_output=True, text=True)
    wall = time.time() - t0
    trace = os.path.join(tmp, "a.json")
    res = {"src": src, "rc": p.returncode, "wall": wall}
    if p.returncode != 0 or not os.path.exists(trace):
        res["err"] = p.stderr[-2000:]
        return res
    ev = json.load(open(trace))["traceEvents"]
    tot = {}
    srcs = []
    insts = []
    open_src = []
    for e in sorted((e for e in ev if e.get("ph") in ("X", "b", "e")), key=lambda e: e["ts"]):
        n = e["name"]
        if e["ph"] == "b" and n == "Source":
            open_src.append(e)
        elif e["ph"] == "e" and n == "Source":
            b = open_src.pop()
            srcs.append((b["ts"], e["ts"], b["args"]["detail"]))
        elif e["ph"] == "X":
            if n.startswith("Total "):
                tot[n[6:]] = e["dur"] / 1e6
            elif n in ("InstantiateClass", "InstantiateFunction"):
                insts.append((e["ts"], e["ts"] + e["dur"], e["args"]["detail"]))
    res["totals"] = {k: tot[k] for k in ("ExecuteCompiler", "Frontend", "Backend", "Source", "InstantiateClass", "InstantiateFunction", "PerformPendingInstantiations", "OptModule", "CodeGen Function") if k in tot}
    # self time of each Source event = dur - sum(dur of direct child Source events)
    srcs.sort(key=lambda x: (x[0], -x[1]))
    stack = []
    selfs = {}
    for s, e, d in srcs:
        while stack and stack[-1][1] <= s:
            stack.pop()
        if stack:
            stack[-1][3] += e - s
        stack.append([s, e, d, 0])
        selfs.setdefault(id(stack[-1]), stack[-1])
    # second pass collecting
    per = {}
    for rec in selfs.values():
        s, e, d, child = rec
        per[lib_of(d)] = per.get(lib_of(d), 0) + (e - s - child)
    res["source_self_by_lib"] = {k: v / 1e6 for k, v in sorted(per.items(), key=lambda kv: -kv[1])}
    # top-level instantiation time (no nesting double count): sort by start, skip nested
    insts.sort(key=lambda x: (x[0], -x[1]))
    last_end = -1
    inst_by = {}
    for s, e, d in insts:
        if s >= last_end:
            k = inst_lib(d)
            inst_by[k] = inst_by.get(k, 0) + (e - s)
            last_end = e
    res["inst_toplevel_by_lib"] = {k: v / 1e6 for k, v in sorted(inst_by.items(), key=lambda kv: -kv[1])}
    # headers: count distinct files per lib & top headers by self time
    hdr_self = {}
    for rec in selfs.values():
        s, e, d, child = rec
        hdr_self[d] = hdr_self.get(d, 0) + (e - s - child)
    res["top_headers"] = [(k, v / 1e6) for k, v in sorted(hdr_self.items(), key=lambda kv: -kv[1])[:25]]
    res["hdr_self"] = {k: v / 1e6 for k, v in hdr_self.items() if v > 20000}
    incl = {}
    for rec in selfs.values():
        incl[rec[2]] = incl.get(rec[2], 0) + (rec[1] - rec[0])
    res["hdr_incl"] = {k: v / 1e6 for k, v in incl.items() if v > 50000}
    res["n_headers"] = len(hdr_self)
    # preprocessed bytes per lib
    pp = subprocess.run([cxx] + flags + extra + ["-E", "-P" if False else "-E", src], cwd="/home/user/redpanda", capture_output=True, text=True)
    cur = "redpanda"
    by = {}
    rx = re.compile(r'^# \d+ "([^"]+)"')
    for line in pp.stdout.splitlines():
        m = rx.match(line)
        if m:
            cur = lib_of(m.group(1) if m.group(1).startswith("/") else "/home/user/redpanda/" + m.group(1))
            continue
        by[cur] = by.get(cur, 0) + len(line) + 1
    res["pp_bytes_by_lib"] = by
    return res


def main():
    src = sys.argv[1]
    extra = sys.argv[2:]
    res = run(src, extra)
    out = os.environ.get("OUT")
    s = json.dumps(res, indent=1)
    if out:
        open(out, "w").write(s)
    else:
        print(s)


if __name__ == "__main__":
    main()
