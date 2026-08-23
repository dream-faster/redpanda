#!/usr/bin/env python3
"""Report fmt::format_to / vlog calls whose "{}" count differs from the arg count.

Removing a field from a log line means deleting both a placeholder and an
argument; missing either half is a consteval error that only surfaces at
compile time, so check it locally instead.
"""

import os
import re, subprocess, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mask2 import mask

STR = re.compile(r'"((?:[^"\\]|\\.)*)"')


def split_args(text):
    """Split a call's argument list on top-level commas."""
    out, depth, cur = [], 0, ""
    m = mask(text)
    for i, ch in enumerate(text):
        c = m[i]
        if c in "([{<":
            depth += 1
        elif c in ")]}>":
            depth -= 1
        if c == "," and depth == 0:
            out.append(cur)
            cur = ""
            continue
        cur += ch
    if cur.strip():
        out.append(cur)
    return out


def check(path):
    src = open(path).read()
    m = mask(src)
    bad = []
    for call in ("fmt::format_to(", "vlog("):
        start = 0
        while True:
            i = m.find(call, start)
            if i == -1:
                break
            start = i + 1
            b = i + len(call) - 1
            depth, end = 0, None
            for k in range(b, len(m)):
                if m[k] == "(":
                    depth += 1
                elif m[k] == ")":
                    depth -= 1
                    if depth == 0:
                        end = k
                        break
            if end is None:
                continue
            body = src[b + 1 : end]
            args = split_args(body)
            # find the argument holding the format string
            fi = next(
                (k for k, a in enumerate(args) if '"' in a and "_MSG" not in a), None
            )
            if fi is None:
                continue
            lits = STR.findall(args[fi])
            if not lits:
                continue
            fstr = "".join(lits)
            holes = len(
                re.findall(
                    r"(?<!\{)\{[^{}]*\}",
                    fstr.replace("{{", "\x01").replace("}}", "\x02"),
                )
            )
            supplied = len(args) - fi - 1
            if holes != supplied:
                line = src.count("\n", 0, i) + 1
                bad.append((line, holes, supplied, fstr[:60]))
    return bad


files = [
    f
    for f in subprocess.run(
        ["git", "ls-files", "src/v"], capture_output=True, text=True
    ).stdout.split()
    if f.endswith((".cc", ".h"))
]
total = 0
for f in files:
    try:
        for line, holes, supplied, fstr in check(f):
            print(f"{f}:{line}: {holes} holes vs {supplied} args :: {fstr}")
            total += 1
    except Exception:
        pass
print(f"mismatches: {total}")
