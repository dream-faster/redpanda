#!/usr/bin/env python3
"""Rank include edges by the marginal subtree they pull in.

Reads `clang -E -H` stderr (one or more files) and, for every header node,
computes how many *new* files / bytes appear beneath it, given the order in
which the preprocessor first reached them. A big number on an edge
`parent -> child` means that removing (or forward declaring away) that one
include would remove that many headers from the TU.

Usage: edge_cost.py <H-output> [--top N] [--cross-lib-only]
"""
import argparse
import collections
import os
import re
import sys

sys.path.insert(0, os.path.dirname(__file__))
from profile_tu import lib_of  # noqa: E402


def norm(p):
    return p if p.startswith("/") else "/home/user/redpanda/" + p


def short(p):
    p = norm(p)
    for pre in ("/home/user/work/src/", "/home/user/redpanda/src/v/", "/usr/lib/llvm-22/bin/../include/c++/v1/"):
        if p.startswith(pre):
            return p[len(pre):] if pre != "/usr/lib/llvm-22/bin/../include/c++/v1/" else "<" + p[len(pre):] + ">"
    return p


def parse(paths):
    """returns list of (depth, path) in include order (first inclusions only appear in -H)."""
    out = []
    for path in paths:
        for l in open(path, errors="replace"):
            m = re.match(r"^(\.+) (\S+)$", l.rstrip("\n"))
            if m:
                out.append((len(m.group(1)), m.group(2)))
    return out


def edges_of(path, cross_only):
    seq = parse([path])
    n = len(seq)
    sub_files = [1] * n
    sub_bytes = []
    for _, p in seq:
        try:
            sub_bytes.append(os.path.getsize(norm(p)))
        except OSError:
            sub_bytes.append(0)
    parent = [-1] * n
    stack = []
    for i, (d, p) in enumerate(seq):
        stack = stack[: d - 1] + [i]
        if d > 1:
            parent[i] = stack[d - 2]
    for i in range(n - 1, -1, -1):
        if parent[i] >= 0:
            sub_files[parent[i]] += sub_files[i]
            sub_bytes[parent[i]] += sub_bytes[i]
    out = {}
    for i in range(n):
        par = parent[i]
        if par < 0:
            continue
        lp, lc = lib_of(norm(seq[par][1])), lib_of(norm(seq[i][1]))
        if cross_only and lp == lc:
            continue
        out[(seq[par][1], seq[i][1], lp, lc)] = (sub_bytes[i], sub_files[i])
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="+", help="-H outputs, one per TU; edge costs are summed over TUs")
    ap.add_argument("--top", type=int, default=40)
    ap.add_argument("--cross-lib-only", action="store_true")
    ap.add_argument("--to-lib", help="only edges whose included header is in this lib")
    a = ap.parse_args()
    agg = collections.defaultdict(lambda: [0, 0, 0])
    for f in a.files:
        for k, (b, n) in edges_of(f, a.cross_lib_only).items():
            r = agg[k]
            r[0] += b
            r[1] += n
            r[2] += 1
    rows = sorted(((v[0], v[1], v[2]) + k for k, v in agg.items()), reverse=True)
    print("%11s %7s %4s  %-9s %-9s  %s  ->  %s" % ("bytes(sum)", "files", "TUs", "from", "to", "includer", "included"))
    shown = 0
    for b, f, t, p, c, lp, lc in rows:
        if a.to_lib and lc != a.to_lib:
            continue
        print("%11d %7d %4d  %-9s %-9s  %s  ->  %s" % (b, f, t, lp, lc, short(p), short(c)))
        shown += 1
        if shown >= a.top:
            break


if __name__ == "__main__":
    main()
