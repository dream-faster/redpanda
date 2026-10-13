#!/usr/bin/env python3
"""Summarize a Bazel JSON trace profile (--profile=...json.gz).

Prints wall time, the critical path, and where action time went, grouped
by mnemonic, by top-level package (src/v/<dir> or external/<repo>) and by
individual action. Used by .github/workflows/build-timing.yml to compare
cold builds of two revisions.
"""

import argparse
import collections
import gzip
import json
import re


def load(path):
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt") as f:
        data = json.load(f)
    return data["traceEvents"] if isinstance(data, dict) else data


def bucket(name):
    """Group an action by what it builds: src/v/<dir>, or for third-party
    code the first path components (action names carry paths relative to
    the external repo, e.g. "src/google/protobuf/..." or "absl/...")."""
    tool = " (tool)" if "[for tool]" in name else ""
    m = re.match(r"Compiling (\S+)", name)
    if not m:
        return "non-compile" + tool
    path = m.group(1)
    if path.startswith("src/v/"):
        return "/".join(path.split("/")[:3]) + tool
    if path.startswith("external/"):
        return "/".join(path.split("/")[:2]) + tool
    return "third-party: " + "/".join(path.split("/")[:2]) + tool


def main():
    p = argparse.ArgumentParser()
    p.add_argument("profile")
    p.add_argument("--top", type=int, default=40)
    args = p.parse_args()

    events = load(args.profile)
    actions = []
    critical = []
    t_min, t_max = None, None
    for e in events:
        if e.get("ph") != "X":
            continue
        ts, dur = e.get("ts", 0), e.get("dur", 0)
        t_min = ts if t_min is None else min(t_min, ts)
        t_max = ts + dur if t_max is None else max(t_max, ts + dur)
        cat = e.get("cat", "")
        a = e.get("args", {}) or {}
        if cat == "critical path component":
            critical.append((dur, e.get("name", "")))
        elif cat == "action processing":
            actions.append((dur, e.get("name", ""), a.get("mnemonic", "?")))

    us = 1e6
    print(f"profile span: {(t_max - t_min) / us:.0f}s")
    total = sum(a[0] for a in actions)
    print(f"actions: {len(actions)}, summed action time: {total / us:.0f}s "
          f"({total / us / 3600:.2f} CPU-ish hours)")

    if critical:
        ctot = sum(c[0] for c in critical)
        print(f"\n== critical path: {ctot / us:.0f}s ==")
        for dur, name in sorted(critical, reverse=True)[:args.top]:
            print(f"{dur / us:8.1f}s  {name}")

    by_mn = collections.Counter()
    n_mn = collections.Counter()
    by_pkg = collections.Counter()
    n_pkg = collections.Counter()
    for dur, name, mn in actions:
        by_mn[mn] += dur
        n_mn[mn] += 1
        b = bucket(name)
        by_pkg[b] += dur
        n_pkg[b] += 1

    print("\n== by mnemonic ==")
    for k, v in by_mn.most_common(args.top):
        print(f"{v / us:9.0f}s {100 * v / total:5.1f}% {n_mn[k]:6d}  {k}")

    print("\n== by package ==")
    for k, v in by_pkg.most_common(args.top * 2):
        print(f"{v / us:9.0f}s {100 * v / total:5.1f}% {n_pkg[k]:6d}  {k}")

    print("\n== slowest actions ==")
    for dur, name, mn in sorted(actions, reverse=True)[:args.top * 2]:
        print(f"{dur / us:8.1f}s  {mn:14s} {name}")


if __name__ == "__main__":
    main()
