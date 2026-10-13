#!/usr/bin/env python3
"""Aggregate clang -ftime-trace JSON files across a build.

For every header this reports the parse time it cost summed over all
translation units, counting each header once per TU at its first
inclusion (with #pragma once that is the only time it is parsed). The
time is inclusive: it covers the headers first pulled in through it.
Each cost is also attributed to the header that included it, which is
what you want when deciding which #include to remove.

Also reports the most expensive template instantiations (outermost only,
so nested instantiations are not double counted) and the slowest TUs.
With --json, writes the aggregates for later querying.
"""

import argparse
import collections
import json
import os
import sys


def norm(path):
    for marker in ("external/", "src/v/", "bazel-out/"):
        i = path.find(marker)
        if i >= 0:
            return path[i:]
    return path


def source_spans(events):
    """Yield (start, dur, path, parent_path) for Source events.

    Newer clang emits Source as async begin/end ("b"/"e") pairs, older
    clang as complete ("X") events; both nest like the include stack.
    """
    spans = []
    for e in events:
        if e.get("name") != "Source":
            continue
        ph = e.get("ph")
        detail = (e.get("args") or {}).get("detail", "")
        if ph == "X":
            spans.append((e["ts"], e["ts"] + e.get("dur", 0), norm(detail)))
        elif ph == "b":
            spans.append([e["ts"], None, norm(detail)])
        elif ph == "e":
            # Close the innermost open span.
            for s in reversed(spans):
                if isinstance(s, list) and s[1] is None:
                    s[1] = e["ts"]
                    break
    spans = [tuple(s) for s in spans if s[1] is not None]
    spans.sort(key=lambda s: (s[0], -s[1]))
    stack = []
    for start, end, path in spans:
        while stack and stack[-1][1] <= start:
            stack.pop()
        parent = stack[-1][2] if stack else "<tu>"
        yield start, end - start, path, parent
        stack.append((start, end, path))


def outermost(events, names):
    spans = sorted(
        (
            (e["ts"], e["ts"] + e.get("dur", 0), e)
            for e in events
            if e.get("name") in names and e.get("ph") == "X"
        ),
        key=lambda s: (s[0], -s[1]),
    )
    end = -1
    for start, stop, e in spans:
        if start >= end:
            end = stop
            yield e


def main():
    p = argparse.ArgumentParser()
    p.add_argument("root")
    p.add_argument("--top", type=int, default=80)
    p.add_argument("--json", help="write aggregates here")
    args = p.parse_args()

    hdr = collections.Counter()
    hdr_n = collections.Counter()
    edge = collections.Counter()
    edge_n = collections.Counter()
    inst = collections.Counter()
    inst_n = collections.Counter()
    tus = []
    frontend = backend = 0
    for dirpath, _, files in os.walk(args.root):
        for f in files:
            if not f.endswith(".json"):
                continue
            path = os.path.join(dirpath, f)
            try:
                with open(path) as fh:
                    data = json.load(fh)
            except (OSError, ValueError):
                continue
            if not isinstance(data, dict) or "traceEvents" not in data:
                continue
            events = data["traceEvents"]
            seen = set()
            for _, dur, h, parent in source_spans(events):
                if h in seen:
                    continue
                seen.add(h)
                hdr[h] += dur
                hdr_n[h] += 1
                edge[(parent, h)] += dur
                edge_n[(parent, h)] += 1
            for e in outermost(events, ("InstantiateClass", "InstantiateFunction")):
                d = (e.get("args") or {}).get("detail", "")[:240]
                inst[d] += e.get("dur", 0)
                inst_n[d] += 1
            total = 0
            for e in events:
                n = e.get("name")
                if n == "Total Frontend":
                    frontend += e.get("dur", 0)
                elif n == "Total Backend":
                    backend += e.get("dur", 0)
                elif n == "Total ExecuteCompiler":
                    total = e.get("dur", 0)
            tus.append((total, norm(path)))

    us = 1e6
    print(f"translation units: {len(tus)}")
    print(f"frontend: {frontend / us:.0f}s  backend: {backend / us:.0f}s")

    print("\n== headers: inclusive parse time at first inclusion, summed over TUs ==")
    for h, v in hdr.most_common(args.top):
        print(f"{v / us:8.0f}s {hdr_n[h]:5d}  {h}")

    print("\n== src/v headers ==")
    rp = [(h, v) for h, v in hdr.items() if h.startswith("src/v/")]
    for h, v in sorted(rp, key=lambda x: -x[1])[: args.top]:
        print(f"{v / us:8.0f}s {hdr_n[h]:5d}  {h}")

    print("\n== include edges (includer -> header) ==")
    for (a, b), v in edge.most_common(args.top):
        print(f"{v / us:8.0f}s {edge_n[(a, b)]:5d}  {a} -> {b}")

    print("\n== template instantiations (outermost) ==")
    for k, v in inst.most_common(args.top // 2):
        print(f"{v / us:8.0f}s {inst_n[k]:6d}  {k}")

    print("\n== slowest TUs ==")
    for dur, path in sorted(tus, reverse=True)[: args.top // 2]:
        print(f"{dur / us:8.1f}s  {path}")

    if args.json:
        with open(args.json, "w") as fh:
            json.dump(
                {
                    "headers": {h: [v, hdr_n[h]] for h, v in hdr.items()},
                    "edges": [
                        [a, b, v, edge_n[(a, b)]]
                        for (a, b), v in edge.items()
                        if v > 100000
                    ],
                    "instantiations": {
                        k: [v, inst_n[k]] for k, v in inst.most_common(2000)
                    },
                    "tus": tus,
                },
                fh,
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
