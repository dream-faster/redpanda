#!/usr/bin/env python3
"""Generate the cumulative PCH ladder preludes from measured include frequency.

Reads the `-E -H` trees of the corpus (hout.sh), counts in how many TUs each
header is reached, and emits one prelude per layer containing every *public*
header (no detail/internal/impl paths) present in >= MINFRAC of the TUs:

  L1 std, L2 +fmt, L3 +boost, L4 +absl, L5 +seastar, L6 +other third-party,
  L7 +redpanda's own headers.

Each prelude includes the one below it, so one A/B per rung gives the marginal
saving of that layer if its headers were free to parse.
Usage: mkladder.py HDIR OUTDIR [minfrac=0.2]
"""
import collections, glob, os, re, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from profile_tu import lib_of

hdir, out = sys.argv[1], sys.argv[2]
minfrac = float(sys.argv[3]) if len(sys.argv) > 3 else 0.2
files = glob.glob(hdir + "/*.h")
cnt = collections.Counter()
cnt_all = collections.Counter()
for f in files:
    seen = set()
    seen_all = set()
    stack = []
    for l in open(f, errors="replace"):
        m = re.match(r"^(\.+) (\S+)$", l.rstrip("\n"))
        if not m:
            continue
        d, p = len(m.group(1)), m.group(2)
        stack = stack[: d - 1] + [p]
        seen_all.add(p)
        # public entry points only: reached from a header/TU of a *different* library
        if d == 1:
            continue
        ap = lambda x: x if x.startswith("/") else "/home/user/redpanda/" + x
        if lib_of(ap(stack[-2])) != lib_of(ap(p)) or lib_of(ap(p)) == "libc++":
            seen.add(p)
    cnt.update(seen)
    cnt_all.update(seen_all)
thr = minfrac * len(files)

def inc_name(p):
    """map an absolute header path to the include spelling used by the harness include dirs"""
    for pre, rep in (("/home/user/work/src/seastar/include/", ""), ("/home/user/work/src/boost-1.84.0/flat/", ""), ("/home/user/work/src/fmt/include/", ""),
                     ("/home/user/work/src/abseil-cpp/", ""), ("/home/user/work/src/yaml-cpp/include/", ""), ("/home/user/work/src/unordered_dense/include/", ""),
                     ("/home/user/work/src/jsoncons/include/", ""), ("/home/user/work/src/rapidjson/include/", ""), ("/home/user/work/src/CRoaring/include/", ""),
                     ("/home/user/work/src/HdrHistogram_c/include/", ""), ("/home/user/work/src/xxHash/", ""), ("/home/user/work/src/lz4/lib/", ""), ("/home/user/work/src/zstd/lib/", ""),
                     ("/home/user/redpanda/src/v/", ""), ("/usr/lib/llvm-22/bin/../include/c++/v1/", "")):
        if p.startswith(pre):
            return p[len(pre):]
    return None

layers = collections.OrderedDict((k, []) for k in ("std", "fmt", "boost", "absl", "seastar", "other3p", "redpanda"))
for p, n in list(cnt.most_common()) + [(q, m) for q, m in cnt_all.most_common() if q.startswith("src/v/") or "/redpanda/src/v/" in q]:
    if n < thr:
        continue
    if not p.startswith("/"):
        p = "/home/user/redpanda/" + p
    low = p.lower()
    if any(x in low for x in ("/detail/", "/internal/", "/impl/", ".ipp", "/__", "/gen/", "/work/gen")):
        continue
    lib = lib_of(p)
    name = inc_name(p)
    if name is None:
        continue
    if lib == "libc++":
        if "/" in name or name.startswith("__"):
            continue
        layers["std"].append(f"<{name}>")
    elif lib in layers:
        layers[lib].append(f"<{name}>" if lib != "redpanda" else f'"{name}"')
    elif lib == "other3p":
        layers["other3p"].append(f"<{name}>")
os.makedirs(out, exist_ok=True)
prev = None
for i, (lib, hs) in enumerate(layers.items(), 1):
    name = f"L{i}_{lib}.h"
    with open(os.path.join(out, name), "w") as f:
        f.write(f"// generated: cumulative PCH layer {i} ({lib}); headers present in >= {minfrac:.0%} of corpus TUs\n#pragma once\n")
        if prev:
            f.write(f'#include "{prev}"\n')
        for h in sorted(hs):
            f.write(f"#include {h}\n")
    print(name, len(hs))
    prev = name
