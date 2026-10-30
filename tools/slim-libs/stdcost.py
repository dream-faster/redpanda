#!/usr/bin/env python3
"""CPU cost (frontend only) of including each libc++ public header on its own.

Usage: stdcost.py [--reps 3] [--extra FLAG ...]   (flags as for a TU, e.g. -nostdinc++ -isystem DIR)"""
import argparse, os, subprocess, tempfile
HARNESS = os.environ.get("HARNESS", "/home/user/work/harness")
HEADERS = """algorithm any array atomic barrier bit bitset charconv chrono codecvt compare complex concepts condition_variable coroutine
cstdint cstdio cstdlib cstring ctime deque exception execution expected filesystem format forward_list fstream functional future generator
initializer_list iomanip ios iosfwd iostream istream iterator latch limits list locale map mdspan memory memory_resource mutex new numeric optional
ostream print queue random ranges ratio regex scoped_allocator semaphore set shared_mutex source_location span spanstream sstream stack
stacktrace stdexcept stop_token streambuf string string_view strstream syncstream system_error thread tuple type_traits typeindex typeinfo
unordered_map unordered_set utility valarray variant vector version""".split()
ap = argparse.ArgumentParser()
ap.add_argument("--reps", type=int, default=3)
ap.add_argument("--extra", nargs="*", default=[])
a = ap.parse_args()
out = subprocess.check_output(["bash", "-c", f'source {HARNESS}/flags.sh; echo "$CXX"; echo "$FLAGS" | tr "\\n" " "'], text=True).splitlines()
base = [out[0], "-std=c++23", "-stdlib=libc++", "-D_LIBCPP_HARDENING_MODE=_LIBCPP_HARDENING_MODE_EXTENSIVE"] + a.extra
res = []
for h in HEADERS:
    f = tempfile.mktemp(suffix=".cc")
    open(f, "w").write(f"#include <{h}>\n")
    best = 1e9
    for _ in range(a.reps):
        p = subprocess.Popen(base + ["-fsyntax-only", f], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        _, st, ru = os.wait4(p.pid, 0)
        if st == 0:
            best = min(best, ru.ru_utime + ru.ru_stime)
    os.unlink(f)
    res.append((best, h))
for t, h in sorted(res, reverse=True):
    if t < 1e8:
        print(f"{t:6.3f}s  <{h}>")
