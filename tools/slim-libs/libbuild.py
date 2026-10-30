#!/usr/bin/env python3
"""Measure the cold compile cost of a third-party library's own sources.

Axis 1 of the experiment: not "how long do Redpanda TUs take with slimmer
headers" but "how long does building the dependency itself take, and how much
of it is needed".  Each source is compiled to an object with the same clang and
language flags the Bazel build uses; CPU time (user+sys) comes from rusage.

Usage: libbuild.py --lib seastar [--opt -O0|-O2] [-j 4] [--list FILE] [--name TAG]
       libbuild.py --lib seastar --exclude 'src/(websocket|rpc)/'   # slim subset
Results: $RESULTS/lib_<lib>_<tag>.json  {src: cpu_seconds}
"""
import argparse, json, os, re, subprocess, sys, tempfile
from concurrent.futures import ThreadPoolExecutor

HARNESS = os.environ.get("HARNESS", "/home/user/work/harness")
RESULTS = os.environ.get("RESULTS", "/home/user/work/results")
S = "/home/user/work/src"

COMMON = ["-std=c++23", "-stdlib=libc++", "-march=westmere", "-fno-omit-frame-pointer", "-fstack-protector", "-U_FORTIFY_SOURCE",
          "-D_LIBCPP_HARDENING_MODE=_LIBCPP_HARDENING_MODE_EXTENSIVE", "-w", "-fPIC"]

# lib -> (root dir, include dirs relative to root or absolute, defines, sources: callable -> list of paths relative to root)
def seastar_srcs():
    return [l.strip() for l in open("/home/user/work/out/seastar_srcs.txt") if l.strip()]

LIBS = {
    "seastar": dict(
        root=f"{S}/seastar",
        inc=["include", "src", "/home/user/work/gen/seastar/proto", "/home/user/work/gen", f"{S}/boost-1.84.0/flat", f"{S}/fmt/include",
             f"{S}/abseil-cpp", f"{S}/protobuf/src", f"{S}/protobuf/third_party/utf8_range", f"{S}/unordered_dense/include", f"{S}/yaml-cpp/include",
             f"{S}/lz4/lib"],
        defs=["BOOST_TEST_DYN_LINK", "BOOST_TEST_NO_LIB", "SEASTAR_API_LEVEL=10", "SEASTAR_HAS_MEMBARRIER", "SEASTAR_SCHEDULING_GROUPS_COUNT=21",
              "SEASTAR_HAVE_OPENSSL", "SEASTAR_SSTRING", "SEASTAR_DEBUG", "SEASTAR_DEBUG_PROMISE", "SEASTAR_DEBUG_SHARED_PTR", "SEASTAR_TYPE_ERASE_MORE",
              "SEASTAR_BUILD_SHARED_LIBS", "SEASTAR_THREAD_STACK_GUARDS", "SEASTAR_DEFERRED_ACTION_REQUIRE_NOEXCEPT", "SEASTAR_HAVE_HWLOC",
              "SEASTAR_HAVE_URING", "SEASTAR_HEAPPROF"],
        srcs=seastar_srcs,
    ),
}


def listfile(path):
    return lambda: [l.strip() for l in open(path) if l.strip()]


def boost_srcs(lib):
    def f():
        d = f"{S}/boost-1.84.0/libs/{lib}/src"
        out = []
        for r, _, fs in os.walk(d):
            for x in fs:
                if x.endswith((".cpp", ".cc")):
                    out.append(os.path.relpath(os.path.join(r, x), f"{S}/boost-1.84.0"))
        return sorted(out)
    return f


PB_INC = ["src", "third_party/utf8_range", f"{S}/abseil-cpp"]
LIBS["protobuf_full"] = dict(root=f"{S}/protobuf", inc=PB_INC, defs=[], srcs=listfile("/home/user/work/out/protobuf_full_all.txt"))
LIBS["protobuf_lite"] = dict(root=f"{S}/protobuf", inc=PB_INC, defs=[], srcs=listfile("/home/user/work/out/protobuf_lite.txt"))
LIBS["absl"] = dict(root=f"{S}/abseil-cpp", inc=["."], defs=[], srcs=listfile("/home/user/work/out/absl_srcs.txt"))
for _b in ("filesystem", "program_options", "thread", "test", "container", "iostreams", "system", "atomic", "chrono"):
    LIBS["boost_" + _b] = dict(root=f"{S}/boost-1.84.0", inc=["flat"], defs=["BOOST_ALL_NO_LIB", "BOOST_THREAD_BUILD_LIB", "BOOST_FILESYSTEM_SOURCE"] if _b in ("filesystem", "thread") else ["BOOST_ALL_NO_LIB"], srcs=boost_srcs(_b))


def compile_one(cxx, flags, root, src, outdir):
    obj = tempfile.mktemp(suffix=".o", dir=outdir)
    cmd = [cxx] + flags + ["-c", os.path.join(root, src), "-o", obj]
    p = subprocess.Popen(cmd, cwd=root, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    err = p.stderr.read()
    _, status, ru = os.wait4(p.pid, 0)
    if os.path.exists(obj):
        os.unlink(obj)
    rc = os.waitstatus_to_exitcode(status)
    return src, ((ru.ru_utime + ru.ru_stime) if rc == 0 else None), err.decode(errors="replace")[:300]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lib", required=True)
    ap.add_argument("--opt", default="-O0")
    ap.add_argument("--tag", default="")
    ap.add_argument("-j", type=int, default=4)
    ap.add_argument("--exclude", default=None)
    ap.add_argument("--inc-prepend", action="append", default=[], help="extra include dir placed first (e.g. a slim fork)")
    ap.add_argument("--root", default=None)
    ap.add_argument("--extra", action="append", default=[])
    a = ap.parse_args()
    L = LIBS[a.lib]
    root = a.root or L["root"]
    inc = a.inc_prepend + [i if i.startswith("/") else os.path.join(root, i) for i in L["inc"]]
    flags = COMMON + [a.opt, "-g0"] + [f"-isystem{i}" for i in inc] + [f"-D{d}" for d in L["defs"]] + a.extra
    srcs = L["srcs"]()
    if a.exclude:
        rx = re.compile(a.exclude)
        srcs = [s for s in srcs if not rx.search(s)]
    out = tempfile.mkdtemp()
    with ThreadPoolExecutor(a.j) as ex:
        rows = list(ex.map(lambda s: compile_one("clang++-22", flags, root, s, out), srcs))
    res = {s: t for s, t, _ in rows}
    failed = [(s, e) for s, t, e in rows if t is None]
    tot = sum(t for t in res.values() if t is not None)
    tag = a.tag or a.opt
    os.makedirs(RESULTS, exist_ok=True)
    json.dump(res, open(f"{RESULTS}/lib_{a.lib}_{tag}.json", "w"), indent=1)
    print(f"{a.lib} {tag}: {len(res)} sources, {len(failed)} failed, total CPU {tot:.1f}s")
    for s, e in failed[:5]:
        print("  FAILED", s, e.strip().splitlines()[-1] if e.strip() else "")


if __name__ == "__main__":
    main()
