#!/usr/bin/env python3
"""Report BUILD file references that name a file which no longer exists.

Bazel's analysis phase resolves labels but does not stat their inputs, so a
rule left pointing at a deleted source analyses clean and only fails at build.
Covers srcs/hdrs/src/out/definitions/data, i.e. every attribute the removal
passes actually touched.
"""

import glob, os, re, sys

ATTRS = (
    "srcs",
    "hdrs",
    "src",
    "out",
    "outs",
    "definitions",
    "data",
    "textual_hdrs",
    "implementation_hdrs",
)
bad = 0
for build in glob.glob("src/**/BUILD", recursive=True):
    pkg = os.path.dirname(build)
    s = open(build).read()
    for attr in ATTRS:
        for m in re.finditer(r"\b" + attr + r'\s*=\s*(\[[^\]]*\]|"[^"]*")', s, re.S):
            for name in re.findall(r'"([^"]+)"', m.group(1)):
                if name.startswith(("//", "@", ":")) or "$" in name:
                    continue
                # generated outputs and codegen results never exist on disk
                if name.endswith(
                    (
                        ".json.hh",
                        ".pb.h",
                        ".pb.cc",
                        ".proto.h",
                        ".proto.cc",
                        "_service.h",
                        ".h.inc",
                    )
                ):
                    continue
                if attr in ("out", "outs"):
                    continue
                if not os.path.exists(os.path.join(pkg, name)):
                    line = s.count("\n", 0, m.start()) + 1
                    print(f"{build}:{line}: {attr} names missing file {name!r}")
                    bad += 1
print("missing BUILD inputs:", bad)
sys.exit(1 if bad else 0)
