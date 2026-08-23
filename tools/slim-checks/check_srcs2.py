#!/usr/bin/env python3
"""Report BUILD srcs/hdrs entries that are missing HERE but present upstream.

Generated outputs (api-doc *.json.hh, *_rpc_service.h) never exist on disk, so
an absolute check is all false positives; comparing against the upstream tree
isolates the entries this branch actually orphaned.
"""

import os
import re
import subprocess

ENTRY = re.compile(r'^\s*"([A-Za-z0-9_./-]+\.(?:cc|h|hh|json|proto|inc))",\s*$')
BLOCK = re.compile(r"^\s*(srcs|hdrs|textual_hdrs)\s*=\s*\[\s*$")


def entries(text, d):
    out, inblock = [], False
    for line in text.split("\n"):
        if BLOCK.match(line):
            inblock = True
            continue
        if inblock:
            if line.strip().startswith("]"):
                inblock = False
                continue
            m = ENTRY.match(line)
            if m:
                out.append(os.path.join(d, m.group(1)))
    return out


tracked = set(
    subprocess.run(["git", "ls-files"], capture_output=True, text=True).stdout.split()
)
up = set(
    subprocess.run(
        ["git", "ls-tree", "-r", "--name-only", "upstream/v26.2.x"],
        capture_output=True,
        text=True,
    ).stdout.split()
)
builds = subprocess.run(
    ["git", "ls-files", "--", "*BUILD"], capture_output=True, text=True
).stdout.split()

bad = []
for b in builds:
    d = os.path.dirname(b)
    for f in entries(open(b).read(), d):
        if f in tracked or os.path.exists(f):
            continue
        # only a problem if upstream had the file (i.e. we deleted it)
        if f in up:
            bad.append((b, f))
for b, f in bad:
    print(f"  ORPHANED {b}: {f}")
print(f"orphaned source refs: {len(bad)}")
