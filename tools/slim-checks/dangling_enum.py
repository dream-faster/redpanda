#!/usr/bin/env python3
"""Find enum-qualified identifiers (Type::name) whose enumerator no longer exists."""

import re, subprocess, collections

files = [
    f
    for f in subprocess.run(
        ["git", "ls-files", "src/v"], capture_output=True, text=True
    ).stdout.split()
    if f.endswith((".cc", ".h"))
]
# collect enum class bodies
enums = {}
for f in files:
    try:
        s = open(f).read()
    except OSError:
        continue
    for m in re.finditer(r"enum class ([A-Za-z_][A-Za-z0-9_]*)[^{;]*\{([^}]*)\}", s):
        name, body = m.group(1), m.group(2)
        body = re.sub(r"//[^\n]*", "", body)
        vals = set(re.findall(r"([A-Za-z_][A-Za-z0-9_]*)\s*(?:=[^,]*)?(?:,|$)", body))
        enums.setdefault(name, set()).update(vals)
missing = collections.Counter()
where = {}
for f in files:
    try:
        s = open(f).read()
    except OSError:
        continue
    for m in re.finditer(r"\b([a-z_][a-z0-9_]*)::([a-z_][a-z0-9_]*)\b", s):
        t, v = m.group(1), m.group(2)
        if t in enums and v not in enums[t]:
            missing[(t, v)] += 1
            where.setdefault((t, v), f)
print(f"dangling enum refs: {len(missing)}")
for (t, v), c in missing.most_common(25):
    print(f"   {t}::{v}  x{c}  e.g. {where[(t, v)]}")
