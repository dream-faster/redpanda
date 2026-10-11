#!/usr/bin/env python3
"""Verify configuration.cc ctor initializer order matches configuration.h decl order."""

import re

decl = []
for line in open("src/v/config/configuration.h"):
    m = re.search(r"\b([a-z_][a-z0-9_]*);\s*$", line)
    if m and (
        "property<" in line
        or "property;" in line
        or line.strip().startswith(
            (
                "property",
                "enum_property",
                "one_or_many",
                "bounded_property",
                "deprecated_property",
                "retention_duration_property",
                "hidden_when_default_property",
                "throughput_control",
                "config::property",
            )
        )
    ):
        decl.append(m.group(1))
    elif m and re.match(
        r"^\s+[A-Za-z_][A-Za-z0-9_:<>,\s\*&\.]*\s+[a-z_][a-z0-9_]*;\s*$", line
    ):
        decl.append(m.group(1))
src = open("src/v/config/configuration.cc").read()
i = src.index("configuration::configuration()")
j = src.index("\n}\n", i)
entries = re.findall(r"^  [:,] ([A-Za-z_][A-Za-z0-9_]*)\(", src[i:j], re.M)
pos = {n: k for k, n in enumerate(decl)}
last, bad = -1, []
for e in entries:
    p = pos.get(e)
    if p is None:
        continue
    if p < last:
        bad.append(e)
    last = p
print(f"ctor entries: {len(entries)}, out of order: {len(bad)}")
for b in bad[:15]:
    print("   ", b)
