#!/usr/bin/env python3
"""Find declarations whose return type merged with the next declaration.

Deleting `virtual T\n    name() const = 0;` by name leaves the bare `virtual T`
line, which then reads as a prefix of whatever declaration follows. The
signature is two type heads on one line with no '(' to separate them.
"""

import re, subprocess, sys

# a type head ends in one of these, and is followed by another type head
TAIL = (
    r"(?:>|\b(?:milliseconds|seconds|size_t|bool|int|int8_t|int16_t|int32_t|"
    r"int64_t|uint8_t|uint16_t|uint32_t|uint64_t|double|float|sstring|void|auto))"
)
HEAD = (
    r"(?:virtual\b|static\b|const\b|std::|ss::|absl::|model::|cluster::|config::|"
    r"kafka::|security::|storage::|chunked_|tristate<|std::optional<)"
)
RX = re.compile(TAIL + r"\s+" + HEAD)

files = [
    f
    for f in subprocess.run(
        ["git", "ls-files", "src/v"], capture_output=True, text=True
    ).stdout.split()
    if f.endswith((".cc", ".h"))
]
bad = 0
for f in files:
    for i, line in enumerate(open(f), 1):
        s = line.split("//")[0].rstrip()
        if "(" in s or ";" in s or "," in s or "=" in s or "{" in s:
            continue
        # comment bodies and trailing-return continuations are not declarations
        if s.lstrip().startswith(("*", "->", "/*")) or "->" in s:
            continue
        if s.count("virtual") > 1 or RX.search(s):
            print(f"{f}:{i}: two declaration heads on one line: {s.strip()!r}")
            bad += 1
print("merged declarations:", bad)
sys.exit(1 if bad else 0)
