#!/usr/bin/env python3
"""Find private fields whose only remaining mentions are the declaration and
the constructor's member-initializer.

`-Wunused-private-field` is enabled and fatal here, and it does fire for
classes declared in headers -- removing a field's last reader leaves a build
error that nothing else in this directory catches.

A field is counted as used if it appears anywhere except its own declaration
and a `: _x(...)` / `, _x(...)` initializer entry.
"""
import re
import subprocess
import sys

# a declaration is either `T _x;` or, when the type wrapped, `_x;` alone on
# its own continuation line -- missing the second form makes the declaration
# look like a use and hides the field
FIELD = re.compile(
  r'^\s{2,}(?!return|using|friend|static|typedef|//)'
  r'(?:[A-Za-z_][A-Za-z0-9_:<>,\s\*&\.]*?\s)?(_[a-z][a-z0-9_]*)\s*'
  r'(?:\{[^{}]*\}|=[^;]*)?;\s*$')
INIT = re.compile(r'^\s*[,:]\s*_[a-z][a-z0-9_]*\(')

BASE = subprocess.run(['git', 'rev-parse', 'upstream/v26.2.x'],
                      capture_output=True, text=True).stdout.strip()

files = [f for f in subprocess.run(['git', 'ls-files', 'src/v'],
                                   capture_output=True, text=True).stdout.split()
         if f.endswith(('.cc', '.h'))]


def used_set(read):
    """Identifiers appearing outside a declaration or initializer entry."""
    out = set()
    for f in files:
        for line in read(f).split('\n'):
            if INIT.match(line) or FIELD.match(line):
                continue
            out.update(TOKEN.findall(line))
    return out

TOKEN = re.compile(r'\b(_[a-z][a-z0-9_]*)\b')


def read_local(f):
    try:
        return open(f).read()
    except OSError:
        return ''


def read_upstream(f):
    r = subprocess.run(['git', 'show', f'{BASE}:{f}'], capture_output=True)
    return r.stdout.decode('utf-8', 'replace') if r.returncode == 0 else ''


used = used_set(read_local)
# fields upstream never read either are RAII members and the like, not our doing
used_before = used_set(read_upstream)

bad = 0
for f in files:
    if not f.endswith('.h'):
        continue
    try:
        src = open(f).read()
    except OSError:
        continue
    for i, line in enumerate(src.split('\n'), 1):
        m = FIELD.match(line)
        if not m:
            continue
        name = m.group(1)
        if name not in used and name in used_before:
            print(f'{f}:{i}: private field {name} has no remaining reader')
            bad += 1
print('unused private fields:', bad)
sys.exit(1 if bad else 0)
