#!/usr/bin/env python3
"""Find private fields whose last reader was removed.

`-Wunused-private-field` is enabled and fatal here, and it does fire for
classes declared in headers, so a field left behind after its only reader went
away is a build error nothing else in this directory catches.

Scope matters. Pooling every identifier in the tree lets a field named
`_controller` pass because some unrelated class also has one; widening to the
whole directory has the same problem. Only the class's own methods can read a
private field, and in this codebase those live in the header or the .cc of the
same name, so that pair is the scope.

Compared against upstream so that RAII members nobody ever reads -- probes,
deferred actions, semaphore units -- do not register.
"""
import os
import re
import subprocess
import sys

# a declaration is `T _x;` or, when the type wrapped, `_x;` alone on its own
# continuation line; missing the second form makes a declaration look like a use
FIELD = re.compile(
  r'^\s{2,}(?!return|using|friend|static|typedef|//)'
  r'(?:[A-Za-z_][A-Za-z0-9_:<>,\s\*&\.]*?\s)?(_[a-z][a-z0-9_]*)\s*'
  r'(?:\{[^{}]*\}|=[^;]*)?;\s*$')
INIT = re.compile(r'^\s*[,:]\s*_[a-z][a-z0-9_]*\(')
TOKEN = re.compile(r'\b(_[a-z][a-z0-9_]*)\b')

BASE = subprocess.run(['git', 'rev-parse', 'upstream/v26.2.x'],
                      capture_output=True, text=True).stdout.strip()
headers = [f for f in subprocess.run(['git', 'ls-files', 'src/v'],
                                     capture_output=True, text=True).stdout.split()
           if f.endswith('.h')]


def read_local(f):
    try:
        return open(f).read()
    except OSError:
        return ''


def read_upstream(f):
    r = subprocess.run(['git', 'show', f'{BASE}:{f}'], capture_output=True)
    return r.stdout.decode('utf-8', 'replace') if r.returncode == 0 else ''


def used_in(read, paths):
    """Identifiers appearing outside a declaration or initializer entry."""
    out = set()
    for p in paths:
        for line in read(p).split('\n'):
            if INIT.match(line) or FIELD.match(line):
                continue
            out.update(TOKEN.findall(line))
    return out


bad = 0
for h in headers:
    src = read_local(h)
    fields = [(i, m.group(1))
              for i, line in enumerate(src.split('\n'), 1)
              if (m := FIELD.match(line))]
    if not fields:
        continue
    scope = [h, h[:-2] + '.cc']
    now = used_in(read_local, scope)
    before = used_in(read_upstream, scope)
    for line, name in fields:
        if name not in now and name in before:
            print(f'{h}:{line}: private field {name} has no remaining reader')
            bad += 1
print('unused private fields:', bad)
sys.exit(1 if bad else 0)
