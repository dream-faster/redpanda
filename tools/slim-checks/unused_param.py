#!/usr/bin/env python3
"""Report function definitions whose named parameters are never used in the body.

-Wextra -Werror makes an unused parameter fatal, and pruning a subsystem tends
to leave exactly that behind.
"""
import os
import re, subprocess, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mask2 import mask

base = subprocess.run(['git', 'rev-parse', 'upstream/v26.2.x'], capture_output=True, text=True).stdout.strip()
changed = [f for f in subprocess.run(['git', 'diff', '--name-only', base, '--', 'src/v'],
                                     capture_output=True, text=True).stdout.split()
           if f.endswith('.cc') and os.path.exists(f)]

SIG = re.compile(r'^([A-Za-z_][A-Za-z0-9_:<>,\s\*&]*?)\b([a-z_][a-z0-9_]*(?:::[a-z_][a-z0-9_]*)?)\s*\(', re.M)
PARAM = re.compile(r'\b([a-z_][a-z0-9_]{2,})\s*(?:=[^,]*)?$')


def params_of(text):
    out, depth, cur = [], 0, ''
    m = mask(text)
    for i, ch in enumerate(text):
        c = m[i]
        if c in '([{<':
            depth += 1
        elif c in ')]}>':
            depth -= 1
        if c == ',' and depth == 0:
            out.append(cur); cur = ''
            continue
        cur += ch
    if cur.strip():
        out.append(cur)
    names = []
    for p in out:
        p = p.split('=')[0].strip()
        if not p or p.endswith(('&', '*', '>')) or ' ' not in p:
            continue
        mo = re.search(r'([a-z_][a-z0-9_]{2,})\s*$', p)
        if mo and mo.group(1) not in ('const', 'auto', 'unsigned'):
            names.append(mo.group(1))
    return names


total = 0
for f in changed:
    s = open(f).read()
    m = mask(s)
    for mo in SIG.finditer(s):
        i = mo.end() - 1
        depth, close = 0, None
        for k in range(i, len(m)):
            if m[k] == '(':
                depth += 1
            elif m[k] == ')':
                depth -= 1
                if depth == 0:
                    close = k
                    break
        if close is None:
            continue
        # must be a definition: '{' shortly after the ')'
        tail = m[close + 1:close + 60]
        if '{' not in tail.split(';')[0]:
            continue
        b = m.index('{', close)
        depth, end = 0, None
        for k in range(b, len(m)):
            if m[k] == '{':
                depth += 1
            elif m[k] == '}':
                depth -= 1
                if depth == 0:
                    end = k
                    break
        if end is None:
            continue
        body = s[b:end]
        for p in params_of(s[i + 1:close]):
            if not re.search(r'\b' + re.escape(p) + r'\b', body):
                line = s.count('\n', 0, mo.start()) + 1
                print(f'{f}:{line}: parameter `{p}` unused in {mo.group(2)}()')
                total += 1
print('unused-parameter candidates:', total)
