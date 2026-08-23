#!/usr/bin/env python3
"""Compare brace and paren balance of every tracked C++ file against upstream."""
import re, subprocess, sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mask2 import balance, mask

base = subprocess.run(['git','rev-parse','upstream/v26.2.x'],capture_output=True,text=True).stdout.strip()
files = subprocess.run(['git','ls-files','src/v'],capture_output=True,text=True).stdout.split()
files = [f for f in files if f.endswith(('.cc','.h'))]
bad = []
for f in files:
    try:
        cur = open(f).read()
    except OSError:
        continue
    b = balance(cur)
    if b == (0,0):
        continue
    up = subprocess.run(['git','show',f'{base}:{f}'],capture_output=True,text=True)
    ref = balance(up.stdout) if up.returncode == 0 else (0,0)
    if b != ref:
        bad.append((f,b,ref))
print(f'balance regressions: {len(bad)}')
for f,b,r in bad[:20]:
    print('   ',f,b,'upstream',r)
