#!/bin/bash
# Extract every compiler error from a CI run's RAW log archive.
# `gh run view --log-failed` shows only the last failing action, which makes a
# multi-error build look like a single-file failure.
set -euo pipefail
RUN="$1"
REPO="${2:-dream-faster/redpanda}"
TMP=$(mktemp -d)
gh api "/repos/$REPO/actions/runs/$RUN/logs" > "$TMP/logs.zip" 2>/dev/null
python3 - "$TMP/logs.zip" <<'PY'
import re, sys, zipfile
z = zipfile.ZipFile(sys.argv[1])
name = next(n for n in z.namelist() if n.endswith('Bazel build.txt'))
s = z.read(name).decode('utf-8', 'replace')
errs = {}
for m in re.finditer(r'(\S+\.(?:cc|h)):(\d+):(\d+): (?:fatal )?error: (.*)', s):
    errs.setdefault((m.group(1).split('_virtual_includes/')[-1], m.group(4)), m.group(2))
for (f, e), line in sorted(errs.items()):
    print(f'{f}:{line}: {e}')
print(f'\n{len(errs)} unique errors')
PY
rm -rf "$TMP"
