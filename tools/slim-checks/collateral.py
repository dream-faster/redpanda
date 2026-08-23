#!/usr/bin/env python3
"""Flag removed hunks that mention none of the subsystems we meant to remove.

A block-cut that runs past its intended end deletes neighbouring code silently;
the giveaway is a deleted run with no keyword from the removal in it.
"""
import re, subprocess, sys

KEYWORDS = re.compile(
  r'cloud|archival|s3_|iceberg|datalake|schema_registry|schema registry|pandaproxy'
  r'|transform|wasm|plugin|cluster_link|shadow_link|shadow_indexing|read_replica'
  r'|remote_|migration|migrated|recovery|manifest|bucket|upload|tiered|scrub'
  r'|inventory|offsets_upload|offsets_recover|si_|lsm|ct_|l0|l1|topic_mount'
  r'|sr_|avro|protobuf_schema|subject|compatibility', re.I)

MIN_RUN = int(sys.argv[1]) if len(sys.argv) > 1 else 6
base = subprocess.run(['git', 'rev-parse', 'upstream/v26.2.x'],
                      capture_output=True, text=True).stdout.strip()
diff = subprocess.run(['git', 'diff', '-U0', base, '--', 'src/v'],
                      capture_output=True).stdout.decode('utf-8','replace')

cur, run, start = None, [], None
def flush():
    if not run:
        return
    body = '\n'.join(run)
    if len(run) >= MIN_RUN and not KEYWORDS.search(body):
        print(f'--- {cur} @ {start}: {len(run)} deleted lines with no removal keyword')
        for l in run[:8]:
            print('   ', l[:100])

for line in diff.split('\n'):
    if line.startswith('+++ b/'):
        flush(); run.clear(); cur = line[6:]
        continue
    if line.startswith('@@'):
        flush(); run.clear()
        m = re.search(r'-(\d+)', line)
        start = m.group(1) if m else '?'
        continue
    if line.startswith('-') and not line.startswith('---'):
        run.append(line[1:])
    else:
        flush(); run.clear()
flush()
