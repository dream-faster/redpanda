#!/usr/bin/env python3
"""Report quoted includes that name a file which no longer exists.

Deleting a subsystem leaves `#include "cloud_storage/foo.h"` behind in files
that survived it. Bazel's analysis phase never opens a source file, so this
only surfaces as a `fatal error: file not found` partway through a build.

Generated headers are skipped: codegen writes them into bazel-out, so they are
correctly absent from the source tree.
"""
import os
import re
import subprocess
import sys

# `_service.h` as a blanket suffix also swallows hand-written headers such as
# inventory_service.h, so the generated set is derived from the BUILD files
# that actually produce these headers.
GENERATED_SUFFIX = ('.json.hh', '.pb.h', '.pb.cc', '.proto.h', '.proto.cc',
                    '.h.inc')


def generated_headers():
    """Header basenames produced by codegen rules rather than checked in."""
    out = set()
    for build in subprocess.run(['git', 'ls-files', '--', '*BUILD'],
                                capture_output=True, text=True).stdout.split():
        try:
            b = open(build).read()
        except OSError:
            continue
        out.update(re.findall(r'out = "([^"]+)"', b))
        # an rpc library names its header after its .json source, not the
        # target, unless an explicit `out` overrides it
        for m in re.finditer(r'src = "([^"]+\.json)"', b):
            out.add(os.path.basename(m.group(1))[:-len('.json')] + '_service.h')
            out.add(os.path.basename(m.group(1)) + '.hh')
    return out
# third-party and toolchain roots resolve outside the repo
EXTERNAL = ('seastar/', 'absl/', 'boost/', 'fmt/', 'rapidjson/', 'openssl/',
            'hdr/', 'google/', 'gtest/', 'gmock/', 'snappy', 'avro/', 'arrow/',
            'parquet/', 'proto/', 'xxhash', 'crc32', 'base64', 'ada/', 're2/',
            'roaring', 'tools/', 'src/v/', 'kafka/protocol/schemata/',
            'serde/test/', 'aws/', 'azure/', 'zstd', 'lz4')

tracked = set(subprocess.run(['git', 'ls-files'], capture_output=True,
                             text=True).stdout.split())
generated = generated_headers()
files = [f for f in tracked if f.startswith('src/v/') and f.endswith(('.cc', '.h'))]

bad = 0
for f in files:
    try:
        src = open(f).read()
    except OSError:
        continue
    for i, line in enumerate(src.split('\n'), 1):
        m = re.match(r'#include "([^"]+)"', line.strip())
        if not m:
            continue
        inc = m.group(1)
        if (inc.startswith(EXTERNAL) or inc.endswith(GENERATED_SUFFIX)
                or os.path.basename(inc) in generated):
            continue
        # resolvable from the include root or relative to the including file
        for cand in ('src/v/' + inc, os.path.join(os.path.dirname(f), inc)):
            if cand in tracked or os.path.exists(cand):
                break
        else:
            print(f'{f}:{i}: includes missing file {inc!r}')
            bad += 1
print('includes of deleted files:', bad)
sys.exit(1 if bad else 0)
