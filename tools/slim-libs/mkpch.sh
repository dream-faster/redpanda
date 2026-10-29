#!/bin/bash
# Build a precompiled header from pch/prelude_*.h with the harness flags.
# usage: mkpch.sh <prelude.h> <out.pch> [extra clang flags]   (env overrides as for flags.sh)
source /home/user/work/harness/flags.sh
pre=$1; out=$2; shift 2
cd /home/user/redpanda
s=$(date +%s.%N)
$CXX $FLAGS "$@" -x c++-header "$pre" -o "$out" || exit 1
e=$(date +%s.%N)
printf "pch built: %s (%.1f MB) in %.1fs wall\n" "$out" "$(echo "$(stat -c %s $out)/1048576" | bc -l)" "$(echo "$e - $s" | bc)"
