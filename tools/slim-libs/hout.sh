#!/bin/bash
# Dump `clang -E -H` include trees for the corpus: hout.sh OUTDIR [N]
source /home/user/work/harness/flags.sh
out=$1; n=${2:-1000}; mkdir -p $out
cd /home/user/redpanda
head -$n tools/slim-libs/corpus.txt | xargs -P4 -I{} sh -c 'f={}; '"$CXX"' '"$(echo $FLAGS | tr '\n' ' ')"' -E -H $f 2> '$out'/$(echo $f | tr / _).h >/dev/null'
