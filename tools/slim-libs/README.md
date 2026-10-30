# slim-libs: how much compile time do slimmer dependencies buy?

Experiment tooling (work in progress). It compiles a fixed corpus of `src/v`
translation units (`corpus.txt`) against variants of the third-party headers
and compares CPU time with paired A/B runs.

- `ab.py`: paired A/B compile-time comparison (rusage CPU time, bootstrap CI)
- `bench.py`, `compare.py`: single-config runs and comparison
- `profile_tu.py`, `profile_all.py`, `hdr_report.py`, `edge_cost.py`, `hout.sh`:
  `-ftime-trace` / `-H` attribution of time to libraries, headers and include edges
- `libbuild.py`: cold compile cost of a dependency's own sources
- `micro.py`, `micro/`, `stdcost.py`: cost of single headers
- `pch/`, `mkpch.sh`: cumulative precompiled-header ladder (ceiling per layer)
- `patches/`: the slim variants (Seastar, fmt, abseil, libc++ ostream/chrono)

The harness expects the pinned dependency sources and clang-22 under
`/home/user/work` (see `flags.sh` conventions in the scripts); it does not use
Bazel. Results and the write-up will follow in `docs/build-time/`.
