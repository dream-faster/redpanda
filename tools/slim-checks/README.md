# slim-checks

Consistency checks for large subsystem removals.

Bazel's analysis phase (`bazel build --nobuild //...`) validates the build
graph but does not typecheck C++ and does not stat rule inputs, so a removal
can leave the tree badly broken while analysis stays green. Each script here
exists because a specific class of breakage escaped to CI and cost a ~35 minute
build round-trip.

Run from the repository root.

## Reading CI failures

`gh run view <id> --log-failed` shows only the **last** failing action, which
makes a build with twenty errors look like a single-file failure and sends you
chasing one file per round-trip. Use the raw log archive instead:

```
tools/slim-checks/ci-errors.sh <run-id> [owner/repo]
```

It downloads the run's log zip and prints every unique compiler error.

## Pass/fail checks

These print a count and exit non-zero when it is not zero.

| Script | Catches |
|---|---|
| `check_files.py` | BUILD rules naming a file that no longer exists — analysis resolves the label without stat'ing it, so the rule only fails at build time |
| `check_srcs2.py` | `srcs`/`hdrs` entries missing locally but present upstream |
| `check_merged_decls.py` | a deleted declaration's return type fusing into the next declaration (`virtual T virtual U`) |
| `sweep.py` | brace/paren imbalance, compared against upstream so pre-existing imbalances (raw string literals) do not register |
| `ctor_order.py` | `configuration.cc`'s initializer order drifting from the header, which `-Wreorder` rejects |
| `check_unused_fields.py` | private fields whose last reader was removed — `-Wunused-private-field` is enabled and fatal, and it *does* fire for classes declared in headers |

## Advisory scans

These report raw counts with false positives; they are only meaningful when
you diff the result against the same script run on the upstream file. See the
git history of this directory for the wrapper used to do that.

| Script | Reports |
|---|---|
| `fmt_arity.py` | `fmt::format_to`/`vlog` calls whose `{}` count differs from the argument count — removing a logged field means deleting both halves |
| `unused_param.py` | named parameters unused in a function body, which `-Wextra -Werror` rejects |
| `collateral.py N` | deleted runs of N+ lines that mention none of the removal's keywords, i.e. a block cut that ran past its intended end |
| `dangling_enum.py` | `Type::value` references whose enumerator no longer exists |

`mask2.py` is the shared helper: it blanks string and comment contents so the
others can scan structure without being confused by braces inside literals. It
deliberately ignores single quotes, because apostrophes in prose corrupted an
earlier version.
