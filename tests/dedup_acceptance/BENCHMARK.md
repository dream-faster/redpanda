# What dedup costs

Two measurements, both reproducible from this repository: one per record
inside the broker, one end to end through the Kafka API.

## Per record: `dedup_window_filter_rpbench`

```
bazel run --config=release //src/v/cluster/tests:dedup_window_filter_rpbench
```

drives `cluster::dedup_window_filter` directly, the way `dedup_stm` calls it.
The `unit-tests` workflow runs it after the tests and uploads the output as
the `dedup-rpbench` artifact. Figures below are from that run on
`a5879f2f` (one core of a `ubuntu-latest` runner, release build; 128
records of a 16-byte key and a 128-byte value per batch; the index at 60 s
window unless stated).

| path | per record | what it is |
|---|---:|---|
| `dedup_produce_path.dedup_disabled_baseline` | 3 ns | moving the batch through with dedup off |
| `dedup_produce_path.filter_all_new_keys` | 176 ns | the leader's `filter_request()`, every key new: digest, insert, sweep step |
| `dedup_produce_path.filter_all_new_header_identities` | 284 ns | the same with the identity in the second of three headers |
| `dedup_produce_path.filter_all_duplicates` | 62 ns | every key already indexed: lookup hit, record dropped |
| `dedup_apply_path.identity_walk_only` | 84 ns | reading the identities out of a batch, nothing else |
| `dedup_apply_path.record_parse_only` | 148 ns | what the apply path parsed before: a `model::record` per record (4 allocations) |
| `dedup_apply_path.identity_walk_then_populate` | 160 ns | a follower's apply path for a committed batch |
| `dedup_apply_path.record_parse_then_populate` | 230 ns | the apply path before the identity walk |
| `dedup_apply_path.header_identity_walk_then_populate` | 274 ns | a follower's apply path in header mode |
| `dedup_index.insert_at_the_ceiling` | 79 ns | a new identity into an index at its ceiling: the bounded sweep steps and the capacity bookkeeping |
| `dedup_snapshot.serialize_1m_entries` | 4 ns | the snapshot write, per entry, yielding between chunks |
| `dedup_snapshot.restore_1m_entries` | 133 ns | restoring that snapshot into an empty index, per entry |

What changed in this PR, per record:

- The apply path dropped from 230 ns to 160 ns (and from 4 allocations to
  none) by reading identities straight out of the batch instead of parsing
  each record into a `model::record`.
- The leader no longer applies the batches it filtered, so a leader pays
  the 176 ns filter once rather than filter plus a 230 ns apply.
- The snapshot write yields: its cost is unchanged at 4 ns an entry, but the
  longest stretch it holds the reactor is one 4096-entry chunk instead of the
  whole index (hundreds of milliseconds at the shard limit, on every segment
  roll).

The index itself takes at most about 31 bytes an entry, down from 50 with the
general-purpose hash map it used before (measured by
`dedup_snapshot_stall_test` at several sizes, which checks that the index's
own count plus the 14-byte snapshot entry fits the 44 bytes per entry the
shard budget turns memory into entries with).

## End to end: `test_bench.py`

```
pytest test_bench.py --bench --image <image> [--bench-out results.md]
```

produces the same load into a topic with and without dedup on one broker
with one core and reports the client's records/s, the broker's reactor CPU
per million records (from its own metrics, so independent of the client),
reactor stalls, and records written. 2,000,000 records of 100 bytes per
scenario from 2 producer processes, 4 partitions, acks=all; median of 3
interleaved runs. Both tables were produced on the same machine, one after
the other.

### Before: the deployed build (`085657d`)

| scenario | records/s | vs baseline | broker CPU ms / 1M records | vs baseline | written | stalls | note |
|---|---:|---:|---:|---:|---:|---:|---|
| baseline | 767,737 | +0% | 432 | +0% | 2,000,000 | 8 | no dedup |
| key, unique | 714,903 | -7% | 1,096 | +154% | 2,000,000 | 101 | every record a new identity |
| key, 50% duplicates | 736,960 | -4% | 1,086 | +152% | 1,000,000 | 58 | each key sent twice; the second is dropped |
| header, unique | 515,710 | -33% | 1,524 | +253% | 2,000,000 | 77 | identity in a header |
| baseline, lz4 | 728,759 | +0% | 273 | +0% | 2,000,000 | 3 |  |
| key, unique, lz4 | 763,383 | +5% | 926 | +239% | 2,000,000 | 67 | batches are decompressed to read identities |

### After: this PR (`a5879f2f`)

| scenario | records/s | vs baseline | broker CPU ms / 1M records | vs baseline | written | stalls | note |
|---|---:|---:|---:|---:|---:|---:|---|
| baseline | 812,329 | +0% | 408 | +0% | 2,000,000 | 6 | no dedup |
| key, unique | 736,726 | -9% | 820 | +101% | 2,000,000 | 29 | every record a new identity |
| key, 50% duplicates | 759,728 | -6% | 880 | +116% | 1,000,000 | 34 | each key sent twice; the second is dropped |
| header, unique | 540,115 | -34% | 1,045 | +156% | 2,000,000 | 61 | identity in a header |
| baseline, lz4 | 829,625 | +0% | 234 | +0% | 2,000,000 | 2 |  |
| key, unique, lz4 | 811,047 | -2% | 607 | +160% | 2,000,000 | 22 | batches are decompressed to read identities |
| key, unique, shard full | 766,702 | -6% | 744 | +83% | 2,000,000 | 8 | index at the shard limit the whole run: every insert trims |

### Reading the two tables

The number to compare is the broker CPU dedup adds over its own baseline,
per million records (the baselines themselves move a little between the
two runs; the client's records/s is bounded by the two producer processes,
not the broker):

| scenario | before | after | change |
|---|---:|---:|---:|
| key, unique | +664 ms | +412 ms | -38% |
| key, 50% duplicates | +654 ms | +472 ms | -28% |
| header, unique | +1,092 ms | +637 ms | -42% |
| key, unique, lz4 | +653 ms | +373 ms | -43% |
| reactor stalls, key unique | 101 | 29 | -71% |

So dedup now costs about 0.4 µs of broker CPU per record on the leader with
a two-million-entry index, down from 0.66 µs, and 0.34 µs with the index
held at 200,000 entries (the shard-full row, where every insert also trims).
That difference is the cost of the index's size itself: a two-million-entry
map does not fit in cache, so each lookup is a memory access, and that is
the largest remaining component. The per-record work the broker does over
and above the lookup is in the microbenchmark table above.

Header mode remains the most expensive identity source: the client sends
larger records and the walk has to read past the key to reach the header.
Compressed batches are decompressed to read their identities on every
replica, which is what puts the lz4 overhead above the plain one in
relative terms.

### Replicated, and in the steady state

The `rf3` scenarios run three brokers with one core each, every partition on
all three, and sum the CPU of all three brokers: what dedup costs a
replicated topic is the leader's filter plus every follower's apply. The
`2 s window` ones keep the index in its steady state, entries expiring as
fast as new identities arrive, which is where a long-running topic spends
its life and where the expiry sweep is paid for.

Each node's data directory is created under `$TMPDIR`. On ext4 the brokers
spend more of their CPU submitting disk writes than on XFS, the filesystem
production runs on: the no-dedup `rf3 baseline` cost a tenth to a sixth
less per record with `TMPDIR` on an XFS mount. Point it at one for numbers
closer to a deployment's.
