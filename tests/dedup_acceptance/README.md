# Dedup acceptance tests

Black-box tests for produce-time deduplication (`redpanda.dedup.window.ms`,
`redpanda.dedup.key.header`). They run real clusters of a broker image as
docker containers and use only what a user has: the Kafka API (through
librdkafka), the admin API, metrics, and the data directory on disk.

```
pip install -r requirements.txt
pytest --image <image-under-test> [--old-image <earlier-dedup-build>] [--require-new-build]
```

* `--image` (or `DEDUP_IMAGE`) is the build under test.
* `--old-image` (or `DEDUP_OLD_IMAGE`) is an earlier dedup build to upgrade
  from, downgrade to, and run mixed with. Without it the upgrade tests skip.
  CI uses the deployed build,
  `ghcr.io/dream-faster/redpanda:085657d38360db6808462a096274b4aabb23b1df`.
* Some tests need behaviour only the current build has: file-backed dedup
  snapshots, the per-shard entry limit, kvstore recovery that drops legacy
  dedup snapshots. They detect it from the image's metrics and skip on an
  older build, so the suite can be pointed at the deployed build for
  comparison. `--require-new-build` (or `DEDUP_REQUIRE_NEW_BUILD=1`) turns
  those skips into failures; CI sets it, so a missing feature cannot pass as
  a skip.
* `--keep-clusters` keeps a failed test's data directories (under `/tmp`).
* `DEDUP_STRESS_SCALE` (default 1) multiplies the stress tests' record counts
  and chaos rounds. Every stress test also asserts that no allocation failed
  (`vectorized_memory_malloc_failed`) and that no shard's available memory
  fell below a floor (`vectorized_memory_available_memory_low_water_mark`).

Brokers run on the host network, so docker must allow `--network host`. A
node's ports are `port_base + 10 * node_id + {2: kafka, 4: admin, 5: rpc}`.

`test_bench.py` (opt-in with `--bench`) measures what dedup costs end to
end; results and the per-record microbenchmark are in
[BENCHMARK.md](BENCHMARK.md).

## What is covered

| module | scenarios |
|---|---|
| `test_semantics.py` | first wins across and within requests; all-duplicate requests are acknowledged; offsets stay contiguous; retries absorbed; acks 0/1/all; window expiry measured on record timestamps; future timestamps cannot disable dedup; changing, disabling, re-enabling and late-enabling dedup; null and empty keys; header mode (header identity, first occurrence, missing or null header rejects the request without indexing it, switching identity source starts fresh); large and binary identities; idempotent and transactional producers bypass dedup, aborted transactions do not poison it; every compression codec with partial drops; partitions and topics independent; concurrent producers; per-partition metrics on leader and followers |
| `test_durability.py` | leadership on every replica; graceful leader restart; leader crash; whole-cluster restart and crash; follower catch-up from the log and from a raft snapshot after prefix truncation; replica moved to a new node, with and without truncation; replicas moved between cores; segment churn with many local snapshots; recovery bounded by the window; topic recreated under the same name; partition count increase |
| `test_capacity.py` | per-partition ceiling keeps the newest identities; per-shard limit across partitions, across topics, and per shard; default limit derived from shard memory; lowering the limit takes effect on restart, with restores bounded by it; both limits change without a restart -- raised, new identities are indexed at once; lowered, nothing is dropped until traffic arrives, then the index (or the shard, an idle partition included) shrinks back under while the newest identities stay indexed; dedup snapshots are files and the kvstore stays small; expired entries on a shard's other partitions are reclaimed before a busy partition's live ones are trimmed, including those of a partition that went quiet, but not those of one stamping records behind its neighbours |
| `test_startup.py` | a full shard restarts (clean and killed) in 512 MiB; restart with half the memory the indices were built with; 24 partitions restoring at once; a three-node cluster with full shards restarting together; rolling restarts under live traffic absorb every retry |
| `test_stress.py` | four producers racing over overlapping keys on eight replicated partitions; snapshot storm (1 MiB segments) on a shard past its budget in 768 MiB; chaos -- random kills, restarts, frozen nodes and leadership moves under retrying producers, with no acknowledged record lost and no identity written twice; 32 partitions at twelve times the shard limit; crash/restart loop with a full index in 512 MiB; a frozen follower catching up hundreds of thousands of identities at once; key-mode, header-mode, plain, idempotent and transactional workloads together on one shard; topic churn returning the budget; 4 KiB identities costing no more than small ones; idle partitions holding the budget while another gets busy; a burst that goes quiet beside busy feeds is reclaimed once it has aged a window, after which no feed loses any of its window |
| `test_upgrade.py` | upgrade drops the legacy kvstore dedup snapshots and rebuilds the index from the log; upgrade into less memory; rolling upgrade of three nodes with leadership on old and new builds; raft snapshots between versions in both directions; downgrade and upgrade again |
