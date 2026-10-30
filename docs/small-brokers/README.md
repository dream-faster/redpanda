# A thousand RF=3 topics on 2 GB

Can one cluster hold ~1000 single-partition topics at replication factor 3
when brokers have 2 GB of memory? What stops it by default, and can we remove
those limits?

Short answers:

- **Yes, and with room to spare.** The memory a replica actually costs is not
  what blocks it. Three 2 GiB brokers held 3000 RF=3 topics (3000 replicas
  each) under load and through a broker restart, with peak non-reclaimable
  memory under 700 MiB per broker. Three brokers sharing *one* 2 GB machine
  (600 MiB each) held 1000 topics with peak usage under 300 MiB each.
- **What blocks it by default is a set of admission heuristics and fixed
  reservations.** None of them reflects what a replica costs. With the stock
  defaults on a 2 GiB broker, topic creation fails at about 1000-1050 replicas
  per broker. 1000 topics plus `__consumer_offsets` lands right at that edge.
- **Every blocker except RF ≤ broker count can be removed.** Most are config
  changes ([`two-gib.yaml`](../../tools/small-broker-poc/profiles/two-gib.yaml),
  [`shared-2gb-box.yaml`](../../tools/small-broker-poc/profiles/shared-2gb-box.yaml)).
  The biggest fixed cost, the compaction key map, is also fixed in code:
  `storage: start the compaction key map small and grow it on demand`.

## How it was measured

These measurements use upstream `redpandadata/redpanda:v26.2.6`, the release
this fork is cut from. This fork's build fetches its dependencies from GitHub,
which the environment these numbers were taken in could not reach. The fork
only removes subsystems (tiered storage, cloud topics, Iceberg, ...) and adds
dedup, which is opt-in per topic. So per-replica costs here should be an upper
bound for the fork, but the fork itself has not been measured yet.

The harness is in [`tools/small-broker-poc`](../../tools/small-broker-poc):

- Three Docker brokers, one core each.
- Single-partition RF=3 topics.
- 1 KB records.
- Load comes from one `acks=all` librdkafka producer spread over every topic,
  plus two consumers in one group subscribed to all of them.

"Hard" memory is Seastar's total minus what it could reclaim, mostly the batch
cache. That is the number that has to fit. Raw `allocated` grows to fill free
memory with cache, so it is not a meaningful limit. "Peak hard" is the
low-water mark of available memory, so it captures the worst moment, not just
the end state.

| setup | profile | topics | load | peak hard per broker | broker restart under load |
|---|---|---:|---|---:|---|
| 3 × 2 GiB | defaults | 1000 | 10 MB/s | 645 MiB | healthy in ~30 s, peak 671 MiB |
| 3 × 2 GiB | defaults + 16 MiB key map | 1000 | 10 MB/s | 454-461 MiB | — |
| 3 × 2 GiB | `two-gib.yaml` | 1000 | 10 MB/s | 459-464 MiB | healthy in 38 s, peak 512 MiB |
| 3 × 2 GiB | `two-gib.yaml` | 3000 | 10 MB/s | 641-692 MiB | healthy in 36 s, peak 641 MiB |
| 3 × 600 MiB (one 2 GB box) | `shared-2gb-box.yaml` | 1000 | 5 MB/s | 292-295 MiB | healthy in 23 s, peak 291 MiB |

None of these runs had an allocation failure or an assert.

Per idle replica, measured as broker RSS before and after creating 1000 topics
and writing 5 records to each: **~100 KiB**. About 20 KiB of that is
per-partition metrics: with `disable_metrics` and `disable_public_metrics` it
was ~80 KiB.

## What stops it, and what removes each limit

### 1. Admission control: `topic_memory_per_partition` (the one you hit)

Topic creation is refused once

```
replicas in the cluster > brokers × min broker memory
                          × topic_partitions_memory_allocation_percent (10%)
                          / topic_memory_per_partition (200 KiB)
```

Topic creation then fails with `INVALID_PARTITIONS: unable to create topic
... due to hardware constraints`. The ceiling per broker is:

- 2 GiB broker: 1048 replicas.
- Docker image in a 2 GiB container without `--memory` (Redpanda gets
  1.936 GiB): 1014 replicas.

So 1000 RF=3 topics plus the 16 `__consumer_offsets` partitions, 1016
replicas per broker, does not fit.

This is a heuristic in `partition_allocator::check_memory_limits`. It is not
a measurement: nothing is reserved per replica. A replica costs ~100 KiB idle,
and 3000 replicas per broker fit in under 700 MiB at peak.

**Remove it:** lower `topic_memory_per_partition`. 64 KiB allows 3277
replicas per 2 GiB broker. Alternatively, raise
`topic_partitions_memory_allocation_percent`; this needs a restart, and it
moves memory out of the Kafka and RPC budgets. Don't set the per-partition
value at or above 400 KiB: `guess_is_memory_group_aware_memory_limit` then
treats it as a pre-memory-groups value and switches to the old formula.

### 2. `topic_partitions_per_shard` in the Docker image's dev mode

`rpk redpanda start --mode dev-container`, which the official image uses,
sets `topic_partitions_per_shard: 1000`. With one core that caps a broker at
1000 replicas, internal topics included, so it trips before limit 1. The
production default is 5000.

**Remove it:** set `topic_partitions_per_shard: 5000` (or higher), or run
outside dev-container mode.

### 3. File descriptors: `topic_fds_per_partition`

Creation is refused once replicas × 5 exceed the brokers' `nofile` limit.
Docker's default of 1024 stopped the first test run at 204 topics.

**Remove it:** raise `ulimit -n`. Check what your container runtime gives
Redpanda; many set a much higher limit than plain Docker. Lowering `topic_fds_per_partition` also works, but only if
replicas really do use fewer files.

### 4. The compaction key map (fixed ~180 MiB)

Sliding-window compaction allocates `min(storage_compaction_key_map_memory =
128 MiB, 12% of memory)` per shard. It does so on the first housekeeping pass,
~30 s after boot, as soon as any log exists, and keeps it forever. That
happens even when the only compacted logs are `__consumer_offsets` with a few
thousand keys.

Measured on an idle 2 GiB broker, hard memory was 352 MiB with the default and
170 MiB at 16 MiB. That one property accounts for about half of a small
broker's idle footprint.

**Remove it:**

- Config: `storage_compaction_key_map_memory: 16777216`. A smaller map stays
  correct: compaction indexes a large log in more passes, which costs disk
  reads.
- Code (this branch): the map starts at `storage_compaction_key_map_min_memory`
  (16 MiB) and doubles after a pass in which it filled, up to the old
  reservation. It halves after a pass in which compaction used under a quarter
  of it. Small brokers keep the small map without tuning, and brokers with
  large compacted topics still grow to the full map. Setting the minimum to the
  maximum restores the old behaviour.

### 5. Disk preallocation: `segment_fallocation_step`

This limit is about disk, not memory, but it is the first thing to break on a
small instance. Each active segment preallocates 32 MiB, so 1000 replicas
reserve 32 GB per broker before any data arrives. The first test run here
filled the disk and every broker died on

```
Assert failure: (src/v/storage/segment_appender.cc:446) 'false' We failed to
fallocate file. This usually means we have ran out of disk space.
```

**Remove it:** `segment_fallocation_step: 4194304` (4 MiB) in both profiles.

### 6. Replication factor needs that many brokers

This one cannot be removed. RF=3 needs three brokers. They can share one
machine: run three processes with `--smp 1 --memory 600M`, as the shared-box
row above does. This only protects against a broker failing, not the machine.

### Things that are *not* the problem

- **The Kafka, RPC, recovery and chunk-cache budgets** (30/20/10/15 shares of
  memory). They look large in the startup log, but they are ceilings on
  concurrent work, not allocations.
- **Reader buffers.** Reducing `storage_read_buffer_size` to 32 KiB and
  `readers_cache_eviction_timeout_ms` to 10 s changed peak usage by 1 MiB
  (454 → 455 MiB) at 10 MB/s over 1000 partitions.
- **Dedup (this fork).** Indices exist only on topics with a dedup window,
  and all of a shard's indices together are capped at 15% of its memory
  (`dedup_max_entries_per_shard` = 0).

## What is left to cut

These fixed or per-replica costs remain. None of them blocks 1000 topics, but
each one matters at the shared-box scale:

- **Chunk cache retention.** The segment-appender chunk cache keeps up to a
  third of its budget as free chunks for good (`chunk_cache::add`):
  ~99 MiB at 2 GiB (measured), ~27 MiB at 600 MiB (from the share formula). Making that target follow the number
  of active appenders would return most of it.
- **Per-partition metrics.** These cost ~20 KiB per replica, measured by
  turning `disable_metrics` and `disable_public_metrics` on, which also drops
  every other metric. `aggregate_metrics` was not measured. Registering
  per-partition series only on leaders would save most of the cost without
  going blind.
- **Admission control could measure instead of guess.** Charging replicas
  against what remains after the fixed reservations, at a measured
  per-replica cost, would make the default ceiling track the hardware.

## Reproduce

```bash
cd tools/small-broker-poc
pip install confluent-kafka
./cluster.sh up && ./cluster.sh hosts | sudo tee -a /etc/hosts && ./cluster.sh down
PROFILE=profiles/two-gib.yaml ./run.sh 1000
PROFILE=profiles/two-gib.yaml ./run.sh 3000
PROFILE=profiles/shared-2gb-box.yaml RPMEM=600M CMEM=680m ./run.sh 1000 60 5
PROFILE=profiles/defaults.yaml ./run.sh 1000
```

`run.sh` uses a 4 KiB fallocation step unless `SMALL_DISK=0`, so the test
machine's disk doesn't fill. A run needs about 2.5 GiB of RAM per broker
container at the 2 GiB size.
