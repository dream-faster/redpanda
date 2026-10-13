# Slim Redpanda: codebase inventory for a Rust rewrite

This maps the code in this tree (the slim, local-storage-only broker described
in `HANDOFF.md`, based on v26.2.x) to features, with line counts, so the
rewrite in `PLAN.md` can be scoped and tracked.

Counted at `v26.2.x` commit `9daa2fab`. That includes the fork's windowed
produce-time deduplication (`5801001a`).

**How it was counted.** Non-blank lines of `.h`/`.cc` under `src/v`, assigned
to features by path globs in `tools/rust-rewrite/loc_inventory.py`. Every file
lands in exactly one feature and none are left unassigned. Code under a
`tests/`/`test/` directory or named `*_test.cc`/`*_bench.cc` counts as test
code. Test lines follow the directory they sit in, so a test exercising produce
and fetch through a fixture in `kafka/server/tests/` counts under "Kafka server
core". Treat per-feature test counts as approximate; production counts are
exact. Re-run with:

```bash
python3 tools/rust-rewrite/loc_inventory.py            # plain
python3 tools/rust-rewrite/loc_inventory.py --markdown # full table
```

**Scope column.**

- `keep`: must be rewritten with parity.
- `drop`: out of scope by decision: authentication, everything that only
  exists because of it, TLS, and enterprise license checks.
- `compat`: only exists so a node can read state written by an older C++
  version. Rust clusters start from empty disks and carry nothing over from
  C++ nodes, so these go.
- `dead`: has no production caller in this tree, so nothing to port.

## Totals

| | Prod C++ | Test C++ |
|---|---:|---:|
| **keep** (to rewrite) | **241,206** | 127,622 |
| drop (authn, authz, audit, license, TLS) | 18,628 | 9,476 |
| compat (state from older C++ versions) | 1,431 | 432 |
| dead (no production caller) | 13,736 | 9,706 |
| **all of `src/v`** | 275,001 | 147,236 |

The `compat` row only counts whole files. Most backward-compatibility code
is spread through files that stay. Examples: `legacy_*` placeholder fields,
decode branches for old serde versions, code gated on 48 feature flags that
would all be active from day one in Rust, and parse-and-skip logic for
controller commands of removed features. None of that needs porting. It is
inside `keep` rows and can't be separated by file, so the real saving is
larger than 1,431 lines.

Not counted above:

| Asset | Size | Role in the rewrite |
|---|---:|---|
| Seastar (external, pinned fork, fetched by Bazel) | not counted | The runtime everything above is built on: reactor per core, `sharded<T>`, scheduling groups, DMA file I/O, the per-shard allocator, the HTTP server, metrics. The rewrite has to replace it; see PLAN.md "Runtime" |
| Kafka API JSON schemas (`kafka/protocol/schemata`) | 4,153 lines, 46 APIs | Reused as-is: the Rust codec generator reads the same files |
| Kafka schema code generator (`generator.py`) | 2,043 lines Python | Replaced by a Rust generator (build script or proc-macro) |
| Internal RPC service definitions (`*.json` in `cluster/`, `raft/`, `kafka/data/`) | 11 services, 68 methods after the cut | Reference for which internal calls exist. The Rust wire encoding is free to differ |
| Admin API v1 Swagger docs (`redpanda/admin/api-doc`) | 6,144 lines JSON | Contract for the REST surface |
| Admin API v2 protobufs (`proto/`) | 1,456 lines | Reused as-is (prost/tonic or connect-rust) |
| `rpk` CLI (Go, `src/go/rpk`) | 55,738 + 26,182 test | **Not rewritten.** It is a client of the Kafka and Admin APIs; parity of those APIs keeps it working |
| Ducktape integration tests (`tests/rptest`) | ~106k Python | **Reused** as black-box acceptance tests against the Rust broker |
| Java/Go verifiers (`tests/java`, `tests/go`) | ~12k | Reused as-is (client-side correctness checkers) |

## Features by layer

Subtotals are production lines only.

### Kafka API (44,534)

| Feature | Scope | Prod LOC | Test LOC |
|---|---|---:|---:|
| Kafka protocol codecs + schema generator | keep | 6,013 | 1,503 |
| Kafka server core (listener, connections, dispatch) | keep | 6,519 | 10,355 |
| Kafka produce path | keep | 1,356 | 0 |
| Kafka fetch path (fetch, sessions, memory units, coalescing) | keep | 3,759 | 2,740 |
| Kafka consumer groups + offsets | keep | 10,490 | 1,529 |
| Kafka transactions + idempotence (broker-side handlers) | keep | 1,131 | 0 |
| Kafka topic + config admin APIs | keep | 6,472 | 0 |
| Kafka metadata / api_versions / describe_cluster | keep | 1,003 | 0 |
| Kafka client quotas + throughput control | keep | 3,686 | 2,412 |
| Kafka usage accounting | keep | 745 | 182 |
| Kafka partition proxy + kafka_data RPC | keep | 3,097 | 1,156 |
| Kafka misc utils | keep | 263 | 538 |

Kafka APIs served after the cut (38 of 46; API key and supported versions):

| Area | APIs |
|---|---|
| Data | Produce (0) v0-8, Fetch (1) v0-13, ListOffsets (2) v0-6, OffsetForLeaderEpoch (23) v0-4, DeleteRecords (21) v0-2 |
| Metadata | Metadata (3) v0-12, ApiVersions (18) v0-4, DescribeCluster (60) v0, DescribeLogDirs (35) v0-4 |
| Groups | FindCoordinator (10) v0-4, JoinGroup (11) v0-6, Heartbeat (12) v0-4, LeaveGroup (13) v0-4, SyncGroup (14) v0-4, DescribeGroups (15) v0-5, ListGroups (16) v0-4, DeleteGroups (42) v0-2, OffsetCommit (8) v0-8, OffsetFetch (9) v0-8, OffsetDelete (47) v0 |
| Transactions | InitProducerId (22) v0-3, AddPartitionsToTxn (24) v0-3, AddOffsetsToTxn (25) v0-1, EndTxn (26) v0-3, TxnOffsetCommit (28) v0-3, DescribeProducers (61) v0, DescribeTransactions (65) v0, ListTransactions (66) v0 |
| Topics and configs | CreateTopics (19) v0-7, DeleteTopics (20) v0-6, CreatePartitions (37) v0-3, DescribeConfigs (32) v0-4, AlterConfigs (33) v0-2, IncrementalAlterConfigs (44) v0-1, AlterPartitionReassignments (45) v0, ListPartitionReassignments (46) v0 |
| Quotas | DescribeClientQuotas (48) v0-1, AlterClientQuotas (49) v0-1 |

Dropped with authentication: SaslHandshake (17), SaslAuthenticate (36),
DescribeAcls (29), CreateAcls (30), DeleteAcls (31),
DescribeUserScramCredentials (50), AlterUserScramCredentials (51),
DescribeRedpandaRoles (15000).

### Raft (21,440)

| Feature | Scope | Prod LOC | Test LOC |
|---|---|---:|---:|
| Raft: consensus core (election, replication, heartbeats) | keep | 11,556 | 891 |
| Raft: recovery + throttling | keep | 1,816 | 385 |
| Raft: membership / joint-consensus configuration | keep | 2,826 | 1,238 |
| Raft: state machine framework (persisted_stm, snapshots) | keep | 3,659 | 1,511 |
| Raft: group manager, RPC service, probes | keep | 1,583 | 6,632 |

`raft/consensus.cc` alone is 4,933 lines. Raft runs one group per partition
plus raft0 (the controller), with heartbeats batched per node pair
(`heartbeat_manager`), leadership transfer, voter priorities, learners, and
the 8-method `raftgen` RPC service.

### Storage (29,196)

| Feature | Scope | Prod LOC | Test LOC |
|---|---|---:|---:|
| Storage: segments, index, appender, readers, log manager | keep | 19,734 | 13,473 |
| Storage: compaction (sliding window, key maps, indexes) | keep | 4,120 | 5,234 |
| Storage: caches (batch cache, readers cache, chunk cache) | keep | 1,950 | 597 |
| Storage: offset translator | keep | 1,021 | 712 |
| Storage: KV store | keep | 907 | 483 |
| Storage: fault-injection file wrappers (test support) | keep | 1,464 | 320 |

`disk_log_impl.cc` (4,315) is the core: append, truncate, prefix-truncate,
retention by size and time, segment roll, housekeeping and compaction
scheduling. Segment data is written with O_DIRECT through
`segment_appender` and read through `chunk_cache`. `batch_cache` registers
with Seastar's memory reclaimer, so it shrinks under allocator pressure.

### Cluster and controller (70,235)

| Feature | Scope | Prod LOC | Test LOC |
|---|---|---:|---:|
| Transactions: coordinator (tm_stm, tx gateway) + rm_stm | keep | 13,162 | 2,742 |
| Cluster shared types, errors, RPC service | keep | 6,467 | 6,355 |
| Controller core (controller stm, backend, snapshot, commands) | keep | 6,286 | 915 |
| Topic metadata (topic_table, frontend, dispatcher) | keep | 6,244 | 1,445 |
| Partition placement: allocator + leader balancer | keep | 5,268 | 575 |
| Membership, discovery, cluster bootstrap | keep | 4,612 | 0 |
| Health monitor + node status | keep | 4,349 | 703 |
| Partition auto-balancer (planner/backend) | keep | 3,712 | 2,756 |
| Feature flags + upgrade gating | keep | 3,581 | 700 |
| Partition runtime (partition, manager, eviction/properties stms) | keep | 3,058 | 1,703 |
| Windowed produce-time dedup (dedup_stm, window filter) | keep | 1,767 | 2,638 |
| Metadata dissemination + leaders table + caches | keep | 2,799 | 812 |
| Shard placement + core balancing | keep | 2,439 | 1,137 |
| Self-test (disk/network benchmarks) | keep | 1,999 | 237 |
| ID allocator + distributed KV stm | keep | 1,584 | 319 |
| Cluster config replication (config_manager) | keep | 1,269 | 0 |
| Phone-home metrics reporter + crash reporter | keep | 1,223 | 159 |
| Maintenance mode / drain | keep | 416 | 0 |

The biggest files: `types` (3,791), `tx_gateway_frontend` (3,066), `rm_stm`
(2,758), `topic_table` (2,412), `partition_balancer_planner` (2,302),
`topics_frontend` (2,046), `controller_backend` (2,041), `members_manager`
(1,917). The controller RPC service has 31 methods and `tx_gateway` has 13.

**Windowed produce-time deduplication** exists only in this fork (design in
`docs/rfcs/20260730_log_derived_dedup_state.md`). Two topic properties turn it
on: `redpanda.dedup.window.ms` and `redpanda.dedup.key.header`. A cluster
property, `dedup_max_entries_per_partition`, bounds the per-partition index.
The partition leader drops records whose key (or chosen header value) it
already saw inside the window, before replicating them. Idempotent and
transactional produce bypass it. Followers rebuild the index from the data
batches they apply (`dedup_stm`), so it needs no extra replicated state. The
index is advisory: only the leader consults it, and duplicates near the window
boundary may get through by design. Outside this module it touches the
produce path (the reported base offset accounts for dropped records via
`replicated_record_count`), the raft state machine manager, topic properties
and `ntp_config`. Ducktape has no tests for it yet; its coverage is the C++
unit tests in `cluster/tests/dedup_*`.

The feature-flag mechanism stays because Rust releases will need rolling
upgrades between themselves. Its contents don't carry over: the 48 C++
feature flags are history, and a Rust cluster starts with all of their
behaviour switched on.

### Admin API (14,125)

| Feature | Scope | Prod LOC | Test LOC |
|---|---|---:|---:|
| Admin API v1 (REST) + debug bundle trigger | keep | 7,488 | 276 |
| Admin API v2 (ConnectRPC) + proto/JSON codecs | keep | 6,637 | 14,859 |

v1 documents 95 operations across brokers, cluster, cluster config, features,
partitions, raft, transactions, usage, recovery, debug (32) and
fault-injection (`hbadger`), plus the user/ACL routes that are dropped. v2
keeps `BrokerService` (GetBroker, ListBrokers), `ClusterService`
(ListKafkaConnections), `FeaturesService` (FinalizeUpgrade, GetUpgradeStatus)
and the internal debug and forced-reconfiguration services; the 15
`SecurityService` RPCs go. `server.cc` is 4,186 lines on its own.

### Platform: config, RPC, networking, model, codecs (26,111)

| Feature | Scope | Prod LOC | Test LOC |
|---|---|---:|---:|
| Configuration framework + ~350 properties | keep | 8,056 | 2,084 |
| Data model (record batches, ntp, offsets) | keep | 5,333 | 1,958 |
| Networking: server, connection limits/rates, transports | keep | 3,387 | 1,014 |
| Internal RPC framework (serde over TCP) | keep | 3,364 | 1,968 |
| HTTP client | keep | 2,109 | 2,207 |
| Compression (gzip, snappy, lz4, zstd) | keep | 2,017 | 804 |
| serde: positional binary envelope format | keep | 1,845 | 1,360 |

Configuration is 320 cluster properties (`config/configuration.h`) and 30
node properties (`config/node_config.h`), each with validation, defaults,
bounds, YAML and JSON (de)serialisation, and live-update bindings. 51
headers declare `serde::envelope` types (284 envelope declarations in
total), which are the internal RPC and on-disk payloads. In Rust these
become derive-generated types in a format of the rewrite's choosing.

### Operations and observability (11,439)

| Feature | Scope | Prod LOC | Test LOC |
|---|---|---:|---:|
| Application wiring (main, service startup/shutdown) | keep | 3,243 | 1,041 |
| Metrics (Prometheus endpoints, host metrics) | keep | 2,727 | 633 |
| Debug bundle service | keep | 2,163 | 1,435 |
| Resource mgmt (memory groups, CPU sched groups, profilers) | keep | 1,490 | 559 |
| Crash tracker (signal-safe crash records) | keep | 1,311 | 419 |
| Startup system checks | keep | 505 | 0 |

### Foundations (24,126)

| Feature | Scope | Prod LOC | Test LOC |
|---|---|---:|---:|
| Utilities (hist, retry chain, caches, encodings, misc) | keep | 14,696 | 9,759 |
| Async primitives on Seastar (ssx) | keep | 4,896 | 3,846 |
| iobuf / zero-copy byte buffers | keep | 2,719 | 2,974 |
| Containers (chunked vectors, radix tree, interval maps) | keep | 1,815 | 2,697 |
| Shared test fixtures | keep | 0 | 2,642 |

Much of this layer exists to make up for gaps in C++ and Seastar. Rust's
standard library and crates (`bytes`, `hdrhistogram`, `crc32c`, `xxhash`,
`smallvec`, `tokio-util` style primitives) cover a lot of it. Expect this
layer to shrink the most.

### Dropped by decision (18,628)

| Feature | Scope | Prod LOC | Test LOC |
|---|---|---:|---:|
| Authorization: ACLs, RBAC roles, user/role management | drop | 7,417 | 5,909 |
| Audit logging (OCSF events to a topic) | drop | 4,304 | 1,220 |
| Authentication: SASL PLAIN/SCRAM, mTLS principal mapping | drop | 3,563 | 968 |
| TLS (OpenSSL contexts, cert reload, crypto wrappers) | drop | 2,565 | 1,079 |
| Enterprise license checks | drop | 779 | 300 |

### Compatibility with older C++ versions (1,431, plus scattered code)

| Feature | Scope | Prod LOC | Test LOC |
|---|---|---:|---:|
| Compatibility with older C++ versions (ADL, legacy wire slots, upgrade migrators) | compat | 1,431 | 432 |

Covers the ADL encoding (`reflection/adl*`, `model/adl_serde*`), the
`legacy_*` placeholder fields in `cluster/legacy_wire.h`, the feature
migrators in `migrations/`, and `raft/broker_compat.h`. See "Totals" for
the compatibility code that sits inside kept files.

### Dead in this tree (13,736)

| Feature | Scope | Prod LOC | Test LOC |
|---|---|---:|---:|
| Kafka client library (only caller was audit) | dead | 9,197 | 5,790 |
| Parquet/Thrift/Avro codecs (Iceberg leftovers) | dead | 4,539 | 3,916 |

The Kafka client becomes dead once audit logging is dropped. Ducktape and
the Go/Java verifiers already provide external clients for tests.

## What the scope cuts remove

Authentication goes, and with it the parts that only exist to serve it.

- **Authorization (ACLs, RBAC).** Without authentication every connection is
  the anonymous principal, so ACL checks have nothing to decide on. Every
  `ctx.authorized(...)` call in the Kafka handlers and the authz checks on
  admin routes disappear rather than being stubbed.
- **Audit logging.** It records *who* did what, so it has nothing to report
  once principals are gone. It is also the only production user of the
  internal Kafka client.
- **Ephemeral credentials and the breakglass admin service.** These only
  exist to mint and bypass credentials.
- **Enterprise license checks.** These gated RBAC, audit, FIPS, continuous
  partition and core balancing, leadership pinning and topic-deletion
  protection. The balancing, pinning and deletion-protection features stay
  and are controlled by config alone.

TLS goes from every listener: Kafka, internal RPC, Admin API, and
Prometheus. That removes OpenSSL, `crypto/`, certificate reload and the
TLS-related node config.

No state carries over from C++ nodes. That means no ADL, no legacy wire
slots, no migrators, no replay of C++ controller logs, and no reading of
C++ data directories.

This changes behaviour that people can see:

- Clients configured with `security.protocol=SASL_*` or `SSL` fail to
  connect. `ApiVersions` stops advertising the SASL, ACL, SCRAM and role
  APIs, so Kafka admin tools report them as unsupported rather than failing
  partway through.
- Anyone who can reach a port has full control, including deleting topics
  and changing cluster config. Network isolation becomes the only access
  control. Deploy accordingly.
- Moving to the Rust broker means new clusters on empty disks. Any data
  that has to move does so at the Kafka level (for example MirrorMaker 2 or
  Redpanda Connect), which copies records but not offsets.

## Interfaces that define parity

Only externally visible surfaces are contracts. Internal RPC, on-disk
formats, and controller command encodings are free for the Rust design to
choose (see PLAN.md §2).

| Contract | Defined in | Notes |
|---|---|---|
| Kafka wire protocol, 38 APIs | `kafka/protocol/schemata/*.json` | Including flexible versions and tagged fields |
| Kafka record batch format v2 | Kafka spec, `model/record*` | Mandatory on the wire; also the natural on-disk payload for zero-copy fetch |
| Kafka-visible semantics | Kafka spec, ducktape, verifiers | Offsets, LSO and HWM, idempotence, transactions, compaction, retention, group rebalancing |
| Config file and property schema | `config/` | YAML node config, cluster config property names, types, defaults, bounds |
| Topic configs | `kafka/server/handlers/configs/*` | Names and semantics of the topic properties clients set, including `redpanda.dedup.window.ms` and `redpanda.dedup.key.header` |
| Admin API v1 routes and JSON shapes | `redpanda/admin/api-doc` | rpk, Helm chart, operator depend on these |
| Admin API v2 protos | `proto/redpanda/core/admin` | |
| Prometheus metric names and labels | probes across the tree | `/metrics` and `/public_metrics`; dashboards in `tools/dashboards` |
| Process CLI flags and `io-properties.yaml` | Seastar options as passed by `rpk redpanda start` and `rpk iotune` | `--smp`, `--memory`, `--overprovisioned`, etc. |
