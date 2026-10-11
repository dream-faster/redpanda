# Slim Redpanda: codebase inventory for a Rust rewrite

This maps the code in this tree (the slim, local-storage-only broker described
in `HANDOFF.md`, based on v26.2.x) to features, with line counts, so the
rewrite in `PLAN.md` can be scoped and tracked.

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
python3 tools/rust-rewrite/loc_inventory.py --markdown # this table
```

**Scope column.**

- `keep`: must be rewritten with parity.
- `drop`: out of scope by decision. Authentication and TLS are removed, along
  with everything that only exists because of them (see "What dropping
  authentication and TLS removes" below).
- `dead`: has no production caller in this tree, so nothing to port.

## Totals

| | Prod C++ | Test C++ |
|---|---:|---:|
| **keep** (to rewrite) | **240,499** | 125,065 |
| drop (authn, authz, audit, license, TLS) | 18,628 | 9,476 |
| dead (no production caller) | 13,736 | 9,706 |
| **all of `src/v`** | 272,863 | 144,247 |

Not counted above:

| Asset | Size | Role in the rewrite |
|---|---:|---|
| Seastar (external, pinned fork, fetched by Bazel) | not counted | The runtime everything above is built on: reactor per core, `sharded<T>`, scheduling groups, DMA file I/O, the per-shard allocator, the HTTP server, metrics. The rewrite has to replace it; see PLAN.md "Runtime" |
| Kafka API JSON schemas (`kafka/protocol/schemata`) | 4,153 lines, 46 APIs | Reused as-is: the Rust codec generator reads the same files |
| Kafka schema code generator (`generator.py`) | 2,043 lines Python | Replaced by a Rust generator (build script or proc-macro) |
| Internal RPC service definitions (`*.json` in `cluster/`, `raft/`, `kafka/data/`) | 11 services, 68 methods after the cut | Reused as-is: same JSON drives Rust RPC stubs |
| Admin API v1 Swagger docs (`redpanda/admin/api-doc`) | 6,144 lines JSON | Contract for the REST surface |
| Admin API v2 protobufs (`proto/`) | 1,456 lines | Reused as-is (prost/tonic or connect-rust) |
| `rpk` CLI (Go, `src/go/rpk`) | 55,738 + 26,182 test | **Not rewritten.** It is a client of the Kafka and Admin APIs; parity of those APIs keeps it working |
| Ducktape integration tests (`tests/rptest`) | ~126k Python | **Reused** as black-box acceptance tests against the Rust broker |
| Java/Go verifiers (`tests/java`, `tests/go`) | ~12k | Reused as-is (client-side correctness checkers) |

## Features by layer

Subtotals are production lines only.

### Kafka API (44,412)

| Feature | Scope | Prod LOC | Test LOC |
|---|---|---:|---:|
| Kafka protocol codecs + schema generator | keep | 6,009 | 1,503 |
| Kafka server core (listener, connections, dispatch) | keep | 6,519 | 10,299 |
| Kafka produce path | keep | 1,337 | 0 |
| Kafka fetch path (fetch, sessions, memory units, coalescing) | keep | 3,759 | 2,740 |
| Kafka consumer groups + offsets | keep | 10,490 | 1,529 |
| Kafka transactions + idempotence (broker-side handlers) | keep | 1,131 | 0 |
| Kafka topic + config admin APIs | keep | 6,375 | 0 |
| Kafka metadata / api_versions / describe_cluster | keep | 1,003 | 0 |
| Kafka client quotas + throughput control | keep | 3,686 | 2,412 |
| Kafka usage accounting | keep | 745 | 182 |
| Kafka partition proxy + kafka_data RPC | keep | 3,095 | 1,156 |
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

### Raft (21,452)

| Feature | Scope | Prod LOC | Test LOC |
|---|---|---:|---:|
| Raft: consensus core (election, replication, heartbeats) | keep | 11,550 | 891 |
| Raft: recovery + throttling | keep | 1,816 | 385 |
| Raft: membership / joint-consensus configuration | keep | 2,879 | 1,238 |
| Raft: state machine framework (persisted_stm, snapshots) | keep | 3,624 | 1,511 |
| Raft: group manager, RPC service, probes | keep | 1,583 | 6,632 |

`raft/consensus.cc` alone is 4,933 lines. Raft runs one group per partition
plus raft0 (the controller), with heartbeats batched per node pair
(`heartbeat_manager`), leadership transfer, voter priorities, learners, and
the 8-method `raftgen` RPC service.

### Storage (29,161)

| Feature | Scope | Prod LOC | Test LOC |
|---|---|---:|---:|
| Storage: segments, index, appender, readers, log manager | keep | 19,699 | 13,473 |
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

### Cluster and controller (68,830)

| Feature | Scope | Prod LOC | Test LOC |
|---|---|---:|---:|
| Transactions: coordinator (tm_stm, tx gateway) + rm_stm | keep | 13,162 | 2,742 |
| Controller core (controller stm, backend, snapshot, commands) | keep | 6,286 | 915 |
| Topic metadata (topic_table, frontend, dispatcher) | keep | 6,193 | 1,312 |
| Cluster shared types, errors, legacy wire shims, RPC service | keep | 6,594 | 6,582 |
| Partition placement: allocator + leader balancer | keep | 5,268 | 575 |
| Membership, discovery, cluster bootstrap | keep | 4,612 | 0 |
| Health monitor + node status | keep | 4,349 | 703 |
| Feature flags + upgrade gating | keep | 3,883 | 700 |
| Partition auto-balancer (planner/backend) | keep | 3,712 | 2,756 |
| Partition runtime (partition, manager, eviction/properties stms) | keep | 3,042 | 1,703 |
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

### Platform: config, RPC, networking, model, codecs (26,562)

| Feature | Scope | Prod LOC | Test LOC |
|---|---|---:|---:|
| Configuration framework + ~350 properties | keep | 8,038 | 2,084 |
| Data model (record batches, ntp, offsets, ADL) | keep | 5,870 | 1,958 |
| Networking: server, connection limits/rates, transports | keep | 3,387 | 1,014 |
| Internal RPC framework (serde over TCP) | keep | 3,364 | 1,968 |
| HTTP client | keep | 2,109 | 2,207 |
| Compression (gzip, snappy, lz4, zstd) | keep | 1,949 | 708 |
| serde: positional binary envelope format | keep | 1,845 | 1,360 |

Configuration is 319 cluster properties (`config/configuration.h`) and 30
node properties (`config/node_config.h`), each with validation, defaults,
bounds, YAML and JSON (de)serialisation, live-update bindings, and the
aliases and deprecations that keep old config files loading. 51 headers
declare `serde::envelope` types (284 envelope declarations in total), which
are the internal RPC and on-disk payloads.

### Operations and observability (11,432)

| Feature | Scope | Prod LOC | Test LOC |
|---|---|---:|---:|
| Application wiring (main, service startup/shutdown) | keep | 3,236 | 1,041 |
| Metrics (Prometheus endpoints, host metrics) | keep | 2,727 | 633 |
| Debug bundle service | keep | 2,163 | 1,435 |
| Resource mgmt (memory groups, CPU sched groups, profilers) | keep | 1,490 | 559 |
| Crash tracker (signal-safe crash records) | keep | 1,311 | 419 |
| Startup system checks | keep | 505 | 0 |

### Foundations (24,525)

| Feature | Scope | Prod LOC | Test LOC |
|---|---|---:|---:|
| Utilities (hist, retry chain, caches, encodings, misc) | keep | 15,095 | 9,898 |
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

### Dead in this tree (13,736)

| Feature | Scope | Prod LOC | Test LOC |
|---|---|---:|---:|
| Kafka client library (only caller was audit) | dead | 9,197 | 5,790 |
| Parquet/Thrift/Avro codecs (Iceberg leftovers) | dead | 4,539 | 3,916 |

The Kafka client becomes dead once audit logging is dropped. Ducktape and
the Go/Java verifiers already provide external clients for tests.

## What dropping authentication and TLS removes

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
- **Enterprise license checks.** These gate RBAC, audit, FIPS and a few
  balancer features. This fork has no use for licence keys, so the remaining
  gated features (continuous balancing, leadership pinning) become plain
  config. This one is a recommendation rather than something you asked for;
  see PLAN.md "Open decisions".

TLS goes from every listener: Kafka, internal RPC, Admin API, and
Prometheus. That removes OpenSSL, `crypto/`, certificate reload and the
TLS-related node config.

This changes behaviour that people can see:

- Clients configured with `security.protocol=SASL_*` or `SSL` fail to
  connect. `ApiVersions` stops advertising the SASL, ACL, SCRAM and role
  APIs, so Kafka admin tools report them as unsupported rather than failing
  partway through.
- Anyone who can reach a port has full control, including deleting topics
  and changing cluster config. Network isolation becomes the only access
  control. Deploy accordingly.
- Existing stock clusters keep users, ACLs and roles in the controller log
  (`user_management_cmd`, `acl_management_cmd`, `role_management_cmd`) and in
  controller snapshots. The Rust controller must **parse and drop** these,
  the same way `cluster/legacy_wire.h` already handles removed features.
  Otherwise it can't replay a stock log.
- A rolling migration from a stock cluster only works if that cluster
  already runs with TLS and SASL off on all listeners, internal RPC included.
  Turning them off is a prerequisite step.

## Interfaces and formats that must stay byte-compatible

These are what "parity" is measured against (see PLAN.md):

| Contract | Defined in | Notes |
|---|---|---|
| Kafka wire protocol, 38 APIs | `kafka/protocol/schemata/*.json` | Including flexible versions and tagged fields |
| Internal RPC framing + 11 services, 68 methods | `rpc/`, `*rpc*.json`, `raft/raftgen.json`, `cluster/controller.json` | Needed for mixed C++/Rust clusters |
| serde envelopes (version, compat_version, size prefix, positional fields) | `serde/rw/*`, 51 headers | Including the `legacy_*` placeholder slots in `cluster/legacy_wire.h` |
| Legacy ADL encoding | `model/adl_serde*`, `reflection/` | Still used by raft: configuration history in the kvstore, some raft types, snapshot metadata |
| Record batch on-disk header and CRCs | `model/record*`, `storage/parser*` | Kafka v2 batch format; Redpanda's own 61-byte packed header with header_crc on disk |
| Segment files, `.base_index`, compaction indexes | `storage/segment*`, `storage/index_state*`, `storage/compacted_index*` | File naming `{base_offset}-{term}-{version}.log`, plus `.base_index`, `.compaction_index` and staging files |
| KV store (per-shard, local, not replicated) | `storage/kvstore*` | Keys: raft voted_for, offset translator, stm snapshots, shard placement |
| Raft/STM snapshot files | `raft/`, `storage/snapshot*` | Controller snapshot, per-partition stm snapshots |
| Controller log command types | `model/record_batch_types.h`, `cluster/commands.h` | 34 batch types in the enum, including ones for removed features that must still be skipped |
| Config file and cluster config schema | `config/` | YAML node config, cluster config JSON, property names and aliases |
| Admin API v1 routes and JSON shapes | `redpanda/admin/api-doc` | rpk, Helm chart, operator depend on these |
| Admin API v2 protos | `proto/redpanda/core/admin` | |
| Prometheus metric names and labels | probes across the tree | `/metrics` and `/public_metrics`; dashboards in `tools/dashboards` |
| Directory layout | `storage/`, `config/` | `{data_directory}/{namespace}/{topic}/{partition}_{revision}/`, `redpanda/` namespace, `.redpanda_data` markers |
