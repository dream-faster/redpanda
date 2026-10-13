# Proposal: a simpler, Rust-native broker

`PLAN.md` describes a parity port. It keeps Redpanda's architecture, ports
its algorithms decision for decision, and matches every operator surface:
350 config properties, the full Admin API, every metric. That is a faithful
translation, about 140–190k lines of Rust.

This proposal is the alternative. It keeps what Kafka clients and the data
depend on, redesigns the rest the way a Rust codebase would naturally be
built, and deletes what only exists because of C++, Seastar or Redpanda's
commercial product. The target is roughly **60k lines of Rust** (estimate,
§5) for the same 38 Kafka APIs and the same data guarantees.

Line counts below are from `INVENTORY.md` (production C++, `v26.2.x` at
`9daa2fab`).

## Summary

| | Parity port (`PLAN.md`) | Rust-native (this proposal) |
|---|---|---|
| Kafka protocol, 38 APIs | same | same |
| Data guarantees (raft safety, `acks=all` fsync, idempotence, transactions, compaction, dedup G1–G5) | same | same |
| Concurrency model | Thread-per-core, Seastar re-implemented | tokio work-stealing runtime, partition actors |
| Disk I/O | O_DIRECT plus Redpanda's own caches | Buffered I/O, OS page cache, explicit fsync |
| Kafka offsets vs raft log | Two offset spaces with a translator | One offset space |
| Cluster metadata | Controller plus dissemination plus per-node tables | One replicated `ClusterState` on every node |
| Config | 350 properties | ~60 properties, plus a list of legacy names that are accepted when left at their defaults |
| Admin API | ~95 v1 operations plus v2 | ~20 routes, including what Helm and `rpk` need |
| Metrics | Every metric, same names | ~30 public metrics the shipped dashboards use, same names |
| Debug bundle, crash tracker, self-test, usage, phone-home | ported | dropped |
| Upgrades | Feature-flag table | One cluster version number |
| Rust size (estimate) | 140–190k lines | ~50–80k lines |
| Effort (rough) | 20–28 engineer-years | 8–12 engineer-years |

What you give up: some peak throughput and tail latency compared with
Seastar on the same hardware (§8), and compatibility with tools that use
Admin API routes or config properties outside the kept set.

## 1. What stays fixed

These are the reasons the broker exists, so they don't move:

1. **The Kafka protocol.** The 38 APIs at the same version ranges listed in
   `INVENTORY.md`, record batch format v2, and Kafka's error codes.
   Existing clients, Kafka CLI tools, Kafka Streams, Flink and Connect work
   unchanged.
2. **The data guarantees** from `PLAN.md` C2:
   - Raft safety.
   - `acks=all` is acknowledged only after a majority has fsynced.
   - Idempotent producers, exactly-once transactions (read_committed, LSO).
   - Compaction and retention semantics.
   - Consumer group offset durability.
   - Windowed dedup per guarantees G1–G5 and bounds B1–B5 of
     `docs/rfcs/20260730_log_derived_dedup_state.md`.
3. **Topic configs.** Kafka's standard names and meanings, plus
   `redpanda.dedup.window.ms` and `redpanda.dedup.key.header`.
4. **What the deployment tooling calls** (§4.11):
   - The six Admin API calls the Helm chart's post-upgrade job makes
     (listed in the commit message of `926f87a6`).
   - `rpk redpanda start`'s config file and flags.
   - The `rpk` commands that go through the Kafka API.
   - The ~30 `redpanda_*` metrics used by the dashboards `rpk generate
     grafana-dashboard` ships.

## 2. Design rules

- **Library first.** Use a maintained crate wherever one exists:
  - Kafka protocol: `kafka-protocol`.
  - Runtime: `tokio`.
  - HTTP: `axum`.
  - Embedded KV: `redb`.
  - Metrics: `prometheus-client`.
  - Logging: `tracing`.
  - Compression: `zstd`, `lz4_flex`, `snap`, `flate2`.
  - Checksums: `crc32c`.

  Write code only where Redpanda's value is: replication, the log, Kafka
  semantics.
- **Sans-IO cores.** Raft, the group coordinator, the transaction
  coordinator, producer state and dedup are pure state machines: `step(event)
  -> actions`, no I/O, no clocks, no tasks. Drivers do the I/O. Everything
  that has to be correct can be tested deterministically without a
  simulator.
- **One owner per piece of state.** Each partition is an actor (one tokio
  task) that owns its log, raft state and state machines, and is driven by a
  channel. No locks on the hot path, no `Arc<Mutex<…>>` webs.
- **Delete before porting.** A C++ component is only ported if a Kafka
  client, a data guarantee, or the deployment tooling in §1 needs it.
  Tuning knobs that exist because of Seastar or Redpanda's caches go away
  with them.
- **Versioned from day one.** Every on-disk structure and internal message
  carries a version; the cluster has one version number (§4.12).

## 3. Simplifications, and what each one removes

| # | Simplification | Removes | Cost |
|---|---|---|---|
| S1 | tokio multi-thread runtime instead of thread-per-core | Seastar re-implementation, `sharded<>`, cross-core message passing, scheduling and memory groups, shard placement and core balancing (2,439 lines), cross-core request routing | Less control over scheduling and tail latency (§8) |
| S2 | Buffered I/O and the OS page cache instead of O_DIRECT plus own caches | `batch_cache`, `chunk_cache`, `readers_cache`, the DMA appender, the I/O scheduler, and their tuning knobs | Less control over memory under mixed read and write load. The page cache is how Apache Kafka works, so it is proven at scale |
| S3 | One offset space: raft log index = Kafka offset | The offset translator (1,021 lines) and the Kafka-offset vs log-offset split that runs through storage, raft, Kafka and transactions | Raft configuration entries become Kafka control batches that clients skip, so they show up as offset gaps the way transaction markers already do. Needs client verification (§9, M0) |
| S4 | Every node holds a replica of the controller state (voters on 3 or 5 nodes, learners elsewhere) | Metadata dissemination, separate members, topic and leader tables, most of the health monitor | Controller log replication to every node. Small: metadata only |
| S5 | Kafka-standard client quotas only | Throughput control groups, usage accounting | Loses Redpanda-specific quota features |
| S6 | Small operations surface | Debug bundle, crash tracker, self-test, phone-home metrics reporter, fault-injection endpoints, most of the Admin API, ~290 config properties | Tools built against those Redpanda endpoints stop working (§4.11) |
| S7 | Fewer balancing modes: placement on create, greedy leader balancing, moves on decommission and node failure | Continuous partition auto-balancing planner, core balancing | No continuous rebalancing for skewed load in v1 |
| S8 | One cluster version number instead of 48 feature flags | Feature table, migrators, gated code paths | Behaviour changes switch on per release rather than per feature |
| S9 | Fixed number of transaction coordinator partitions | `tx_manager_migrator`, recovery mode | Repartitioning the coordinator topic needs a new cluster |

**What does not get simpler:** raft, transactions, consumer groups,
compaction, fetch long-polling and produce validation. These are where the
correctness work goes, and they set the schedule.

## 4. Design

### 4.1 Process and concurrency

One process, one tokio multi-thread runtime. Each partition replica is a
tokio task that owns its log, raft node and state machines, and is driven
by a bounded `mpsc` channel: produce, replication messages, ticks,
config changes. Reads don't go through the actor. Fetches read immutable
segment files concurrently, through a cheap `Arc` snapshot of the segment
list and committed offset that the actor publishes on a `watch` channel.
Long-polling fetches wait on that same channel.

Backpressure is explicit: per-connection and global semaphores for
in-flight produce bytes and fetch response bytes, sized from the memory
budget.

Prioritisation without scheduling groups: raft traffic and heartbeats use
their own connections and are never queued behind Kafka requests. If
measurement shows it is needed, they get a second small runtime.

### 4.2 Storage

- **Layout.** One directory per partition. Each segment is a file of Kafka
  v2 batches stored exactly as received, so fetch can send bytes straight
  from file to socket. Alongside each segment: a sparse offset index and a
  sparse time index (Kafka's layout, which is well understood).
- **Writes.** Appends use buffered `pwrite`. The partition actor coalesces
  fsyncs: one `fdatasync` covers every append queued since the last one
  (group commit). `acks=all` and raft votes wait on the fsync. File I/O
  runs on a small blocking pool behind a `LogFile` trait. io_uring can be
  added behind that trait later if it pays for itself.
- **Recovery.** On startup, scan only the tail segment and truncate it at
  the first bad batch CRC.
- **Retention and compaction.** Retention by size and time. Compaction
  works like Kafka's cleaner: build a key → latest offset map over the
  dirty range, rewrite segments, swap atomically, honour
  `delete.retention.ms` for tombstones. One algorithm, not Redpanda's
  sliding-window plus adjacent-segment variants.
- **Node-local metadata** goes in one `redb` database per node: raft hard
  state, applied indexes, snapshot metadata. That replaces the custom
  kvstore and its key spaces.

### 4.3 One offset space

Today the raft log contains six batch types that aren't Kafka data (raft
configuration, prefix truncation, partition property updates, version
fences and others), so every read and write translates between raft
offsets and Kafka offsets.

Instead, every raft entry is a Kafka batch. Data batches are data. Raft
configuration and leader no-op entries are Kafka **control batches**
(attribute bit 5) with a Redpanda-specific control type. Clients already
skip control batches: transaction commit and abort markers are control
batches, and consumers step over them today. The leader epoch Kafka uses
in `OffsetForLeaderEpoch` and fetch is the raft term.

The visible effect: offsets have gaps where raft entries sit (one per
leader election or membership change), exactly like transaction markers.
Lag calculations already handle that. Kafka-visible behaviour is otherwise
unchanged, and an entire translation layer disappears.

### 4.4 Replication

- **Model.** One raft group per partition, as today. It gives the
  guarantees in §1, and per-partition leadership scales across nodes.
- **Raft core.** A sans-IO implementation of about 3–4k lines:
  - pre-vote, joint-consensus membership changes, learners;
  - leadership transfer, priority-based elections;
  - follower recovery from log or snapshot.

  It is modelled on etcd raft and `raft-rs`, and backed by a TLA+ spec and
  simulation (§7).

  Why not a library? `raft-rs` has not published a release since 0.7.0 in
  2023 (TiKV builds it from git). `openraft` is maintained but runs one
  set of tasks per group with per-group heartbeats, which doesn't fit tens
  of thousands of groups per node. Both should be evaluated in M0; the
  sans-IO core is the default.
- **Multi-raft driver.** Per node, one driver multiplexes all local groups:
  - It batches heartbeats per node pair: one message carries every group's
    heartbeat for that peer, as Redpanda's `heartbeat_manager` does.
  - It sends append-entries over a dedicated connection per peer.
  - It coalesces ticks so idle groups cost nothing.
- **Transport.** One framed binary protocol over TCP (`tokio-util` codec,
  versioned message enum, zero-copy `Bytes` for batch payloads), used for
  raft traffic and for forwarding requests to partition and coordinator
  leaders. No RPC framework and no code generator.

### 4.5 Controller

- **State.** The controller is one raft group whose state machine is a
  plain Rust struct, `ClusterState`: brokers, topics, partition
  assignments, cluster config, quotas, cluster version. It is changed only
  by applying a `Command` enum.
- **Replicas.** Every node holds a replica (voters on 3 or 5 nodes,
  learners on the rest), so Kafka `Metadata` and config reads are served
  from local state.
- **Leadership.** Partition leadership isn't controller state. It travels
  as a small leader table piggybacked on the node-pair heartbeats.
- **Reconciler.** Each node runs one loop that compares `ClusterState`
  with the replicas it hosts, then creates, deletes, reconfigures or moves
  them. It replaces `controller_backend`, the topic updates dispatcher, the
  members backend and the drain manager with one diff-and-act loop.
- **Health.** Node liveness comes from raft transport heartbeats; disk
  usage is reported in them. A separate health monitor isn't needed.

### 4.6 Kafka layer

- **Protocol.** Encoding and decoding come from the `kafka-protocol` crate
  (generated from Apache Kafka's own JSON schemas, all API versions). The
  broker owns connection handling and dispatch: one handler module per
  API, written against typed requests.
- **Produce.** Validate the batch, check idempotence and dedup in the
  partition actor, append through raft, reply when the requested acks are
  met.
- **Fetch.** Fetch sessions (KIP-227), reads from segment files bounded by
  the response byte budget, and long polling on the partition's `watch`
  channel. Requests for partitions this node doesn't lead get
  `NOT_LEADER_OR_FOLLOWER`, as in Kafka and in Redpanda today. The
  `kafka_data` RPC isn't ported: its only remaining caller was audit
  logging.

### 4.7 Consumer groups, idempotence and transactions

These follow Apache Kafka's server design closely, because clients are
written against its behaviour.

- **Group coordinator.** A sans-IO state machine per `__consumer_offsets`
  partition, covering:
  - the classic rebalance protocol and static membership;
  - offset commit, expiry and deletion.

  Group state is rebuilt from the compacted partition on leader change.
- **Producer state.** Per-partition producer ID → sequence and epoch
  tracking, part of the partition actor's state machine and included in
  its snapshot.
- **Transaction coordinator.** A sans-IO state machine per
  `__transaction_state` partition, implementing KIP-98 and the
  epoch-bumping defence from KIP-890. It writes commit and abort markers
  as control batches; the LSO and aborted-transaction index are computed
  per partition.

### 4.8 Windowed dedup

Ported as the RFC describes: the window filter lives in the partition
actor. Followers rebuild it by applying data batches, and it is consulted
only on the leader, before replication. With one offset space (§4.3) and
one owner per partition, it needs no separate STM plumbing. It is the
smallest component and nearly 1:1 with today's design.

### 4.9 Placement and balancing

- **Placement on create.** Rack-aware, least-loaded by replica count and
  disk use.
- **Leader balancing.** Greedy, toward an even leader count per node.
- **Moves.** On decommission and after a node has been unreachable for a
  configurable time, replicas move using joint consensus.
- **Maintenance mode.** Leaders drain off the node and it stops being a
  placement target.

Continuous load-based rebalancing and leadership pinning are left out of v1
(§10).

### 4.10 Quotas

Kafka-standard client quotas (`producer_byte_rate`, `consumer_byte_rate`
per user or client ID, with users effectively anonymous). Served through
DescribeClientQuotas and AlterClientQuotas, enforced with throttle times in
responses as Kafka does.

### 4.11 Operations surface

- **Admin API.** `axum` on the admin port, about 20 routes:
  - Status and probes: readiness, liveness.
  - Brokers: list, decommission, recommission, maintenance on and off.
  - Cluster config: get, patch, schema, status.
  - Node config.
  - Partitions: list, move replicas, transfer leadership.
  - Cluster health overview.
  - Helm compatibility: `PUT /v1/features/license` (accepted, ignored)
    and `PUT /v1/debug/restart_service` (succeeds, does nothing).

  These are the Helm chart job's six calls, plus the routes `rpk` uses for
  cluster config, broker and maintenance commands. The exact `rpk` set is
  recorded in M0 by capturing `rpk` traffic against the C++ broker.
- **Config.** One typed `Config` struct (serde) with about 60 properties:
  - Node config is read from the same `redpanda.yaml` layout `rpk` writes
    (data directory, node ID, seed servers, listeners, advertised
    addresses).
  - Cluster config lives in `ClusterState`.
  - The full v26.2.x property name list is kept as data. A legacy property
    set to its old default is accepted with a warning. A legacy property
    set to anything else is rejected, so nothing is silently ignored. That
    covers the Helm chart, which sends whole config blocks, and it follows
    the precedent `926f87a6` set for `cloud_storage_enabled`.
- **Process flags.** The binary accepts the flags `rpk redpanda start`
  passes:
  - `--smp` sets the worker thread count.
  - `--memory` and `--reserve-memory` set the memory budget.
  - `--overprovisioned`, `--lock-memory`, `--thread-affinity` and
    `--io-properties-file` are accepted and logged.
  - `--unsafe-bypass-fsync` is honoured.
- **Metrics.** Prometheus on `/public_metrics` with the ~30 `redpanda_*`
  names and labels that `rpk generate grafana-dashboard` uses, plus
  whatever new metrics the Rust components need.
- **Logs.** `tracing` with structured fields. Panics abort and leave a core
  dump.

### 4.12 Upgrades

The cluster has one version number: the minimum binary version across
nodes, stored in `ClusterState`.

- New on-disk or wire formats are written only once every node is at the
  required version.
- Decoders always accept the previous version.
- CI runs N-1 → N rolling upgrades from the first release.

## 5. What each C++ area becomes

| Area | C++ lines | Rust-native approach | Rust estimate |
|---|---:|---|---:|
| Runtime and foundations (`ssx`, `bytes`, containers, utils, `resource_mgmt`) plus Seastar | 25,616 + Seastar | tokio, `bytes`, std | ~2,000 |
| Codecs and model (Kafka protocol, model, serde, compression) | 15,208 | `kafka-protocol` crate, serde with versioned enums, compression crates | ~3,000 |
| Config | 8,056 | Typed serde struct, ~60 properties, legacy-name policy | ~2,000 |
| Networking and internal RPC (`net`, `rpc`, `http`) | 8,860 | tokio, one framed transport | ~2,500 |
| Storage | 29,196 | Page cache, Kafka-style segments and cleaner, `redb` | ~8,000 |
| Raft | 21,440 | Sans-IO core plus multi-raft driver | ~6,000 |
| Controller and cluster core | 40,665 | `ClusterState` + `Command`, one reconciler per node | ~8,000 |
| Placement and balancing | 11,419 | Placement, greedy leaders, decommission and failure moves | ~2,500 |
| Kafka server (connections, produce, fetch, metadata, topic admin) | 23,214 | Handler per API on `kafka-protocol` types | ~10,000 |
| Client quotas | 3,686 | Kafka-standard byte-rate quotas | ~800 |
| Consumer groups | 10,490 | Sans-IO coordinator per offsets partition | ~4,500 |
| Transactions and idempotence | 14,293 | Sans-IO coordinator per Kafka's design; producer state in the partition actor | ~6,000 |
| Windowed dedup | 1,767 | Same design, inside the partition actor | ~1,500 |
| Admin API | 14,125 | `axum`, ~20 routes | ~2,500 |
| Operations and app wiring | 13,171 | Metrics, startup, CLI flags; the rest dropped | ~2,500 |
| **Total** | **241,206** | | **~62,000** |

The estimates are judgement, not measurement; read them as 50–80k. Tests
come on top. Expect roughly as much test code again, much of it simulation
and property tests rather than ported fixtures.

## 6. Workspace layout

| Crate | Contents |
|---|---|
| `rp-log` | Segments, indexes, retention, compaction, recovery; `LogFile` trait with real and fault-injecting in-memory backends |
| `rp-raft` | Sans-IO raft core |
| `rp-cluster` | `ClusterState`, `Command`, reconciler, placement, balancing |
| `rp-partition` | Partition actor: log + raft node + producer state, transactions, dedup; multi-raft driver |
| `rp-kafka` | Connection handling, dispatch, per-API handlers, group and transaction coordinators, quotas |
| `rp-transport` | Framed node-to-node protocol |
| `rp-admin` | Admin API, metrics, config |
| `redpanda` | Binary: flag parsing, startup, shutdown |
| `rp-sim` | Deterministic cluster simulation (test-only) |

Nine crates, a shallow graph that compiles in parallel.

## 7. How the guarantees are proven

1. **Sans-IO unit and property tests.** Raft, group coordinator,
   transaction coordinator, producer state and dedup are pure state
   machines, so `proptest` drives them through millions of event sequences
   and checks invariants directly.
2. **Deterministic cluster simulation.** Whole clusters run in one thread:
   - network simulated with `turmoil` (maintained, tokio-native);
   - disk simulated with the fault-injecting `LogFile` backend (torn
     writes, lost unsynced data, EIO);
   - time controlled by the simulator.

   Any failing seed reproduces exactly.
3. **Model checking.** TLA+ specs for raft membership changes and the
   transaction coordinator, with simulation traces checked against them.
4. **Black-box checks with real clients.** Run the Java and Go verifiers
   (`tests/java`, `tests/go`) and the pruned ducktape suite. Add a
   Jepsen-style linearizability and EOS checker on real 3- and 5-node
   clusters with process kills and network partitions.
5. **C++ as the reference.** Run the same workloads against the C++ and
   Rust brokers and compare what clients see: fetched data, offsets
   modulo control-batch gaps, group behaviour, transaction outcomes,
   error codes.
6. **Client compatibility matrix.** franz-go, librdkafka, the Java client,
   sarama and kafka-python, plus Kafka CLI tools, Kafka Streams and Flink
   (for EOS), across every supported API version.
7. **Fuzzing** every decoder: Kafka requests, transport frames, segment and
   index parsing, config.

## 8. Performance

Seastar's thread-per-core design is Redpanda's main performance argument,
and this proposal gives it up for simplicity. Expected outcome (a judgement
to be tested in M0, not a measurement):

- **Throughput.** Should be close. The work is dominated by syscalls,
  fsync and copying, and fetch sends bytes from the page cache.
- **p99 latency at moderate load.** Should be comparable.
- **p99.9 under saturation, and very high partition counts per core.**
  Likely worse: work stealing and the page cache give up the control
  Seastar has.

Proposed gates, to be agreed: produce and fetch throughput ≥ 80% of C++ on
the same hardware; `acks=all` p99 ≤ 1.5× C++ at 50% of max throughput;
10k partitions per node without idle CPU over 5%. If M0 shows these can't
be met with tokio, the fallback is a thread-per-core runtime behind the
same partition-actor design. That costs complexity, but the actor
boundaries make it a contained change.

## 9. Milestones

Rough, assuming 4–6 engineers who are strong in Rust and distributed
systems.

| Milestone | Months | Exit criteria |
|---|---|---|
| M0: Spikes and decisions | 0–3 | Single-node produce and fetch on tokio vs C++ benchmark; client matrix accepts raft control batches (§4.3); raft core choice; `rpk` and Helm Admin API traffic recorded |
| M1: Single node | 2–6 | Log, compaction, retention, single-replica partitions, Kafka produce, fetch, metadata, topic admin; clients produce and consume |
| M2: Replication | 4–10 | Sans-IO raft, multi-raft driver, transport, controller with `ClusterState` and reconciler; 3-node cluster survives fault simulation and Jepsen-style runs |
| M3: Kafka completeness | 8–14 | Consumer groups, idempotence, dedup, quotas, offset deletion, delete records |
| M4: Transactions | 12–18 | Transaction coordinator, LSO, read_committed; Kafka Streams and Flink EOS tests pass |
| M5: Operations and hardening | 15–22 | Admin API, config, metrics, Helm chart install and upgrade, `rpk` workflows, performance gates, soak runs, N-1 → N upgrade |

Total roughly 8–12 engineer-years, versus 20–28 for the parity port. Raft
and transactions set the critical path, as before.

## 10. Risks and trade-offs

| Risk | Mitigation |
|---|---|
| Performance below expectations on tokio | M0 benchmark before committing; partition actors keep a thread-per-core fallback contained |
| A widely used client mishandles the raft control batches | M0 client matrix; fallback is a much smaller offset translation than today's (only raft entries, not six batch types) |
| Behaviour differences from Redpanda that users notice (offset gaps, missing Admin API routes, fewer config properties) | Document them; the C++ broker is the reference for anything a Kafka client sees |
| Own raft core | Sans-IO plus property tests, simulation, TLA+, Jepsen-style runs; evaluate `raft-rs` and `openraft` in M0 |
| Page cache under heavy mixed load (cold reads evicting the hot tail) | Measure in M5; `posix_fadvise` hints; an application-level cache can be added later if needed |
| Losing continuous balancing | Add later as an offline planner over `ClusterState`; the data model supports it |
| Licensing: a design derived from BSL-licensed code | The native design copies less code than a port, but get the same legal advice as for `PLAN.md` |

## 11. Decisions needed

1. **Native or parity?** This proposal (recommended) or `PLAN.md`. If this
   one, `PLAN.md` becomes the fallback and `INVENTORY.md` stays as the map
   of what is being replaced.
2. **Runtime.** tokio multi-thread (recommended) or thread-per-core from the
   start.
3. **Operations compatibility.** Helm chart and `rpk` compatibility shim
   (recommended) or no Redpanda tooling compatibility at all.
4. **Transactions in the first production release?** Recommended: yes. Java
   clients have enabled idempotence by default since Kafka 3.0, and EOS
   users can't move without it. The alternative is shipping M3 and adding
   transactions after.
5. **Balancing scope for v1.** Placement, greedy leader balancing,
   decommission and failure moves only (recommended), or also continuous
   rebalancing and leadership pinning.
6. **Performance bar.** The gates in §8 (recommended) or strict parity with
   C++.
