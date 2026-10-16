# Plan: a Rust-native broker

**Status: chosen direction (decisions recorded 2026-10-13, §11).** This
replaces the parity port in `PLAN.md`, which is kept only as a reference.
`INVENTORY.md` remains the map of the C++ code being replaced.

The broker keeps what Kafka clients and the data depend on. It is designed
the way a Rust codebase would naturally be built, and it deletes what only
exists because of C++, Seastar or Redpanda's commercial product.
Compatibility with Redpanda's own tooling (Helm chart, operator, `rpk`
admin commands, config files, metric names) is not a goal: the broker ships
its own. The v1 target is roughly **60k lines of Rust** (estimate, §5).

Line counts below are from `INVENTORY.md` (production C++, `v26.2.x` at
`9daa2fab`).

## Summary

| | Parity port (`PLAN.md`, not chosen) | Rust-native (this plan) |
|---|---|---|
| Kafka protocol | 38 APIs | 32 APIs in v1; the 6 transaction APIs follow in v1.1 |
| Data guarantees (raft safety, `acks=all` fsync, idempotence, compaction, dedup G1–G5) | same | same |
| Transactions (exactly-once) | yes | after v1 (§4.7) |
| Concurrency model | Thread-per-core, Seastar re-implemented | Standard tokio multi-thread runtime, one task per partition |
| Disk I/O | O_DIRECT plus Redpanda's own caches | Buffered I/O, OS page cache, explicit fsync |
| Kafka offsets vs raft log | Two offset spaces with a translator | One offset space |
| Cluster metadata | Controller plus dissemination plus per-node tables | One replicated `ClusterState` on every node |
| Balancing | Full set | Full set: placement, leader balancing, continuous rebalancing, leadership pinning, decommission, failure moves, maintenance mode |
| Config | 350 Redpanda properties | ~60 properties, own format, Apache Kafka names where Kafka has an equivalent |
| Admin API and tooling | Redpanda's Admin API, `rpk`, Helm chart | Own small HTTP API, CLI, Helm chart and Grafana dashboard |
| Upgrades | Feature-flag table | One cluster version number |
| Rust size (estimate) | 140–190k lines | ~60k for v1, ~63k with transactions |
| Effort (rough) | 20–28 engineer-years | 7–10 engineer-years for v1, plus 1.5–2 for transactions |

What this gives up:
- **Performance.** Some peak throughput and tail latency compared with
  Seastar on the same hardware. The bar is the gates in §8, not parity.
- **Redpanda tooling.** Nothing built for Redpanda's Admin API, config or
  metrics will work.
- **Exactly-once users.** They can't move until transactions land.

## 1. What stays fixed

1. **The Kafka protocol.** v1 serves 32 of the 38 APIs listed in
   `INVENTORY.md`, at the same version ranges, with record batch format v2
   and Kafka's error codes. The six transaction-only APIs follow in v1.1
   (§4.7). Existing clients, Kafka CLI tools, Kafka Connect and Kafka
   Streams (at-least-once) work unchanged. `rpk`'s Kafka-protocol commands
   (`topic`, `group`, `produce`, `consume`) should also keep working,
   because they only speak Kafka.
2. **The data guarantees:**
   - Raft safety.
   - `acks=all` is acknowledged only after a majority has fsynced.
   - Idempotent producers.
   - Compaction and retention semantics.
   - Consumer group offset durability.
   - Windowed dedup per guarantees G1–G5 and bounds B1–B5 of
     `docs/rfcs/20260730_log_derived_dedup_state.md`.

   Exactly-once transactions join this list in v1.1.
3. **Topic configs.** Kafka's standard names and meanings, plus
   `redpanda.dedup.window.ms` and `redpanda.dedup.key.header`.

Everything else (process flags, config files, the admin interface, metric
names, deployment) is designed fresh in §4.11.

## 2. Design rules

- **Library first.** Use a maintained crate wherever one exists:
  - Kafka protocol: `kafka-protocol`.
  - Runtime: `tokio`.
  - HTTP: `axum`.
  - Embedded KV: `redb`.
  - Metrics: `prometheus-client`.
  - Logging: `tracing`.
  - CLI: `clap`.
  - Compression: `zstd`, `lz4_flex`, `snap`, `flate2`.
  - Checksums: `crc32c`.

  Write code only where the broker's value is: replication, the log, Kafka
  semantics, balancing.
- **Sans-IO cores.** Raft, the group coordinator, producer state, dedup,
  the balancer planners and (later) the transaction coordinator are pure
  state machines: `step(event) -> actions`, no I/O, no clocks, no tasks.
  Drivers do the I/O. Everything that has to be correct can be tested
  deterministically without a simulator.
- **One owner per piece of state.** Each partition is a tokio task that owns
  its log, raft state and state machines, and is driven by a channel. No
  locks on the hot path, no `Arc<Mutex<…>>` webs.
- **Delete before porting.** A C++ component is only rebuilt if a Kafka
  client, a data guarantee, or running the cluster needs it. Tuning knobs
  that exist because of Seastar or Redpanda's caches go away with them.
- **Versioned from day one.** Every on-disk structure and internal message
  carries a version; the cluster has one version number (§4.12).

## 3. Simplifications, and what each one removes

| # | Simplification | Removes | Cost |
|---|---|---|---|
| S1 | Standard tokio multi-thread runtime instead of thread-per-core | Seastar re-implementation, `sharded<>`, cross-core message passing, scheduling and memory groups, shard placement and core balancing (2,439 lines), cross-core request routing | Less control over scheduling and tail latency (§8) |
| S2 | Buffered I/O and the OS page cache instead of O_DIRECT plus own caches | `batch_cache`, `chunk_cache`, `readers_cache`, the DMA appender, the I/O scheduler, and their tuning knobs | Less control over memory under mixed read and write load. The page cache is how Apache Kafka works, so it is proven at scale |
| S3 | One offset space: raft log index = Kafka offset | The offset translator (1,021 lines) and the Kafka-offset vs log-offset split that runs through storage, raft and Kafka | Raft configuration entries become Kafka control batches that clients skip, so they show up as offset gaps the way transaction markers already do. Needs client verification (M0) |
| S4 | Every node holds a replica of the controller state (voters on 3 or 5 nodes, learners elsewhere) | Metadata dissemination, separate members, topic and leader tables, most of the health monitor | Controller log replication to every node. Small: metadata only |
| S5 | Kafka-standard client quotas only | Throughput control groups, usage accounting | Loses Redpanda-specific quota features |
| S6 | Own small operations surface, no Redpanda tooling compatibility | Redpanda's Admin API (~95 v1 operations plus v2), ~290 config properties, debug bundle, crash tracker, self-test, phone-home reporter, fault-injection endpoints, Seastar process flags | Helm chart, operator, `rpk` admin commands and Redpanda dashboards stop working; replaced by our own (§4.11) |
| S7 | One cluster version number instead of 48 feature flags | Feature table, migrators, gated code paths | Behaviour changes switch on per release rather than per feature |
| S8 | Transactions after v1, fixed number of coordinator partitions | `tx_manager_migrator`, recovery mode; the transaction coordinator moves out of the v1 critical path | Exactly-once users wait for v1.1 |

**What does not get simpler:** raft, consumer groups, compaction, fetch
long-polling, produce validation, the balancer, and (in v1.1) transactions.
These are where the correctness work goes, and they set the schedule.

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
(attribute bit 5) with a broker-specific control type. Clients already
skip control batches: transaction commit and abort markers are control
batches, and consumers step over them today. When transactions arrive,
their markers are ordinary Kafka control batches in the same space. The
leader epoch Kafka uses in `OffsetForLeaderEpoch` and fetch is the raft
term.

The visible effect: offsets have gaps where raft entries sit (one per
leader election or membership change), exactly like transaction markers.
Lag calculations already handle that. Kafka-visible behaviour is otherwise
unchanged, and an entire translation layer disappears.

### 4.4 Replication

- **Model.** One raft group per partition. It gives the guarantees in §1,
  and per-partition leadership scales across nodes and is what the
  balancer moves.
- **Raft core.** A sans-IO implementation of about 3–4k lines:
  - pre-vote, joint-consensus membership changes, learners;
  - leadership transfer, priority-based elections (used by leadership
    pinning);
  - follower recovery from log or snapshot.

  It is modelled on etcd raft and `raft-rs`, and backed by a TLA+ spec and
  simulation (§7).

  Why not a library? `raft-rs` has not published a release since 0.7.0 in
  2023 (TiKV builds it from git). `openraft` is maintained but runs one
  set of tasks per group with per-group heartbeats, which doesn't fit tens
  of thousands of groups per node. Both are evaluated in M0; the sans-IO
  core is the default.
- **Multi-raft driver.** Per node, one driver multiplexes all local groups:
  - It batches heartbeats per node pair: one message carries every group's
    heartbeat for that peer, as Redpanda's `heartbeat_manager` does.
  - It sends append-entries over a dedicated connection per peer.
  - It coalesces ticks so idle groups cost nothing.
  - It enforces a node-wide recovery bandwidth limit, so replica moves
    from the balancer can't starve live traffic.
- **Transport.** One framed binary protocol over TCP (`tokio-util` codec,
  versioned message enum, zero-copy `Bytes` for batch payloads), used for
  raft traffic and for forwarding requests to partition and coordinator
  leaders. No RPC framework and no code generator.

### 4.5 Controller

- **State.** The controller is one raft group whose state machine is a
  plain Rust struct, `ClusterState`: brokers (with rack), topics, partition
  assignments, in-progress moves, cluster config, quotas, leadership
  preferences, cluster version. It is changed only by applying a `Command`
  enum.
- **Replicas.** Every node holds a replica (voters on 3 or 5 nodes,
  learners on the rest), so Kafka `Metadata` and config reads are served
  from local state.
- **Leadership.** Partition leadership isn't controller state. It travels
  as a small leader table piggybacked on the node-pair heartbeats.
- **Reconciler.** Each node runs one loop that compares `ClusterState`
  with the replicas it hosts, then creates, deletes, reconfigures or moves
  them. It replaces `controller_backend`, the topic updates dispatcher, the
  members backend and the drain manager with one diff-and-act loop.
- **Health.** Node liveness comes from raft transport heartbeats. Each node
  also sends the controller leader a periodic report of disk usage and
  per-partition sizes, which the balancer needs (§4.9).

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

**In v1:**

- **Group coordinator.** A sans-IO state machine per `__consumer_offsets`
  partition, covering:
  - the classic rebalance protocol and static membership;
  - offset commit, expiry and deletion.

  Group state is rebuilt from the compacted partition on leader change.
- **Idempotent producers.** `InitProducerId` without a transactional ID,
  plus per-partition producer ID → sequence and epoch tracking, which is
  part of the partition actor's state machine and its snapshot. This is in
  v1 because modern clients turn it on by default (Java clients since
  Kafka 3.0), so most producers depend on it.
- **Transactions are refused clearly.**
  - `ApiVersions` does not advertise AddPartitionsToTxn, AddOffsetsToTxn,
    EndTxn, TxnOffsetCommit, DescribeTransactions or ListTransactions.
  - `FindCoordinator` for a transaction coordinator returns an error.
  - `InitProducerId` with a transactional ID fails with a fatal error, so
    a transactional producer stops at start-up rather than part-way
    through. M3 picks the exact error code by checking how the Java client,
    librdkafka and franz-go report each option.
  - `read_committed` consumers work: with no open transactions, the LSO
    equals the high watermark.

**In v1.1: transactions.** A sans-IO coordinator per `__transaction_state`
partition, implementing KIP-98 and the epoch-bumping defence from KIP-890.
Commit and abort markers are written as control batches; the LSO and
aborted-transaction index are computed per partition; transactional offset
commits go to the group coordinator.

v1 leaves room for this. Producer state already carries epochs and is
versioned. The one offset space already treats control batches as
first-class. The fetch path already computes an LSO, which is equal to the
high watermark until transactions exist.

### 4.8 Windowed dedup

Ported as the RFC describes: the window filter lives in the partition
actor. Followers rebuild it by applying data batches, and it is consulted
only on the leader, before replication. Idempotent produce bypasses it,
and so will transactional produce. With one offset space (§4.3) and one
owner per partition, it needs no separate STM plumbing. It is the smallest
component and nearly 1:1 with today's design.

### 4.9 Placement and balancing

The full set from the C++ broker, minus core balancing (there are no
per-core shards under tokio):

- **Placement on create.** Rack-aware, least-loaded by replica count and
  disk use.
- **Leader balancing.** Greedy, toward an even leader count per node,
  subject to leadership pinning.
- **Leadership pinning.** Cluster-wide and per-topic preferred racks for
  leaders. Enforced by the leader balancer and by raft priority-based
  elections, so leaders return to preferred racks after failures.
- **Continuous rebalancing.** A sans-IO planner over `ClusterState` and the
  node reports (§4.5). It produces replica moves:
  - to keep disk usage under a configurable threshold;
  - to even out replica counts after a node joins;
  - to restore replication after a node has been unreachable for a
    configurable time.

  Moves run through joint consensus, with a cap on concurrent moves and the
  node-wide recovery bandwidth limit (§4.4).
- **Decommission and recommission**, including draining all replicas off a
  node.
- **Maintenance mode.** Leaders drain off the node and it stops being a
  placement or leadership target.

The planner is tested against recorded and generated cluster states, plus
simulation of whole clusters with nodes joining, failing and filling up.

### 4.10 Quotas

Kafka-standard client quotas (`producer_byte_rate`, `consumer_byte_rate`
per user or client ID, with users effectively anonymous). Served through
DescribeClientQuotas and AlterClientQuotas, enforced with throttle times in
responses as Kafka does.

### 4.11 Operations surface

Designed fresh. No compatibility with Redpanda's Admin API, config files,
process flags, metric names, Helm chart or operator.

- **Config.**
  - Node config: one TOML file (node ID, data directories, listeners,
    advertised addresses, seed nodes, rack, memory budget, worker
    threads), with environment-variable overrides.
  - Cluster config: about 60 properties, stored in `ClusterState` and
    changed through the admin API.
  - Naming: where Apache Kafka has a broker setting for the same thing,
    the property uses its name and meaning (`log.retention.ms`,
    `num.partitions`, `default.replication.factor`, and so on).
  - Validation: one typed serde struct. Unknown properties are rejected.
- **Command line.** `clap`: config path and a few overrides.
- **Admin API.** HTTP/JSON with `axum` under `/v1`, about 20 routes, with
  an OpenAPI spec generated from the handlers:
  - health and readiness;
  - brokers: list, decommission, recommission, maintenance on and off;
  - cluster config: get, patch, schema;
  - partitions: list, move replicas, transfer leadership;
  - balancer status;
  - cluster version and upgrade status.
- **CLI.** A small `rpctl` binary over the admin API, for the cluster
  operations Kafka tools can't do: brokers, maintenance, decommission,
  config, partition moves, balancer status. Topic, group, produce and
  consume work stays with Kafka tools (and with `rpk`'s Kafka commands).
- **Metrics.** Prometheus on `/metrics`, own names, plus a Grafana
  dashboard in the repo.
- **Deployment.** A container image, a minimal Helm chart (StatefulSet,
  services, config map, readiness probe on the admin API, a pre-stop hook
  that enters maintenance mode), and a systemd unit for bare metal.
- **Logs.** `tracing` with structured fields. Panics abort and leave a core
  dump.

### 4.12 Upgrades

The cluster has one version number: the minimum binary version across
nodes, stored in `ClusterState`.

- New on-disk or wire formats are written only once every node is at the
  required version.
- Decoders always accept the previous version.
- CI runs N-1 → N rolling upgrades from the first release.

v1.1 turns transactions on once the whole cluster runs it.

## 5. What each C++ area becomes

| Area | C++ lines | Rust-native approach | Rust estimate |
|---|---:|---|---:|
| Runtime and foundations (`ssx`, `bytes`, containers, utils, `resource_mgmt`) plus Seastar | 25,616 + Seastar | tokio, `bytes`, std | ~2,000 |
| Codecs and model (Kafka protocol, model, serde, compression) | 15,208 | `kafka-protocol` crate, serde with versioned enums, compression crates | ~3,000 |
| Config | 8,056 | Typed serde struct, ~60 properties | ~1,500 |
| Networking and internal RPC (`net`, `rpc`, `http`) | 8,860 | tokio, one framed transport | ~2,500 |
| Storage | 29,196 | Page cache, Kafka-style segments and cleaner, `redb` | ~8,000 |
| Raft | 21,440 | Sans-IO core plus multi-raft driver with recovery throttling | ~6,000 |
| Controller and cluster core | 40,665 | `ClusterState` + `Command`, one reconciler per node, node reports | ~8,000 |
| Placement and balancing | 11,419 | Full set: placement, leaders, pinning, continuous planner, decommission, maintenance | ~5,000 |
| Kafka server (connections, produce, fetch, metadata, topic admin) | 23,214 | Handler per API on `kafka-protocol` types | ~10,000 |
| Client quotas | 3,686 | Kafka-standard byte-rate quotas | ~800 |
| Consumer groups | 10,490 | Sans-IO coordinator per offsets partition | ~4,500 |
| Idempotence (v1) and transactions (v1.1) | 14,293 | Producer state in the partition actor; later a sans-IO transaction coordinator per Kafka's design | ~1,500 v1, +~4,500 v1.1 |
| Windowed dedup | 1,767 | Same design, inside the partition actor | ~1,500 |
| Admin API and CLI | 14,125 | `axum`, ~20 routes, OpenAPI; `rpctl` | ~3,000 |
| Operations and app wiring | 13,171 | Metrics, startup, config loading; the rest dropped | ~1,500 |
| **Total** | **241,206** | | **~59,000 v1, ~63,000 with transactions** |

The estimates are judgement, not measurement; read v1 as 50–75k. Tests
come on top. Expect roughly as much test code again, much of it simulation
and property tests rather than ported fixtures. Deployment assets (Helm
chart, dashboard, systemd unit) are not counted.

## 6. Workspace layout

| Crate | Contents |
|---|---|
| `rp-log` | Segments, indexes, retention, compaction, recovery; `LogFile` trait with real and fault-injecting in-memory backends |
| `rp-raft` | Sans-IO raft core |
| `rp-cluster` | `ClusterState`, `Command`, reconciler, placement, balancer planners |
| `rp-partition` | Partition actor: log + raft node + producer state + dedup; multi-raft driver |
| `rp-kafka` | Connection handling, dispatch, per-API handlers, group coordinator, quotas; later the transaction coordinator |
| `rp-transport` | Framed node-to-node protocol |
| `rp-admin` | Admin API, metrics, config |
| `redpanda` | Broker binary: startup and shutdown |
| `rpctl` | Operations CLI |
| `rp-sim` | Deterministic cluster simulation (test-only) |

Ten crates, a shallow graph that compiles in parallel.

## 7. How the guarantees are proven

1. **Sans-IO unit and property tests.** Raft, group coordinator, producer
   state, dedup and the balancer planners are pure state machines, so
   `proptest` drives them through millions of event sequences and checks
   invariants directly.
2. **Deterministic cluster simulation.** Whole clusters run in one thread:
   - network simulated with `turmoil` (maintained, tokio-native);
   - disk simulated with the fault-injecting `LogFile` backend (torn
     writes, lost unsynced data, EIO);
   - time controlled by the simulator.

   Any failing seed reproduces exactly. Balancer scenarios (nodes joining,
   failing, filling up, maintenance) run the same way.
3. **Model checking.** A TLA+ spec for raft membership changes in v1, and
   one for the transaction coordinator in v1.1. Simulation traces are
   checked against them.
4. **Black-box checks with real clients.** Run the Java and Go verifiers
   (`tests/java`, `tests/go`). Add a Jepsen-style linearizability and
   durability checker on real 3- and 5-node clusters with process kills,
   network partitions and disk-full. In v1.1, add exactly-once checks with
   Kafka Streams (`exactly_once_v2`) and Flink.
5. **C++ as the reference.** Run the same workloads against the C++ and
   Rust brokers and compare what Kafka clients see: fetched data, offsets
   modulo control-batch gaps, group behaviour, error codes. Only the Kafka
   protocol is compared, since everything else is deliberately different.
6. **Client compatibility matrix.** franz-go, librdkafka, the Java client,
   sarama and kafka-python, plus Kafka CLI tools and Kafka Connect, across
   every supported API version.
7. **Fuzzing** every decoder: Kafka requests, transport frames, segment and
   index parsing, config.

The ducktape suite is not reused. It drives the broker through Redpanda's
Admin API, config and `rpk`, none of which carry over. Its Kafka-level
scenarios are a useful checklist for the black-box tests.

## 8. Performance gates

Strict parity with C++ is not a goal. Seastar's thread-per-core design is
Redpanda's main performance argument, and this plan gives it up for
simplicity. Expected outcome (a judgement to be tested in M0, not a
measurement):

- **Throughput.** Should be close. The work is dominated by syscalls,
  fsync and copying, and fetch sends bytes from the page cache.
- **p99 latency at moderate load.** Should be comparable.
- **p99.9 under saturation, and very high partition counts per core.**
  Likely worse: work stealing and the page cache give up the control
  Seastar has.

Gates, measured against C++ on the same hardware from M1 onwards:

| Benchmark | Gate |
|---|---|
| Produce and fetch throughput | ≥ 80% of C++ |
| `acks=all` p99 at 50% of max throughput (3 nodes, RF=3) | ≤ 1.5× C++ |
| 10k partitions per node, idle | < 5% CPU |
| Recovery after node restart | ≤ 1.5× C++ |

If M0 shows tokio can't meet them, the options are tuning the hot paths
(dedicated runtime for replication, io_uring behind `LogFile`) or
revisiting the runtime. That would come back to you as a decision.

## 9. Milestones

Rough, assuming 4–6 engineers who are strong in Rust and distributed
systems.

| Milestone | Months | Exit criteria |
|---|---|---|
| M0: Spikes and decisions | 0–3 | Single-node produce and fetch on tokio vs C++ benchmark; client matrix accepts raft control batches (§4.3); raft core choice |
| M1: Single node | 2–6 | Log, compaction, retention, single-replica partitions, Kafka produce, fetch, metadata, topic admin; clients produce and consume; performance gates tracked |
| M2: Replication | 4–10 | Sans-IO raft, multi-raft driver, transport, controller with `ClusterState` and reconciler; 3-node cluster survives fault simulation and Jepsen-style runs |
| M3: Kafka completeness | 8–13 | Consumer groups, idempotence, dedup, quotas, offset deletion, delete records, transactional producers refused cleanly |
| M4: Balancing | 10–15 | Placement, leader balancing, leadership pinning, continuous rebalancing, decommission, failure moves, maintenance mode; balancer simulations pass |
| M5: Operations and v1.0 | 13–18 | Admin API, `rpctl`, config, metrics and dashboard, container image, Helm chart, N-1 → N upgrades, soak runs, gates met: **v1.0** |
| M6: Transactions (v1.1) | 18–24 | Transaction coordinator, LSO and aborted-transaction index, the six transaction APIs; Kafka Streams `exactly_once_v2` and Flink exactly-once tests pass |

v1.0 is roughly 7–10 engineer-years; transactions add 1.5–2. Raft and the
balancer set the v1 critical path; transactions set v1.1's.

## 10. Risks and trade-offs

| Risk | Mitigation |
|---|---|
| Performance below the gates on tokio | M0 benchmark before committing; partition actors keep a runtime change contained |
| A widely used client mishandles the raft control batches | M0 client matrix; fallback is a much smaller offset translation than today's (only raft entries, not six batch types) |
| Exactly-once users (Kafka Streams `exactly_once_v2`, Flink exactly-once sinks, Connect exactly-once sources) can't move before v1.1 | Refuse transactional producers clearly at start-up (§4.7) so nothing fails half-way; v1 leaves room for transactions in producer state, offsets and fetch |
| No Redpanda tooling compatibility | Own CLI, Helm chart and dashboard are part of v1.0 (M5), not follow-ups |
| Own raft core | Sans-IO plus property tests, simulation, TLA+, Jepsen-style runs; evaluate `raft-rs` and `openraft` in M0 |
| Continuous rebalancing makes bad moves under churn | Sans-IO planner tested on recorded and generated states; move caps and recovery throttling; can be switched off by config |
| Page cache under heavy mixed load (cold reads evicting the hot tail) | Measure in M5; `posix_fadvise` hints; an application-level cache can be added later if needed |
| Licensing: a design derived from BSL-licensed code | The native design copies less code than a port, but get legal advice before starting |

## 11. Decisions

Recorded 2026-10-13:

| # | Question | Decision |
|---|---|---|
| 1 | Native redesign or parity port | **Native.** `PLAN.md` is kept for reference only |
| 2 | Runtime | **Standard tokio** multi-thread runtime |
| 3 | Compatibility with Redpanda tooling (Helm chart, operator, `rpk` admin commands, Admin API, config, metrics) | **None.** Own admin API, CLI, chart and dashboard (§4.11) |
| 4 | Transactions | **After v1** (v1.1, M6). Idempotence stays in v1 |
| 5 | Balancing | **Full set:** placement, leader balancing, leadership pinning, continuous rebalancing, decommission, failure moves, maintenance mode. Core balancing doesn't apply under tokio |
| 6 | Performance | **Gates in §8**, not strict parity |

Still open, to be settled in M0 and M3:

- Raft core: own sans-IO implementation (default) vs `raft-rs` or `openraft`.
- The exact error returned to transactional producers in v1 (§4.7).
- Final list of cluster config properties (target ~60).
