# Rewriting slim Redpanda in Rust: plan

Companion to `INVENTORY.md`, which has the feature map and line counts this
plan is sized from.

## Summary

- **What is being rewritten.** The slim broker in this tree: a distributed
  Kafka broker on local storage, based on v26.2.x. That is 240k lines of
  production C++ (plus 125k lines of C++ tests) across 60 features, plus the
  Seastar runtime underneath it. Authentication (SASL, mTLS, ACLs, RBAC,
  audit) and TLS are out of scope by decision; that removes 18.6k lines.
  Another 13.7k lines are already dead in this tree.
- **What "full parity and guarantees" means.** Byte-compatible Kafka
  protocol, internal RPC, and on-disk formats, plus the same config, Admin API
  and metrics surfaces. Proof is that a Rust node can join a running C++
  cluster, take over its data directory, and hand it back. The full contract
  is below.
- **Strategy.** Replace one node at a time, not the whole thing at once. The
  Rust broker is built to interoperate with the C++ broker at the RPC and disk
  level, so every phase can be tested against the real thing, and the final
  rollout is the same rolling replacement `tools/slim-checks/upgrade_test.sh`
  already does for the slim build.
- **Untouched.** `rpk` (Go) is a client of the Kafka and Admin APIs. The
  ducktape suite, the Java/Go verifiers, and the Kafka and RPC schema JSON
  files are reused as-is. They are the parity oracle.
- **Rough size.** On the order of 25–35 engineer-years: 6–10 strong
  Rust/distributed-systems engineers for 2.5–4 years, with the first
  mixed-cluster milestone around month 12–18. This is a rough estimate (see
  "Effort").

## 1. Scope

### In scope (keep, 240,499 lines)

Every `keep` row in `INVENTORY.md`: the Kafka API (38 APIs), Raft, the
storage engine, the controller and everything in `cluster/`, transactions
and idempotence, the Admin API v1 and v2 (minus security routes),
configuration, internal RPC, metrics, debug bundle, crash tracker, self-test,
syschecks, and the runtime foundations.

### Out of scope

| What | Why | Lines |
|---|---|---:|
| SASL PLAIN/SCRAM, mTLS principals, ephemeral credentials, breakglass | Authentication dropped | 3,563 |
| ACLs, RBAC, user/role management | No principals left to authorize | 7,417 |
| Audit logging | Records principals; nothing to record | 4,304 |
| TLS on every listener, OpenSSL, `crypto/` | TLS dropped | 2,565 |
| Enterprise license checks | Gated the features above; no licence keys in this fork (recommended, see §11) | 779 |
| Internal Kafka client | Only production caller was audit | 9,197 |
| Parquet/Thrift/Avro | Iceberg leftovers, no callers | 4,539 |
| `rpk`, ducktape, verifiers | Clients and tests, reused unchanged | n/a |

Dropping authentication and TLS has knock-on effects. `INVENTORY.md`
lists them under "What dropping authentication and TLS removes". The ones the
plan has to act on:

1. The Rust controller must **parse and discard** `user_management_cmd`,
   `acl_management_cmd` and `role_management_cmd` batches, plus the security
   parts of controller snapshots, so it can replay a log a stock cluster wrote.
2. `ApiVersions` does not advertise the dropped APIs.
3. A live migration needs the source cluster to run with SASL and TLS off on
   all listeners, internal RPC included, before the first Rust node joins.
4. With no access control at all, network isolation is the only protection.
   That goes in the operator docs, not just this plan.

## 2. What parity means: the contracts

Parity is defined as a list of contracts, each with a test that proves it.
"Behaves the same" is only accepted where a contract below says so.

| # | Contract | Proven by |
|---|---|---|
| C1 | **Kafka wire protocol.** The 38 APIs at the same version ranges, same error codes, same tagged fields | Codec generated from the same `schemata/*.json`; differential request replay against C++ (§7.2); client compatibility matrix |
| C2 | **Internal RPC.** Same framing, header, checksum, compression flag, method IDs (service ID XOR crc32 of method name and input/output type names, honouring `legacy_id`, per `rpc/rpc_compiler.py`), and serde payloads | Golden fixtures in both directions; mixed C++/Rust clusters |
| C3 | **serde and ADL encodings.** All 284 envelopes, including `legacy_*` placeholder slots and skipped snapshot parts | Golden fixtures generated from C++ (§7.1) |
| C4 | **On-disk formats.** Segments, 61-byte batch header with CRCs, `.base_index`, compaction indexes, staging files, kvstore, raft and stm snapshots, directory layout `{ns}/{topic}/{partition}_{revision}` | Cross-read and cross-write: C++ writes and Rust reads, and the reverse, under randomized workloads with compaction and truncation |
| C5 | **Replication and safety guarantees.** Raft safety (no committed-entry loss, single leader per term), `acks=all` durability with fsync semantics, idempotent producer dedup, exactly-once transactions (read_committed, LSO, aborted-tx index), compaction correctness, consumer group offset durability | Deterministic simulation, linearizability checking, the existing Java/Go verifiers, ducktape |
| C6 | **Cluster behaviour.** Topic lifecycle, partition placement, leader balancing, partition auto-balancing, maintenance mode, node add and decommission, forced reconfiguration, feature activation and upgrade gating, cluster config propagation | Ducktape suites; mixed clusters |
| C7 | **Operator surfaces.** Node config YAML, 319 cluster and 30 node properties (names, aliases, defaults, bounds, validation), Admin API v1 routes and JSON shapes, Admin API v2 protos, Prometheus metric names and labels, process CLI flags | `rpk` end-to-end; Helm chart post-upgrade job; metric and config schema diffs (zero differences allowed); Admin API contract tests from Swagger |
| C8 | **Runtime contract with `rpk`.** `rpk redpanda start` passes Seastar flags (`--smp`, `--memory`, `--reserve-memory`, `--lock-memory`, `--cpuset`, `--overprovisioned`, `--io-properties-file`, `--unsafe-bypass-fsync`, `--default-log-level`, `--thread-affinity`), and `rpk iotune` writes `io-properties.yaml` | Rust binary accepts the same flags with the same meaning; `rpk` start tests |
| C9 | **Feature table identity.** A Rust node reports the same logical version and feature set as v26.2.x slim, so features activate (and don't) exactly as in a C++ cluster | Mixed-cluster feature activation tests |
| C10 | **Performance.** No regression beyond agreed tolerances (§8) | Benchmark gates in CI |

Things that are deliberately **not** contracts: log message text, internal
metric names under `/metrics` that no dashboard or alert uses (to be decided
per metric, default is keep), crash-report file formats, and exact timing of
background work (housekeeping, balancer ticks), as long as configured
intervals are honoured.

## 3. Strategy: replace one node at a time

Three ways to do it, and why only the last gives the guarantees:

1. **Big bang.** Write the Rust broker, cut over when it passes tests. Fails
   on rollback: there is nothing to go back to once Rust has written data.
2. **In-process strangler (FFI).** Replace modules inside the C++ binary one
   at a time. Doesn't work here: everything runs on Seastar's reactor, and a
   Rust async module can't share that reactor without its own runtime. The FFI
   surface at module boundaries (futures, `iobuf`, `sharded<>`) would be
   bigger than some of the modules.
3. **Node-level strangler (recommended).** The unit of replacement is a node.
   The Rust broker speaks the C++ broker's internal RPC, reads and writes its
   disk formats, and reports the same feature table, so the two can share a
   cluster. This is how the work gets tested at every phase:
   - Storage is done when Rust and C++ read each other's data directories.
   - Raft is done when Rust and C++ replicas share a raft group under fault
     injection.
   - The controller is done when a Rust node joins a C++ cluster, follows the
     controller log, and can become controller leader.
   - Rollout is a rolling replacement on the same volumes. Rollback is the
     same operation in the other direction.

   The cost is that compatibility work (C2–C4, C9) is up front rather than
   optional. It would be needed for an in-place migration anyway.

## 4. Runtime

The largest technical decision. The C++ code depends on Seastar features that
no Rust runtime provides as one package:

| Seastar feature | Used for | Rust approach |
|---|---|---|
| One reactor per core, shared-nothing, `!Send` futures | Everything | Thread-per-core executor, one per core, pinned |
| `sharded<T>` (1,436 uses), `smp::submit_to` | Per-core service instances, cross-core calls | `Sharded<T>` built on per-core SPSC queues; cross-core calls return futures |
| `smp_service_group` (140 uses) | Limits concurrent cross-core work | Semaphore per (source, dest, group) |
| Scheduling groups with CPU shares (142 uses) | Kafka vs raft vs compaction vs admin prioritization | Multiple task queues per executor, picked by share-weighted vruntime |
| I/O scheduler with priority classes, `io-properties.yaml` | Fair disk bandwidth between appends, compaction, recovery | Per-core I/O queue with token-bucket model fed by the same `io-properties.yaml` (C8) |
| DMA file I/O (`open_file_dma`, O_DIRECT, aligned buffers) | Segment append and read, chunk cache | io_uring with O_DIRECT; aligned buffer pool |
| Per-shard allocator, memory limits, reclaimer | `batch_cache` shrinks under pressure; `--memory` budget | Explicit per-shard memory accounting with semaphores (already exists for fetch, recovery and storage); `batch_cache` driven by a budget plus pressure callback instead of allocator hooks |
| lowres clock, timers | Heartbeats, timeouts | Per-core timer wheel |
| Stall detector, task-quota preemption | Spotting long-running tasks; latency | Cooperative yield budget (`maybe_yield`) plus a watchdog thread |
| HTTP server | Admin API, Prometheus | hyper (or equivalent) on the per-core executor |
| Metrics registry | Probes everywhere | Custom per-core registry aggregated at scrape, same names and labels |

**Recommendation.** Write a thin `rp-runtime` crate that owns this API, and
put an existing thread-per-core io_uring executor under it. Candidates to
evaluate in Phase 0: glommio (has share-based task queues, the closest match
to scheduling groups), monoio, compio, and tokio with one `current_thread`
runtime per core. Check each one's maintenance status at decision time.

Owning the abstraction matters for two reasons. The executor can be swapped
if the first choice is wrong. More importantly, the same API can run on a
**simulated** backend (virtual time, simulated network and disk, seeded
scheduling) for deterministic simulation testing (§7.3). That is where this
rewrite can *exceed* today's guarantees rather than only match them.

Phase 0 spike criteria (must be measured against Seastar on the same box):
`acks=all` produce p50/p99 at fixed throughput, fetch throughput,
cross-core round-trip latency, fsync-heavy append throughput, and behaviour
under CPU contention between share groups.

## 5. Target architecture: crate map

| Crate | Replaces | C++ lines |
|---|---|---:|
| `rp-runtime` | Seastar + `ssx/` + `resource_mgmt/` | 6,386 + Seastar |
| `rp-bytes` | `bytes/` (iobuf: rope of refcounted chunks, zero-copy share/split) | 2,719 |
| `rp-util` | `utils/`, `container/`, `base/`, `hashing/`, `random/`, `strings/`, `json/` | 16,910 |
| `rp-serde` (+ derive) | `serde/` envelope, `model/adl_serde*` (plus `reflection/`, counted under `rp-util`) | ~2,400 |
| `rp-model` | `model/` | 5,870 |
| `rp-compression` | `compression/` (zstd streaming, lz4 frame, snappy Java framing, gzip) | 1,949 |
| `rp-config` (+ derive) | `config/` | 8,038 |
| `rp-net` | `net/` minus TLS (conn quotas, connection rate limits) | 3,387 |
| `rp-rpc` (+ build.rs codegen from service JSON) | `rpc/` | 3,364 |
| `rp-storage` | `storage/`, `compaction/` | 29,161 |
| `rp-raft` | `raft/` | 21,452 |
| `rp-cluster` | `cluster/`, `features/`, `migrations/` | 68,830 |
| `rp-kafka-protocol` (build.rs codegen from `schemata/*.json`) | `kafka/protocol/` + `generator.py` | 6,009 |
| `rp-kafka` | `kafka/server/`, `kafka/data/`, `kafka/utils/` | 38,403 |
| `rp-admin` | `redpanda/admin/`, `serde/json`, `serde/protobuf` (use prost) | 14,125 |
| `rp-ops` | `metrics/`, `debug_bundle/`, `crash_tracker/`, `syschecks/`, `cluster/self_test`, `http/` | ~10,800 |
| `redpanda` (bin) | `redpanda/` application wiring, CLI | 3,236 |
| `rp-sim` | (new) deterministic cluster simulator | n/a |
| `rp-compat` | (new) golden fixtures, cross-read and cross-write harness, differential replay | n/a |

Expected Rust size is roughly 150–200k production lines. The foundations
layer shrinks the most (crates replace hand-rolled utilities). Codegen
replaces most protocol boilerplate. `cluster/` and `raft/` will be close to
1:1 because the logic is irreducible.

Ground rules:

- **Port algorithms faithfully, write plumbing idiomatically.** Raft,
  storage, the transaction coordinator, `rm_stm`, group state machine, the
  allocator and balancers are ported decision for decision from the C++,
  with references to the original file in doc comments, so reviewers can
  diff behaviour. Service wiring, error handling, and buffers are written
  as idiomatic Rust.
- **`unsafe` lives only in `rp-runtime` and `rp-bytes`.** Those crates run
  under Miri and sanitizers in CI. Everything else has `#![forbid(unsafe_code)]`.
- **No panics on input.** Every decoder (Kafka, RPC, serde, segment parser,
  config) returns errors, and is fuzzed (§7.6). `vassert`-style invariants
  become explicit `assert!`s that crash the process, as today.
- **Reuse the schema files.** `kafka/protocol/schemata/*.json`, the RPC
  service JSON and `proto/` are inputs to Rust codegen. Don't fork them into
  hand-written Rust.

## 6. Phases

Each phase has an exit gate. Nothing moves to the next phase on "mostly
works". Phases 1–2 and 5a can overlap with others once Phase 0 is done.

### Phase 0: Foundations and decisions (months 0–4)

- Runtime spike and decision (§4). Build `rp-runtime` v0 with the real and
  simulated backends behind one API.
- `rp-bytes`, `rp-util` essentials.
- **Compatibility tooling in C++ first.** A fixture generator binary in this
  tree that emits, for every serde/ADL type, randomized instances and their
  encoded bytes. It extends the pattern in
  `//src/v/cluster/tests:legacy_wire_test`. Also a data-directory generator
  that runs randomized workloads and snapshots the resulting directory.
- CI: cargo workspace in this repo (`src/rust/`), wired into Bazel via
  `rules_rust`, so the C++ fixture generators and Rust consumers build in
  one graph.
- Prune the ducktape suite to what applies to slim. Per `HANDOFF.md`, about
  160 of 267 modules reference removed features, and authn/TLS tests now
  join them. Get the remaining suite green **against the C++ broker**. It is
  the oracle for everything after.

Exit: runtime chosen with spike numbers recorded; fixtures generated in CI;
pruned ducktape suite green on C++.

### Phase 1: Codecs and model (months 2–6, ~27k C++ lines)

`rp-serde` (derive macro for positional envelopes with version and
compat_version; ADL codec), `rp-model` (record batches, CRCs, ntp, offsets,
timestamps), `rp-compression`, `rp-kafka-protocol` codegen, `rp-rpc`
framing and codegen, `rp-config` property framework with all 349
properties.

Exit (C3, part of C1, C2, C7): every golden fixture decodes and re-encodes
byte-identically. Config schema diff against the C++ Admin API
`cluster_config/schema` output is empty. Fuzzers run nightly.

### Phase 2: Storage engine (months 4–11, ~29k)

Segments, appender (O_DIRECT, fallocate, chunked flush), index, readers and
readers cache, batch cache, log manager and housekeeping, retention (size,
time), truncation and prefix truncation, offset translator, kvstore,
snapshots, sliding-window and adjacent-segment compaction, compaction
indexes and key maps, the fault-injection file wrappers.

Exit (C4): cross-read and cross-write both ways under randomized workloads
including compaction, truncation and crash at random points (power-loss
simulation: drop unsynced writes). C++ storage unit tests ported (the 20k
lines in `storage/tests` encode most edge cases). Append and read
microbenchmarks within tolerance.

### Phase 3: Raft (months 8–16, ~21k)

Consensus, elections and pre-vote, replicate batcher and append-entries
buffering, heartbeat manager (node-pair batching), recovery and
`coordinated_recovery_throttle`, joint-consensus reconfiguration, learners,
leadership transfer, voter priority, `persisted_stm` and the state machine
manager, snapshots, raftgen RPC.

Exit (C5 raft part, C2): (a) deterministic simulation of thousands of seeds
per CI run with partitions, crashes, disk faults, clock skew and
reconfiguration, checking election safety, log matching, leader
completeness and state machine safety; (b) **mixed raft groups**: C++ and
Rust replicas in one group under fault injection, with a
linearizability checker on a register workload; (c) ported raft unit tests
pass.

### Phase 4: Controller and cluster core (months 12–22, ~41k)

Controller stm, backend and snapshot; commands (including parse-and-drop
for removed and security commands); topic table, topics frontend and
dispatcher; members manager, backend and discovery; cluster bootstrap;
config manager; feature manager and table (C9); health monitor; node status;
metadata dissemination; partition manager and partition runtime; leaders
table; id allocator; distributed kv stm; log eviction and partition
properties stms; drain; forced reconfiguration.

Exit (C6 partial, C9): **milestone: a Rust node joins a C++ cluster.** It
replays the controller log, serves as controller follower and leader, and
hosts raft replicas for user partitions. Ducktape cluster-management suites
pass on 3-node mixed clusters (1 Rust, 2 C++, then 2+1) and all-Rust
clusters.

### Phase 5: Kafka data path (5a: months 6–14 against a single-node stub; 5b: 14–20 on real raft, ~27k)

Server core (connection handling, request dispatch, flexible versions,
memory and connection quotas), produce (validation, `acks` semantics),
fetch (sessions, memory units, read coalescing, `fetch_pid_controller`,
`min_bytes`/`max_wait` long polling), metadata, ApiVersions,
DescribeCluster, list offsets, offset-for-leader-epoch, delete records,
topic and config admin APIs, client quotas and throughput control, usage
accounting, the `kafka_data` RPC (partition proxy for remote leaders).

5a builds the server against a single-node log without raft, so the
protocol layer is shaken out early.

Exit (C1): client compatibility matrix (franz-go, librdkafka, Java client,
sarama, kafka-python, plus Kafka CLI tools) for every supported API version.
Differential replay shows no unexplained differences. `kgo-verifier`
produce and consume, including on mixed clusters.

### Phase 6: Consumer groups, idempotence and transactions (months 16–26, ~25k)

Group coordinator (group state machine, rebalance protocol, static
membership, offset commit and expiry, group recovery from
`__consumer_offsets`, group router), `rm_stm` and producer state
(idempotence, sequence tracking, producer eviction), transaction
coordinator (`tm_stm`, `tx_gateway_frontend`, tx hash ranges and
`__transaction_state` partitions), group transactions
(`group_tx_tracker_stm`, group fencing), describe/list transactions,
describe producers, tx manager migrator.

This is the highest-risk phase for subtle semantic bugs: `rm_stm` and
`tx_gateway_frontend` alone are 5.8k lines with a lot of state.

Exit (C5 transactions part): Java EOS verifiers
(`tests/java/e2e-verifiers`) and transaction ducktape suites on all-Rust and
mixed clusters; simulation tests for coordinator failover mid-commit and
mid-abort; aborted-tx index and LSO checked against C++ on the same
workload.

### Phase 7: Placement and balancing (months 20–28, ~11k)

Partition allocator and constraints, leader balancer (greedy and random
strategies), partition auto-balancer planner and backend, shard placement
table and core balancer, maintenance mode interaction, node decommission
and recommission.

Exit (C6): balancer decisions match C++ on recorded cluster states (feed the
same `health_report` and `topic_table` snapshots to both planners and
compare plans). Ducktape balancer, decommission and maintenance suites pass.

### Phase 8: Admin API, metrics and operations (months 18–30, ~23k)

Admin API v1 (all non-security routes), v2 ConnectRPC services, metrics on
`/metrics` and `/public_metrics`, debug bundle, crash tracker, syschecks,
self-test, process CLI flags (C8), metrics reporter (if kept, §11).

Exit (C7, C8): Swagger-derived contract tests pass; metrics name and label
diff against C++ is empty; `rpk` end-to-end suite passes; the Helm chart
post-upgrade job works.

### Phase 9: Hardening and rollout (months 26–36+)

- Performance gates (§8) on production-like hardware.
- Long soak runs (weeks) with chaos, on all-Rust and mixed clusters.
- Antithesis runs (`tools/antithesis` packaging exists for the C++ binary).
- **Rollout drill:** `upgrade_test.sh` adapted to replace C++ nodes with
  Rust one by one on the same volumes, then the reverse. Both directions
  must pass before any production rollout.
- Staged production rollout: one Rust node in a non-critical cluster, then
  majority, then all-Rust. Keep the C++ binary as a rollback target until an
  agreed bake period has passed.

## 7. How the guarantees are proven

1. **Golden fixtures (C2, C3).** The C++ fixture generator emits bytes for
   every serde and ADL type with randomized contents. Rust must decode them,
   re-encode them byte-identically, and produce bytes C++ decodes. This runs
   in CI on every change to either side.
2. **Differential replay (C1).** Record Kafka request streams (from
   ducktape runs and synthetic generators), replay them against a C++ and
   a Rust single-node broker started from the same state, and diff
   responses after normalizing non-deterministic fields (timestamps,
   producer IDs, node IDs).
3. **Deterministic simulation (C5, C6).** Whole clusters (raft, controller,
   transactions, groups) run in one thread on `rp-runtime`'s simulated
   backend with seeded scheduling, network faults, disk faults (torn
   writes, lost unsynced data, EIO) and clock skew. Any failure is
   reproducible from its seed. This is the main way the rewrite gets
   *stronger* guarantees than the C++, which can't be run this way.
4. **Model checking for the riskiest protocols.** TLA+ (or a Rust model
   checker like Stateright) specs for raft joint-consensus reconfiguration,
   the transaction coordinator commit and abort paths, and the controller's
   topic and partition lifecycle. Trace-check simulation runs against the
   specs.
5. **Black-box suites.** Pruned ducktape suite, Java and Go verifiers, and
   Antithesis, all run against C++, Rust and mixed clusters with the same
   expectations.
6. **Fuzzing.** cargo-fuzz on every decoder: Kafka requests, RPC frames,
   serde envelopes, segment and index parsing, config YAML and JSON.
7. **Ported unit tests.** The 125k lines of C++ tests in scope encode years
   of edge cases. Port the storage, raft, transactions, groups and
   balancer tests first. Lower-value fixture-heavy tests can be replaced by
   simulation and property tests.
8. **Memory safety.** Miri and sanitizers on `rp-runtime` and `rp-bytes`;
   `forbid(unsafe_code)` everywhere else.

## 8. Performance gates

Baselines measured on the C++ broker on the same hardware at Phase 0, then
enforced in CI from Phase 5 onwards. Tolerances below are proposals to be
agreed:

| Benchmark | Proposed gate |
|---|---|
| `acks=all` produce throughput (3 nodes, RF=3) | ≥ 95% of C++ |
| `acks=all` produce p99 latency at 50% of max throughput | ≤ 110% of C++ |
| Consumer fetch throughput (tail and catch-up reads) | ≥ 95% of C++ |
| Partition count scaling (10k+ partitions per node: memory per partition, heartbeat CPU) | ≤ 110% of C++ |
| Recovery time after node restart (log replay, raft recovery) | ≤ 110% of C++ |
| Compaction throughput | ≥ 90% of C++ |
| Cross-core RPC latency (microbenchmark) | ≤ 120% of Seastar |

OpenMessaging Benchmark and `kgo-verifier` provide the workloads.

## 9. Effort

This is a rough, order-of-magnitude estimate and the uncertainty is real.

- 240k lines of in-scope C++ plus Seastar, ported with byte-level
  compatibility and a verification stack that doesn't exist today.
- Assuming 15–20k lines of tested production Rust per engineer-year for
  this class of code, the port alone is 8–13 engineer-years. Compatibility
  tooling, simulation, test porting, performance work and rollout typically
  double or triple that. Hence **25–35 engineer-years**.
- With 6–10 engineers who are strong in Rust *and* distributed storage:
  first mixed-cluster milestone (end of Phase 4) at 12–18 months, full
  parity at 2.5–4 years.

The biggest swing factors are the runtime (a weak choice costs a rewrite of
the rewrite) and transactions (Phase 6).

## 10. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| Runtime can't match Seastar's scheduling, I/O fairness or tail latency | Fails C10; possibly redo `rp-runtime` | Phase 0 spike with hard numbers before committing; own the abstraction |
| Subtle semantic drift in transactions, groups or raft | Data loss or duplicates that tests miss | Faithful port with references to the C++; simulation; model checking; mixed-cluster tests where C++ is the oracle |
| Compatibility gaps found late (an envelope or kvstore key missed) | Mixed cluster or rollback breaks in production | Fixture generator enumerates types from the C++ build, not a hand list; cross-read and cross-write in CI from Phase 2 |
| Memory behaviour differs (no allocator reclaimer) | OOM or cache thrash under load | Explicit budgets everywhere; memory-pressure tests in simulation and soak |
| Upstream keeps moving (this tree tracks v26.2.x) | Parity target drifts | Freeze the target at v26.2.x slim; batch upstream fixes into a tracked list; re-baseline at agreed points only |
| Ducktape suite is half about removed features | Weak oracle | Phase 0 pruning is a gate, not a nice-to-have |
| Team availability (Rust plus distributed storage is a scarce combination) | Schedule | Phase plan lets 2–3 sub-teams work in parallel after Phase 0 |
| Licensing: Redpanda core is under the Business Source License 1.1 (`licenses/bsl.md`) | A close port is very likely a derivative work bound by the same licence and its use grant | Get legal advice before starting; it changes nothing technically, but could change whether to do this at all |
| No authentication or TLS | Anyone on the network can read, write or delete | Document loudly; network isolation in every deployment; leave room in `rp-net` so TLS could be added back without redesign |

## 11. Open decisions

1. **Runtime.** Decided at the end of Phase 0 from spike numbers.
2. **Enterprise license checks.** Recommendation: drop. They only gated
   features this fork either dropped or should just allow.
3. **Phone-home metrics reporter** (`cluster/metrics_reporter`, sends
   cluster telemetry to Redpanda). Recommendation: drop or default off in a
   fork. Parity says keep.
4. **Mixed-cluster interop.** Recommended above, and it carries roughly
   15–20% of the effort. The alternative is offline migration only (stop the
   cluster, start Rust on the same data dirs). That still needs C4, but
   loses incremental validation and live rollback.
5. **Kafka API versions.** Freeze at today's ranges for parity, or plan
   newer ones (for example the KIP-848 consumer group protocol) as
   post-parity work. Recommendation: freeze.
6. **Vestigial config** (`retention.local.target.*`, `space_management_*`,
   `disk_reservation_percent`). Keep accepting them for config
   compatibility (C7), as the C++ slim build does.

## 12. First 90 days

1. Stand up `src/rust/` as a cargo workspace built by Bazel via `rules_rust`;
   CI job for it.
2. Runtime spike: two candidate executors behind a prototype `rp-runtime`
   API; benchmark against Seastar.
3. C++ fixture generator for serde and ADL types; data-directory generator.
4. Prune ducktape to the slim and no-auth/no-TLS feature set; get it green
   on the C++ broker.
5. `rp-bytes` and `rp-serde` derive macro, decoding the first fixtures.
6. Kafka protocol codegen from `schemata/*.json`, round-tripping requests
   captured from a ducktape run.
7. Write the TLA+ (or Stateright) spec for raft reconfiguration while the
   raft port is still months away.
