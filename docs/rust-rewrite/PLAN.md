# Rewriting slim Redpanda in Rust: plan

Companion to `INVENTORY.md`, which has the feature map and line counts this
plan is sized from.

## Summary

- **What is being rewritten.** The slim broker in this tree: a distributed
  Kafka broker on local storage, based on v26.2.x (counted at `9daa2fab`).
  That is 241k lines of production C++ (plus 128k lines of C++ tests) across
  61 features, plus the Seastar runtime underneath it. The total includes the
  fork's own windowed produce-time deduplication.
- **Out of scope.** Authentication (SASL, mTLS, ACLs, RBAC, audit), TLS and
  enterprise license checks are dropped (18.6k lines). Nothing carries over
  from existing C++ machines: Rust clusters start on empty disks, so the
  backward-compatibility code (ADL, legacy wire slots, migrators, old-version
  decode paths) goes too. Another 13.7k lines are already dead in this tree.
- **What "full parity and guarantees" means.** Externally visible behaviour.
  That is the Kafka protocol and its semantics (durability, idempotence,
  transactions, compaction, consumer groups), cluster behaviour, and the
  operator surfaces: config, Admin API, metrics, CLI flags. Internal RPC,
  on-disk formats and controller encodings are free for the Rust design to
  choose. The contracts are in §2.
- **Strategy.** Build a new Rust broker and use the C++ broker as a
  reference: both are driven with the same workloads through the same
  external APIs and must behave the same. Correctness comes from
  deterministic simulation, model checking and the existing black-box test
  suites.
- **Untouched.** `rpk` (Go) is a client of the Kafka and Admin APIs. The
  ducktape suite, the Java/Go verifiers, and the Kafka schema JSON files are
  reused as-is.
- **Rough size.** On the order of 20–28 engineer-years: 6–10 strong
  Rust/distributed-systems engineers for 2–3.5 years. The first milestone
  (a 3-node all-Rust cluster serving produce and consume) lands around month
  10–15. This is a rough estimate (see §9).

## 1. Scope

### In scope (keep, 241,206 lines)

Every `keep` row in `INVENTORY.md`: the Kafka API (38 APIs), Raft, the
storage engine, the controller and everything in `cluster/`, transactions
and idempotence, windowed produce-time deduplication, the Admin API v1 and v2 (minus security routes),
configuration, internal RPC, metrics, debug bundle, crash tracker, self-test,
syschecks, and the runtime foundations.

### Out of scope

| What | Why | Lines |
|---|---|---:|
| SASL PLAIN/SCRAM, mTLS principals, ephemeral credentials, breakglass | Authentication dropped | 3,563 |
| ACLs, RBAC, user/role management | No principals left to authorize | 7,417 |
| Audit logging | Records principals; nothing to record | 4,304 |
| TLS on every listener, OpenSSL, `crypto/` | TLS dropped | 2,565 |
| Enterprise license checks | Dropped | 779 |
| Compatibility with older C++ versions | No state carried over from C++ nodes | 1,431 in whole files, plus code inside kept files |
| Internal Kafka client | Only production caller was audit | 9,197 |
| Parquet/Thrift/Avro | Iceberg leftovers, no callers | 4,539 |
| `rpk`, ducktape, verifiers | Clients and tests, reused unchanged | n/a |

Consequences the plan has to act on:

1. `ApiVersions` does not advertise the dropped Kafka APIs (SASL, ACL,
   SCRAM, roles), so tools report them as unsupported.
2. The features that were behind a licence (continuous partition and core
   balancing, leadership pinning, topic-deletion protection) are kept and
   controlled by config alone.
3. Anyone who can reach a port has full control. Network isolation is the
   only protection; that goes in the operator docs, not just this plan.
4. Moving to the Rust broker means standing up new clusters. If data has to
   move, it moves at the Kafka level (MirrorMaker 2, Redpanda Connect), which
   copies records but not offsets. Consumer groups re-establish positions on
   the new cluster.

## 2. What parity means: the contracts

Parity is defined as a list of contracts, each with a test that proves it.
"Behaves the same" is only accepted where a contract below says so.

| # | Contract | Proven by |
|---|---|---|
| C1 | **Kafka wire protocol.** The 38 APIs at the same version ranges, same error codes, same tagged fields, record batch format v2 | Codec generated from the same `schemata/*.json`; differential replay against the C++ broker (§7.2); client compatibility matrix |
| C2 | **Data guarantees.** Raft safety (no committed-entry loss, at most one leader per term); `acks=all` durability with the same fsync semantics; idempotent producer dedup; exactly-once transactions (read_committed, LSO, aborted transactions); compaction keeps the latest value per key and honours tombstone retention; retention by size and time; consumer group offset durability; windowed dedup as specified by guarantees G1–G5 and bounds B1–B5 in `docs/rfcs/20260730_log_derived_dedup_state.md` (including: a fully deduplicated `acks=all` request is acked only once the original write is committed; idempotent and transactional produce bypass it; base offsets stay correct when records are dropped) | Deterministic simulation, linearizability checking, model checking, the Java/Go verifiers, ducktape |
| C3 | **Cluster behaviour.** Topic lifecycle, partition placement, leader balancing, partition auto-balancing, maintenance mode, node add and decommission, forced reconfiguration, cluster config propagation | Ducktape suites; differential runs against C++ |
| C4 | **Operator surfaces.** Node config YAML; 320 cluster and 30 node properties (names, types, defaults, bounds, validation); topic config names and meaning; Admin API v1 routes and JSON shapes; Admin API v2 protos; Prometheus metric names and labels | `rpk` end-to-end; Helm chart post-upgrade job; config schema and metrics diffs against C++ (zero differences allowed); Admin API contract tests from Swagger |
| C5 | **Runtime contract with `rpk`.** `rpk redpanda start` passes Seastar flags (`--smp`, `--memory`, `--reserve-memory`, `--lock-memory`, `--cpuset`, `--overprovisioned`, `--io-properties-file`, `--unsafe-bypass-fsync`, `--default-log-level`, `--thread-affinity`), and `rpk iotune` writes `io-properties.yaml` | Rust binary accepts the same flags with the same meaning; `rpk` start tests |
| C6 | **Performance.** No regression beyond agreed tolerances (§8) | Benchmark gates in CI |

**Not contracts** (free for the Rust design): internal RPC framing and
encodings, on-disk segment, index, kvstore and snapshot formats, controller
command encodings, feature-flag identities, log message text, crash-report
formats, and exact timing of background work as long as configured intervals
are honoured.

**A requirement that is not parity: Rust-to-Rust upgrades.** Nothing comes
from C++, but Rust release N+1 must be able to take over release N's disks
and join a cluster of release N nodes. Otherwise every release means a new
cluster. So from the first release:

- every on-disk structure and RPC message carries a version, and decoders
  accept the previous version;
- a feature-flag mechanism (the C++ `feature_table` idea, starting empty)
  gates behaviour changes until the whole cluster is upgraded;
- an N-1 → N rolling upgrade test runs in CI from the first tagged release.

## 3. Strategy: a new broker, with the C++ broker as the reference

Nothing has to interoperate with C++, so there is no need to build the Rust
broker one node at a time inside a C++ cluster. Instead:

- **Build a standalone Rust broker.** Components are tested on their own
  (storage against a reference model, raft in simulation) and assembled into
  all-Rust clusters.
- **Use the C++ broker as a reference implementation.** Run the same
  workload against a C++ cluster and a Rust cluster and compare what clients
  and operators see: Kafka responses, fetched data, offsets, group state,
  Admin API output, config schema, metric names. This is where most
  semantic bugs show up, and it needs no internal compatibility.
- **Keep the architecture, simplify the encodings.** The design of v26.2.x
  is proven: one raft group per partition, controller as raft0, Kafka
  batches stored on disk as-is, offset translation for non-data batches,
  per-core partition ownership. Port its algorithms faithfully. Change the
  encodings where that removes code: one versioned binary format instead of
  serde plus ADL, no legacy slots, no old-version decode branches.
  Redesigning the algorithms themselves is out of scope for parity. It is
  where semantic drift comes from; propose it as post-parity work.

What this gives up compared with an interop strategy: there is no
mixed-cluster test where C++ catches Rust bugs on the internal protocol, and
no in-place rollback. In exchange, the compatibility tooling (golden
fixtures, cross-reading data directories, mixed raft groups, controller-log
replay) is gone, along with 15–20% of the effort.

**Rollout.** New Rust clusters run alongside the existing C++ clusters,
optionally fed by Kafka-level mirroring, until they've proven themselves
under real load. Rollback means pointing clients back at the C++ cluster,
so keep it running for an agreed bake period.

## 4. Runtime

The largest technical decision. The C++ code depends on Seastar features that
no Rust runtime provides as one package:

| Seastar feature | Used for | Rust approach |
|---|---|---|
| One reactor per core, shared-nothing, `!Send` futures | Everything | Thread-per-core executor, one per core, pinned |
| `sharded<T>` (1,436 uses), `smp::submit_to` | Per-core service instances, cross-core calls | `Sharded<T>` built on per-core SPSC queues; cross-core calls return futures |
| `smp_service_group` (140 uses) | Limits concurrent cross-core work | Semaphore per (source, dest, group) |
| Scheduling groups with CPU shares (142 uses) | Kafka vs raft vs compaction vs admin prioritization | Multiple task queues per executor, picked by share-weighted vruntime |
| I/O scheduler with priority classes, `io-properties.yaml` | Fair disk bandwidth between appends, compaction, recovery | Per-core I/O queue with token-bucket model fed by the same `io-properties.yaml` (C5) |
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
scheduling) for deterministic simulation testing (§7.3). Without interop
testing against C++, simulation becomes the main source of confidence in
the distributed parts. It is also where this rewrite can *exceed* today's
guarantees rather than only match them.

Phase 0 spike criteria (must be measured against Seastar on the same box):
`acks=all` produce p50/p99 at fixed throughput, fetch throughput,
cross-core round-trip latency, fsync-heavy append throughput, and behaviour
under CPU contention between share groups.

## 5. Target architecture: crate map

| Crate | Replaces | C++ lines |
|---|---|---:|
| `rp-runtime` | Seastar + `ssx/` + `resource_mgmt/` | 6,386 + Seastar |
| `rp-bytes` | `bytes/` (iobuf: rope of refcounted chunks, zero-copy share/split) | 2,719 |
| `rp-util` | `utils/`, `container/`, `base/`, `hashing/`, `random/`, `strings/`, `json/`, `reflection/` (minus ADL) | 16,511 |
| `rp-codec` (+ derive) | `serde/` envelopes; one versioned binary format for RPC and disk, no ADL | 1,845 |
| `rp-model` | `model/` | 5,333 |
| `rp-compression` | `compression/` (zstd streaming, lz4 frame, snappy Java framing, gzip) | 2,017 |
| `rp-config` (+ derive) | `config/` | 8,056 |
| `rp-net` | `net/` minus TLS (conn quotas, connection rate limits) | 3,387 |
| `rp-rpc` | `rpc/` (framing, method dispatch, connection cache) | 3,364 |
| `rp-storage` | `storage/`, `compaction/` | 29,196 |
| `rp-raft` | `raft/` | 21,440 |
| `rp-cluster` | `cluster/`, `features/` | 70,235 |
| `rp-kafka-protocol` (build.rs codegen from `schemata/*.json`) | `kafka/protocol/` + `generator.py` | 6,013 |
| `rp-kafka` | `kafka/server/`, `kafka/data/`, `kafka/utils/` | 38,521 |
| `rp-admin` | `redpanda/admin/`, `serde/json`, `serde/protobuf` (use prost) | 14,125 |
| `rp-ops` | `metrics/`, `debug_bundle/`, `crash_tracker/`, `syschecks/`, `cluster/self_test`, `http/` | ~10,800 |
| `redpanda` (bin) | `redpanda/` application wiring, CLI | 3,243 |
| `rp-sim` | (new) deterministic cluster simulator | n/a |
| `rp-oracle` | (new) differential harness: drives C++ and Rust clusters with the same workload and compares external behaviour | n/a |

Expected Rust size is roughly 140–190k production lines. The foundations
layer shrinks the most (crates replace hand-rolled utilities). Codegen
replaces most protocol boilerplate. Dropping backward compatibility removes
scattered branches everywhere. `cluster/` and `raft/` will be close to 1:1
because the logic is irreducible.

Ground rules:

- **Port algorithms faithfully, write plumbing idiomatically.** Raft,
  storage, the transaction coordinator, `rm_stm`, group state machine, the
  allocator and balancers are ported decision for decision from the C++,
  with references to the original file in doc comments, so reviewers can
  diff behaviour. Service wiring, error handling, buffers and encodings are
  written as idiomatic Rust.
- **Version everything persisted or sent between nodes** from the first
  commit (see the Rust-to-Rust upgrade requirement in §2). Format specs live
  next to the code as docs.
- **`unsafe` lives only in `rp-runtime` and `rp-bytes`.** Those crates run
  under Miri and sanitizers in CI. Everything else has `#![forbid(unsafe_code)]`.
- **No panics on input.** Every decoder (Kafka, RPC, codec, segment parser,
  config) returns errors, and is fuzzed (§7.6). `vassert`-style invariants
  become explicit `assert!`s that crash the process, as today.
- **Reuse the schema files.** `kafka/protocol/schemata/*.json` and `proto/`
  are inputs to Rust codegen. Don't fork them into hand-written Rust.
- **Design the crate graph for build speed.** Rust compiles a crate at a
  time, so a wide, shallow graph builds in parallel and a deep chain doesn't.
  Keep generic-heavy code from being instantiated in every downstream crate;
  run proc macros and codegen once, in leaf crates; keep LTO and
  `codegen-units=1` for release builds only. Measure the C++ clean and
  incremental build times in Phase 0 and track the Rust ones in CI against
  them. Rust is not automatically faster to compile; it only wins with this
  discipline.

## 6. Phases

Each phase has an exit gate. Nothing moves to the next phase on "mostly
works". After Phase 0, phases overlap as shown.

### Phase 0: Foundations and decisions (months 0–4)

- Runtime spike and decision (§4). Build `rp-runtime` v0 with the real and
  simulated backends behind one API.
- `rp-bytes`, `rp-util` essentials.
- Cargo workspace in this repo (`src/rust/`), built in CI. Bazel integration
  via `rules_rust` only if the C++ tree is still built alongside.
- Write the format specs: internal RPC framing, segment and index files,
  kvstore, snapshots, controller commands. Each one versioned.
- `rp-oracle` v0: start a C++ cluster and (later) a Rust cluster from the
  same config, run a workload, capture externally visible results.
- Finish pruning the ducktape suite and get it running. Per `HANDOFF.md`,
  `tests/rptest` is down from 669 modules to 438 (246 test files), with the
  tiered-storage, cloud, Iceberg, OIDC and Kerberos suites removed, but none
  of the survivors have been run against the slim broker. About 31 test
  files still exercise SASL, TLS, ACLs or audit and need to go or lose those
  cases. Get the remaining suite green **against the C++ broker**. It is the
  reference for everything after.
- Write ducktape tests for windowed dedup. It has none today; its coverage
  is C++ unit tests, which are not a black-box reference for the Rust
  broker.

Exit: runtime chosen with spike numbers recorded; format specs reviewed;
pruned ducktape suite green on C++.

### Phase 1: Codecs and model (months 2–5, ~27k C++ lines)

`rp-codec` (derive macro for versioned envelopes), `rp-model` (record
batches, CRCs, ntp, offsets, timestamps), `rp-compression`,
`rp-kafka-protocol` codegen, `rp-rpc` framing, `rp-config` property
framework with all 350 properties.

Exit (part of C1, C4): Kafka requests and responses captured from ducktape
runs against C++ decode and re-encode byte-identically. Config schema diff
against the C++ Admin API `cluster_config/schema` output is empty. Fuzzers
run nightly.

### Phase 2: Storage engine (months 3–9, ~29k)

Segments, appender (O_DIRECT, fallocate, chunked flush), index, readers and
readers cache, batch cache, log manager and housekeeping, retention (size,
time), truncation and prefix truncation, offset translator, kvstore,
snapshots, sliding-window and adjacent-segment compaction, compaction
indexes and key maps, the fault-injection file wrappers.

Exit (C2 storage part): model-based tests against an in-memory reference
log (random appends, truncations, compactions, retention, restarts) with
no divergence. Crash consistency under simulated power loss (unsynced
writes dropped, torn writes) at random points. C++ storage unit tests
ported (the 20k lines in `storage/tests` encode most edge cases). Append and
read microbenchmarks within tolerance.

### Phase 3: Raft (months 6–13, ~21k)

Consensus, elections and pre-vote, replicate batcher and append-entries
buffering, heartbeat manager (node-pair batching), recovery and
`coordinated_recovery_throttle`, joint-consensus reconfiguration, learners,
leadership transfer, voter priority, `persisted_stm` and the state machine
manager, snapshots, raft RPC service.

Exit (C2 raft part): (a) deterministic simulation of thousands of seeds
per CI run with partitions, crashes, disk faults, clock skew and
reconfiguration, checking election safety, log matching, leader
completeness and state machine safety; (b) simulation traces checked
against a TLA+ (or Stateright) model of reconfiguration (§7.4); (c) a
linearizability checker on a register workload over a real 3-node
deployment with network and process faults; (d) ported raft unit tests
pass.

### Phase 4: Controller and cluster core (months 10–18, ~41k)

Controller stm, backend and snapshot; commands; topic table, topics
frontend and dispatcher; members manager, backend and discovery; cluster
bootstrap; config manager; feature manager and table (starting empty);
health monitor; node status; metadata dissemination; partition manager and
partition runtime; leaders table; id allocator; distributed kv stm; log
eviction and partition properties stms; drain; forced reconfiguration.

Exit (C3 partial): **milestone: a 3-node all-Rust cluster** bootstraps,
creates and deletes topics, adds and decommissions nodes, survives
controller leader loss, and passes the cluster-management ducktape suites.
`rp-oracle` shows the same topic, partition and config state as C++ after
the same sequence of admin operations.

### Phase 5: Kafka data path (5a: months 5–12 against a single-node stub; 5b: 12–17 on real raft, ~27k)

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
Differential replay against C++ shows no unexplained differences.
`kgo-verifier` produce and consume on 3-node clusters with faults.

### Phase 6: Consumer groups, idempotence, transactions and dedup (months 14–22, ~27k)

Group coordinator (group state machine, rebalance protocol, static
membership, offset commit and expiry, group recovery from
`__consumer_offsets`, group router), `rm_stm` and producer state
(idempotence, sequence tracking, producer eviction), transaction
coordinator (`tm_stm`, `tx_gateway_frontend`, tx hash ranges and
`__transaction_state` partitions), group transactions
(`group_tx_tracker_stm`, group fencing), describe/list transactions,
describe producers, tx manager migrator (recovery-mode repartitioning of
the transaction coordinator topic). Windowed produce-time dedup
(`dedup_window_filter`, `dedup_stm`, the two topic properties and the
per-partition entry bound), ported from the design in
`docs/rfcs/20260730_log_derived_dedup_state.md`.

This is the highest-risk phase for subtle semantic bugs: `rm_stm` and
`tx_gateway_frontend` alone are 5.8k lines with a lot of state.

Exit (C2 transactions part): Java EOS verifiers
(`tests/java/e2e-verifiers`) and transaction ducktape suites pass;
simulation tests for coordinator failover mid-commit and mid-abort;
`rp-oracle` shows the same LSO, aborted-transaction lists and
read_committed results as C++ on the same transactional workloads. For
dedup: the ported `dedup_window_filter` and `dedup_stm` tests pass, and the
oracle checks results against G1–G5 and B1–B5 rather than requiring
identical output. The index is advisory by design, so leader changes and
window-boundary timing can legitimately let different duplicates through
on C++ and Rust.

### Phase 7: Placement and balancing (months 17–24, ~11k)

Partition allocator and constraints, leader balancer (greedy and random
strategies), partition auto-balancer planner and backend, shard placement
table and core balancer, maintenance mode interaction, node decommission
and recommission.

Exit (C3): planners produce the same plans as C++ for the same cluster
state (export health report and topic table snapshots from C++ clusters as
JSON test inputs; these are test fixtures, not a compatibility format).
Ducktape balancer, decommission and maintenance suites pass.

### Phase 8: Admin API, metrics and operations (months 15–25, ~23k)

Admin API v1 (all non-security routes), v2 ConnectRPC services, metrics on
`/metrics` and `/public_metrics`, debug bundle, crash tracker, syschecks,
self-test, process CLI flags (C5), metrics reporter (if kept, §11).

Exit (C4, C5): Swagger-derived contract tests pass; metrics name and label
diff against C++ is empty; `rpk` end-to-end suite passes; the Helm chart
post-upgrade job works.

### Phase 9: Hardening and rollout (months 22–30+)

- Performance gates (§8) on production-like hardware.
- Long soak runs (weeks) with chaos.
- Antithesis runs (adapt the `tools/antithesis` packaging to the Rust
  binary).
- **Rust-to-Rust upgrade drill:** rolling upgrade from the previous Rust
  build to the current one on the same volumes, and back, under load.
- Rollout: new Rust clusters next to the C++ ones, optionally mirrored,
  first for non-critical workloads. Retire a C++ cluster only after its
  Rust replacement has run through an agreed bake period.

## 7. How the guarantees are proven

1. **Model-based testing (storage, state machines).** Each stateful
   component is checked against a simple in-memory reference under random
   operation sequences, including crashes and restarts.
2. **Differential testing against C++ (C1, C3, C4).** `rp-oracle` runs the
   same workload against a C++ and a Rust cluster and compares
   externally visible results after normalizing non-deterministic fields
   (timestamps, producer IDs, node IDs, leader placement). It covers Kafka
   responses and fetched data, consumer group state, transaction outcomes,
   Admin API output, config schema and metric names.
3. **Deterministic simulation (C2, C3).** Whole clusters (raft, controller,
   transactions, groups) run in one thread on `rp-runtime`'s simulated
   backend with seeded scheduling, network faults, disk faults (torn
   writes, lost unsynced data, EIO) and clock skew. Any failure is
   reproducible from its seed. This is the main way the rewrite gets
   *stronger* guarantees than the C++, which can't be run this way.
4. **Model checking for the riskiest protocols.** TLA+ (or Stateright)
   specs for raft joint-consensus reconfiguration, the transaction
   coordinator commit and abort paths, and the controller's topic and
   partition lifecycle. Simulation traces are checked against the specs.
5. **Black-box suites.** Pruned ducktape suite, Java and Go verifiers, and
   Antithesis, run against C++ and Rust with the same expectations.
6. **Fuzzing.** cargo-fuzz on every decoder: Kafka requests, RPC frames,
   codec envelopes, segment and index parsing, config YAML and JSON.
7. **Ported unit tests.** The 128k lines of C++ tests in scope encode years
   of edge cases. Port the storage, raft, transactions, groups and
   balancer tests first. Lower-value fixture-heavy tests can be replaced by
   simulation and property tests.
8. **Upgrade tests.** From the first tagged Rust release, CI runs N-1 → N
   rolling upgrades and N → N-1 downgrades on real disks.
9. **Memory safety.** Miri and sanitizers on `rp-runtime` and `rp-bytes`;
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

- 241k lines of in-scope C++ plus Seastar, ported with behavioural parity
  and a verification stack (simulation, differential harness, model
  checking) that doesn't exist today.
- Assuming 15–20k lines of tested production Rust per engineer-year for
  this class of code, the port alone is 7–12 engineer-years. Simulation,
  the differential harness, test porting, performance work and rollout
  roughly double that. Hence **20–28 engineer-years**. Dropping C++
  compatibility saved about 15–20% compared with the interop strategy.
- With 6–10 engineers who are strong in Rust *and* distributed storage:
  first 3-node cluster milestone (end of Phase 4) at 10–15 months, full
  parity at 2–3.5 years.

The biggest swing factors are the runtime (a weak choice costs a rewrite of
the rewrite) and transactions (Phase 6).

## 10. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| Runtime can't match Seastar's scheduling, I/O fairness or tail latency | Fails C6; possibly redo `rp-runtime` | Phase 0 spike with hard numbers before committing; own the abstraction |
| Subtle semantic drift in transactions, groups or raft | Data loss or duplicates that tests miss | Faithful port with references to the C++; simulation; model checking; differential testing against C++ |
| No interop means internal-protocol bugs are only caught by Rust-side testing | Bugs that a mixed cluster would have exposed reach later phases | Simulation from Phase 0; model checking of raft and transactions; port raft and storage unit tests early |
| Redesign temptation (free formats invite free algorithms) | Drift from proven behaviour; schedule slip | Rule in §3: encodings may change, algorithms are ported; redesigns are post-parity |
| Forgetting Rust-to-Rust upgradability | Every release forces a new cluster | Versioned formats and feature flags from the first commit; N-1 → N tests from the first release |
| Memory behaviour differs (no allocator reclaimer) | OOM or cache thrash under load | Explicit budgets everywhere; memory-pressure tests in simulation and soak |
| Upstream keeps moving (this tree tracks v26.2.x) | Parity target drifts | Freeze the target at v26.2.x slim; batch upstream fixes into a tracked list; re-baseline at agreed points only |
| Ducktape suite is half about removed features | Weak reference | Phase 0 pruning is a gate, not a nice-to-have |
| Team availability (Rust plus distributed storage is a scarce combination) | Schedule | Phase plan lets 2–3 sub-teams work in parallel after Phase 0 |
| Licensing: Redpanda core is under the Business Source License 1.1 (`licenses/bsl.md`) | A close port is very likely a derivative work bound by the same licence and its use grant | Get legal advice before starting; it changes nothing technically, but could change whether to do this at all |
| No authentication or TLS | Anyone on the network can read, write or delete | Document loudly; network isolation in every deployment; leave room in `rp-net` so TLS could be added back without redesign |

## 11. Open decisions

1. **Runtime.** Decided at the end of Phase 0 from spike numbers.
2. **Phone-home metrics reporter** (`cluster/metrics_reporter`, sends
   cluster telemetry to Redpanda). Recommendation: drop, or default off in
   a fork. Parity says keep.
3. **Kafka API versions.** Freeze at today's ranges for parity, or plan
   newer ones (for example the KIP-848 consumer group protocol) as
   post-parity work. Recommendation: freeze.
4. **Vestigial config** (`retention.local.target.*`, `space_management_*`,
   `disk_reservation_percent`). They only exist so stock configs load and
   do nothing in slim. Recommendation: drop them, since Rust clusters start
   from new configs; or accept and ignore them if operators will reuse
   existing config files.
5. **Rust-to-Rust upgradability from the first release.** Recommended above
   (§2). The alternative, allowing format breaks until a declared 1.0, is
   cheaper early on but must end before any production data lands on Rust.

## 12. First 90 days

1. Stand up `src/rust/` as a cargo workspace with CI.
2. Runtime spike: two candidate executors behind a prototype `rp-runtime`
   API, with the simulated backend; benchmark against Seastar.
3. Prune ducktape to the slim, no-auth, no-TLS feature set; get it green on
   the C++ broker.
4. Format specs for RPC, segments, kvstore and snapshots, with versioning.
5. `rp-oracle` v0 running workloads against a C++ cluster and recording
   externally visible results.
6. `rp-bytes`, `rp-codec` derive macro, and Kafka protocol codegen from
   `schemata/*.json`, round-tripping requests captured from a ducktape run.
7. Write the TLA+ (or Stateright) spec for raft reconfiguration while the
   raft port is still months away.
