- Feature Name: Kafka message ID based deduplication
- Status: draft
- Start Date: 2026-06-22
- Authors: Redpanda
- Issue: TBD

# Executive Summary

Add optional producer-side duplicate suppression for Kafka records using the Kafka record key as a client supplied message ID and a per-topic deduplication window. The behavior is modeled after JetStream duplicate windows: the first accepted record for a key is stored, retries with the same key during the configured window are acknowledged as duplicates without appending another record, and the comparison is based only on the serialized key bytes rather than the record value or headers.

## What is being proposed

Redpanda will use the Kafka record key as the message ID for enabled topics, keep a replicated time-bounded index of recently accepted keys per partition, and consult that index on the partition leader before appending produce records. If a key is already present and still within the deduplication window, the broker returns success for that record but does not append it again. Records with null keys continue through the current produce path unchanged unless a future strict mode is added.

## Why (short reason)

Kafka idempotent producers prevent duplicates for a single producer session and sequence number stream, but they do not cover application-level retry keys that survive producer restarts, multiple producers, or cross-service retry orchestration. A record-key deduplication window gives users a practical publishing primitive similar to NATS JetStream duplicate windows: make retries safe by reusing a deterministic application ID as the Kafka key.

## How (short plan)

Implement this as a Raft-replicated partition feature. The leader extracts IDs from Kafka record keys, performs in-memory lookups against a dedup table reconstructed from the partition log plus snapshots, appends only novel records, and records enough metadata in the replicated log to rebuild the dedup table after leadership changes, restarts, and recovery. The table is pruned by record timestamp/append timestamp and the configured window.

## Impact

The feature is opt-in and scoped per topic. It adds CPU for key extraction and hash lookups, memory proportional to publish rate times the deduplication window, and a small amount of replicated metadata. It changes produce acknowledgements only for keyed records that duplicate a still-live key: those requests will succeed without increasing the partition's log end offset. Consumers see only the first accepted record for a key within the window.

# Motivation

## Why are we doing this?

NATS JetStream supports idempotent publication by letting publishers set a `Nats-Msg-Id` header. JetStream tracks IDs in a duplicate window, whose default is two minutes, and rejects duplicate storage based only on that ID, not on payload equality. NATS documentation describes this as a sliding-window stream property, with `DuplicateWindow` controlling how long IDs are tracked. This feature gives Kafka users a comparable application-level idempotency primitive in Redpanda.

Research notes:

- JetStream duplicate suppression is triggered by the `Nats-Msg-Id` publish header.
- JetStream consults only the message ID; a retry with the same ID and different body is still a duplicate.
- JetStream's default duplicate tracking window is two minutes and can be configured per stream.
- Operationally, the server retains an ID-to-sequence mapping in memory for the window, so memory grows with publish rate times window duration.

Sources: [NATS JetStream model deep dive](https://docs.nats.io/using-nats/developer/develop_jetstream/model_deep_dive), [NATS stream configuration](https://docs.nats.io/nats-concepts/jetstream/streams), [NATS JetStream headers](https://docs.nats.io/nats-concepts/jetstream/headers), and [Synadia large deduplication window note](https://www.synadia.com/insights/checks/nats-large-deduplication-window).

## What use cases does it support?

- A producer times out after a successful append and retries with the same deterministic message ID.
- A producer process restarts and loses Kafka idempotent producer state, but its business event still has a stable event ID.
- Multiple active producers race to publish the same business event and use a shared event ID.
- An upstream workflow engine retries a step after failover and must avoid creating duplicate Kafka records.

## What is the expected outcome?

For enabled topics, users can publish records with a stable Kafka record key and rely on Redpanda to store at most one record with that key within the deduplication window for the target partition. The feature should continue to work across leadership transfer, node restart, follower promotion, partition movement, and cluster upgrades.

# Guide-level explanation

## How do we teach this?

Deduplication is a topic-level produce option. Users choose:

- `redpanda.message.id.deduplication.enabled=true`
- `redpanda.message.id.deduplication.window.ms=<duration>`; default proposal: `120000`
- optional limits for maximum key length and maximum tracked keys per partition

Producers attach a stable Kafka record key to each record that needs retry protection. The Kafka record key itself is the message ID:

```text
key = "order-2026-06-22-000123"
value = {...}
headers = {...}
```

This is appropriate when applications already use the Kafka key as the business event ID, or when they want deduplication to align with Kafka partitioning and existing compacted-topic patterns.

The first produce request that reaches the partition leader and passes validation is appended. Any later produce request to the same partition with the same key during the deduplication window is acknowledged successfully but not appended. Consumers therefore see the first record only. If the second produce has a different value, timestamp, or headers, those fields do not matter. The exact serialized key bytes alone control duplicate detection.

The deduplication boundary is the Kafka partition. This matches Kafka ordering and routing semantics: a key is unique only among records produced to the same topic partition. If an application needs global topic-level deduplication, it must ensure records with the same key use the same partitioning key or explicit partition.

### Message-ID source: record key

The Kafka record key is the only message-ID source in this proposal. Redpanda must use the exact serialized key bytes as the ID. It should not deserialize, normalize, or schema-interpret the key, because producers using different serializers could otherwise disagree about equality. A null key means there is no message ID for that record and the record is not deduplicated.

A duplicate response should be observable but protocol-compatible. The Kafka ProduceResponse has no standard per-record duplicate marker, so Redpanda should return a normal successful partition response. Optional future surfacing can use broker logs, metrics, and an Admin API lookup rather than altering Kafka protocol behavior.

# Reference-level explanation

## Interaction with other features

- Idempotent producers and transactions: message-ID deduplication runs after Kafka request validation and before append. It is complementary to producer ID/sequence validation. For transactional records, duplicates are suppressed within the transaction append path, and duplicate metadata is committed atomically with the transaction's replicated batches.
- Compaction and retention: the dedup index is independent of data retention. IDs expire by the deduplication window even if the original record remains in the log; IDs may also be unavailable before the window if an operator lowers the window and triggers pruning.
- Tiered storage and recovery: recovery must not depend on scanning remote segments on every startup. The implementation should persist compact dedup snapshots locally and in Raft snapshots so a promoted replica can rebuild recent IDs without remote-log reads.
- Schema Registry, transforms, and consumers: unchanged, because duplicate records are never appended.
- MirrorMaker and cluster linking: deduplication applies to produce requests received by a Redpanda leader. Replicated records that already lack duplicates do not require special consumer-side behavior.

## Telemetry & Observability

Expose per-topic-partition metrics:

- deduplication enabled flag and configured window
- current tracked ID count and estimated bytes
- lookup count, hit count, miss count, and hit ratio
- evicted ID count by age and by capacity pressure
- rejected records due to malformed ID, ID too large, or limits
- dedup snapshot write/read latency and failures
- leadership recovery rebuild time and recovered ID count

Add debug/admin surfaces:

- `rpk topic describe` should show deduplication config.
- Admin API partition status should include tracked ID count, oldest tracked timestamp, newest tracked timestamp, and last snapshot offset.
- Optional diagnostic endpoint can check whether a message ID is currently tracked for a topic partition without exposing the full table by default.

## Corner cases dissected by example

- Same ID, same partition, within window: append the first record; acknowledge but skip subsequent records.
- Same ID, same partition, after window: append again, because the ID expired.
- Same ID, different partition: append independently on each partition.
- Same ID, different payload: treat as duplicate if the ID is still tracked.
- Null record key: append normally and do not add to the dedup table.
- Empty key: treat as a valid message ID because it is a valid serialized key byte sequence.
- Oversized key: reject with `INVALID_RECORD` and count a validation metric.
- Batched records with duplicates inside the same produce request: append the first occurrence in batch order and suppress later occurrences.
- Partial batch filtering: if only some records in a batch are duplicates, the broker must rewrite the record batch or split it so offsets, CRCs, and record counts remain valid.
- Leader change between original produce and retry: the new leader rebuilds the dedup table from replicated metadata before accepting writes, so the retry is suppressed.
- Clock skew: use broker append time or log offset ordering for expiration, not producer wall-clock time alone.
- Window config changes: increasing the window only affects records from the change point forward unless retained metadata is sufficient; decreasing the window prunes immediately after the config is applied.

## Detailed design - What needs to change to get there

1. Product/API definition
   - Define topic properties for enablement, window duration, max key length, and max tracked IDs/bytes.
   - Document that the Kafka record key is the message ID; no custom message-ID header is supported.
   - Define duplicate acknowledgement semantics as successful produce without append.

2. Kafka request parsing
   - Extend the produce path to read the Kafka record key only when deduplication is enabled for the target topic.
   - Treat the exact serialized key bytes as the message ID and validate key size.
   - Preserve the fast path for disabled topics and null-key records.

3. Partition-local dedup index
   - Add an in-memory index keyed by message ID hash plus collision-safe stored bytes.
   - Store first accepted offset, append timestamp, and expiry timestamp.
   - Maintain an expiry queue ordered by expiry timestamp for efficient pruning.
   - Account memory against partition resources and expose backpressure/rejection behavior when limits are exceeded.

4. Replicated metadata
   - Append dedup metadata in the same Raft operation as accepted user records, or encode the message ID in a deterministic sidecar batch adjacent to the data batch.
   - Ensure followers apply the same ID additions in log order.
   - For duplicates, do not append a user record. A separate duplicate marker is not required unless needed for auditing; metrics on the leader are sufficient.

5. Batch rewrite path
   - Implement record-batch filtering for mixed novel/duplicate records.
   - Recompute record offsets, batch count, CRC, timestamps, and attributes correctly.
   - Preserve transactional and idempotent-producer invariants.

6. Snapshot and recovery
   - Include live dedup entries in partition snapshots or a local persistent snapshot tied to a committed offset.
   - On startup or leadership acquisition, load the latest snapshot and replay subsequent log entries to reconstruct the table before writes are accepted.
   - Validate snapshot staleness against the current window and append timestamps.

7. Config propagation
   - Store topic deduplication properties in cluster metadata.
   - Make leaders update their partition dedup subsystem when topic configs change.
   - Add validation to prevent unsafe values, such as unbounded windows without explicit memory caps.

8. Tests
   - Unit-test ID extraction, validation, hash collision handling, pruning, and batch rewriting.
   - Integration-test duplicate suppression, expiry, intra-batch duplicates, leadership transfer, node restart, partition movement, transactions, idempotent producers, and config changes.
   - Add ducktape tests with multi-broker clusters to prove cross-cluster behavior through failover and recovery.
   - Add performance benchmarks for disabled path overhead, enabled path throughput, and memory growth.

9. Documentation and rollout
   - Document the feature as application-level produce deduplication, not exactly-once end-to-end processing.
   - Warn that memory grows with publish rate multiplied by the window.
   - Roll out behind a feature flag and require all brokers to support the feature before enabling it on topics.

## Detailed design - How it works

Each partition owns a `message_id_deduplicator` on the shard that owns the partition leader state. The deduplicator has two responsibilities: filtering incoming records and applying replicated additions from the log.

On produce:

1. The leader checks whether deduplication is enabled for the topic.
2. If disabled, it uses the existing append path.
3. If enabled, it iterates records and extracts the Kafka record key as the message ID.
4. For each ID, the leader prunes expired entries, then checks the index.
5. If no live entry exists, the record is retained for append and a pending dedup entry is associated with it.
6. If a live entry exists, the record is removed from the append set and a duplicate metric is incremented.
7. If all records are duplicates, the leader returns success without appending user data.
8. If any records are novel, the leader appends filtered user batches plus replicated dedup metadata.
9. Once the append is committed according to the request's required acknowledgements, the produce response is returned.

On follower apply:

1. The follower reads committed batches in log order.
2. For each dedup metadata entry, it inserts the ID and offset/timestamp into its in-memory table.
3. Expired entries are pruned using the same append-time basis.

On leadership change:

1. The replica waits until it has applied through its committed log end and loaded the latest dedup snapshot.
2. It rejects or stalls produce requests until the dedup table is ready.
3. It then serves lookups locally. No quorum read is needed because the Raft log is the source of truth.


## Cluster-correct implementation plan

The produce-path prototype is not sufficient for clustered Redpanda because an in-memory, `thread_local` table on the current leader is not part of the partition's replicated state. The cluster-correct implementation must make the deduplication index a deterministic, partition-owned state machine whose source of truth is the Raft log.

### Phase 0: Remove the unsafe prototype behavior

- Do not enable deduplication from a process-local global or `thread_local` table.
- Keep batch-filtering helpers only if they are moved behind topic-level enablement and wired to partition-owned replicated state.
- Add a feature flag/version gate so no topic can enable message-ID deduplication until every broker understands the metadata batch format and recovery rules.

### Phase 1: Define replicated deduplication metadata

Add a versioned internal metadata record for every accepted message ID:

- topic/partition identity is implicit from the log receiving the metadata;
- message ID bytes;
- accepted record offset;
- append timestamp used for window expiry;
- optional producer identity and sequence information for diagnostics;
- metadata schema version.

Encode this metadata as either:

1. a new data-partition internal batch type that is included in `offset_translator_batch_types()`, so it is replicated but not exposed to Kafka consumers; or
2. a command in a partition STM dedicated to message-ID deduplication.

The preferred path is a partition STM because it gives a natural place for apply, snapshot, recovery, and leadership readiness logic. The STM should still write compact metadata batches to the partition log so followers and future leaders can rebuild the same dedup table.

### Phase 2: Add a partition-owned deduplication STM

Create `cluster::message_id_dedup_stm` with the following responsibilities:

- maintain an in-memory lookup table keyed by exact message ID bytes;
- maintain an expiry queue ordered by append timestamp plus configured window;
- apply replicated accepted-ID commands in log order on leaders and followers;
- expose `is_duplicate(id)`, `filter_records(batch)`, and `prepare_append_metadata(ids)` APIs to the produce path;
- snapshot live entries and the last applied offset;
- reject or stall produce requests until recovery has loaded the snapshot and replayed through the committed log end.

The STM must be owned by the partition, not by the Kafka request handler. This keeps the state on the shard that owns the partition and lets leadership transfer use the same recovery path as other replicated partition state.

### Phase 3: Make append and dedup metadata atomic

The leader must never append user records without also making their message IDs recoverable. Use one of these atomicity strategies:

- append a single Raft replicate batch group that contains filtered user data and the dedup metadata command;
- or have the STM replicate a command that includes both the accepted IDs and the offsets assigned by the append path;
- or append user data first but delay produce success until the dedup metadata has also been durably replicated, with recovery code able to repair any committed data that lacks metadata.

The first option is preferred: the same Raft operation should commit the filtered records and the corresponding accepted-ID metadata. If that is not possible with the current append API, introduce a partition-level append method that accepts a vector of batches and returns one combined replication result.

### Phase 4: Rebuild state on followers, restart, and leadership transfer

Follower apply must insert accepted IDs into the STM as metadata commands are replayed. Restart recovery must:

1. load the latest dedup snapshot;
2. drop snapshot entries already outside the current deduplication window;
3. replay subsequent metadata commands from the log;
4. prune by append timestamp;
5. mark the STM ready only after replay reaches the partition's committed offset.

Leadership transfer must gate produce requests on `message_id_dedup_stm::is_ready()`. Until ready, the leader should return a retriable error or wait within the produce timeout. Once ready, duplicate checks are local and require no cross-node request because Raft log replay has synchronized the state.

### Phase 5: Wire topic-level configuration

Add topic properties for:

- `redpanda.message.id.deduplication.enabled`;
- `redpanda.message.id.deduplication.window.ms`, defaulting to `120000`;
- `redpanda.message.id.max.key.bytes`;
- optional per-partition memory/entry caps.

Configuration is stored in cluster metadata and applied to partitions through the existing topic-property update path. The STM should observe config changes and prune immediately when the window shrinks. Increasing the window should only apply to entries that are still recoverable from metadata and snapshots; otherwise the effective increase begins from the config-change point.

### Phase 6: Integrate with the Kafka produce path

The Kafka handler should remain mostly stateless:

1. Validate the request and schema as it does today.
2. Route to the leader shard.
3. Ask the partition-owned STM to filter the batch using the Kafka record key as the ID.
4. If every record is duplicate, return a successful ProduceResponse using the original accepted offset retained by the STM.
5. If any records are novel, append filtered user batches plus dedup metadata atomically.
6. Return success only after the required acknowledgement level is satisfied.

All duplicate decisions must come from the STM, never from a request-handler-global table.

### Phase 7: Correctness tests for clustered behavior

Add tests that fail against the prototype and pass with the replicated design:

- duplicate suppressed on the same leader;
- duplicate suppressed after leadership transfer;
- duplicate suppressed after broker restart inside the window;
- duplicate suppressed after partition movement/reallocation;
- duplicate allowed after the deduplication window expires;
- follower promotion waits for dedup STM readiness before accepting produce;
- mixed batches preserve Kafka offset, timestamp, transaction, and idempotent-producer invariants;
- transactional append plus dedup metadata is atomic across commit and abort paths;
- old brokers reject enabling the topic property during rolling upgrade;
- memory caps produce deterministic errors or backpressure without corrupting STM state.

### Phase 8: Observability and operations

Expose per-partition metrics from the STM:

- tracked ID count and estimated memory;
- duplicate lookup hits/misses;
- IDs pruned by time and by memory pressure;
- snapshot read/write latency;
- replay duration and readiness state;
- rejected records due to malformed or oversized IDs.

Add an Admin API diagnostic endpoint to inspect aggregate state for a topic partition and optionally check whether a specific message ID is currently live in the dedup window.

# Drawbacks

- Memory can grow quickly for high-throughput topics with long windows.
- Produce latency increases because the broker must extract keys and possibly rewrite batches.
- Kafka clients cannot receive a standard duplicate marker in ProduceResponse.
- Cross-partition deduplication is intentionally not provided, which may surprise users expecting topic-wide uniqueness.
- Persisting and snapshotting the index adds recovery complexity.

Mitigations include strict defaults, topic-level opt-in, memory caps, clear metrics, and documentation that mirrors JetStream's guidance against very large windows.

# Rationale and Alternatives

## Why this design?

A partition-local, Raft-replicated dedup table aligns with Kafka's append and ordering model. It avoids introducing a global coordinator in the produce path, continues to work after failover, and makes correctness depend on the same replicated log that already controls partition state.

## Alternatives considered

- Client-only deduplication: cheaper for the broker but does not protect against producer restarts, multiple producers, or uncertain append outcomes.
- Kafka idempotent producers only: already available but scoped to producer IDs and sequence numbers, not application message IDs.
- Global topic-level deduplication service: offers stronger uniqueness but adds cross-partition coordination, availability tradeoffs, and significant latency.
- Consumer-side deduplication: prevents duplicate effects only for consumers that implement it and still stores duplicate records.
- Infinite deduplication: closer to JetStream's newer per-subject discard patterns, but it requires different retention semantics and can create unbounded state. This RFC targets the common sliding-window behavior first.

## Impact of not doing this

Users that need retry-safe application event publication must continue to build deduplication outside Redpanda or accept duplicates in Kafka topics. That increases operational complexity and makes Redpanda less attractive for workloads migrating from systems that provide server-side message-ID deduplication.

# Unresolved questions

- Should duplicates be exposed through a Redpanda-specific produce response extension, logs only, or metrics only?
- Should the dedup metadata be encoded in a new internal batch type or embedded in existing record-batch metadata?
- What are the default and maximum allowed values for window duration, key length, and memory usage?
- How should duplicates inside aborted transactions be represented in the dedup table?
- Can we avoid batch rewriting in v1 by requiring all records in a compressed batch to be either accepted or rejected, or is per-record filtering mandatory for user expectations?
