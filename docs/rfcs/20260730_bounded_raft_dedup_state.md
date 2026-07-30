- Feature Name: Bounded replicated write-path deduplication state
- Status: in-progress
- Start Date: 2026-07-30
- Authors: @almostintuitive
- Issue: dream-faster/redpanda#2

# Executive Summary

The replicated write-path deduplication STM currently retains a reverse undo
record for every state update so that it can construct Raft snapshots at
arbitrary historical offsets. This makes memory and local snapshot size grow
with the retained lifetime of the topic rather than with the active
deduplication window. Its metadata batches are also protected from compaction,
so compacted topics retain copies of admitted keys indefinitely. Finally, the
leader serializes all plain produce requests through full Raft completion and
followers build undo records with quadratic scans.

This RFC replaces reverse history with replicated full-state checkpoints and
forward mutations, coordinates checkpoint retention with Raft snapshot
requirements, reclaims metadata covered by checkpoints, and replaces the
leader's full speculative copy with a bounded in-flight overlay. The result
preserves the existing first-wins semantics while bounding memory, local
snapshot size, and retained metadata by active state plus configured
checkpoint and in-flight limits.

## What is being proposed

The dedup STM will persist periodic, chunked checkpoints of its complete active
state. Between checkpoints it will replicate forward mutations containing key
puts, key removals, generation resets, and window changes. Raft recovery will
pin exact historical snapshot targets before dedup metadata needed by those
targets may be reclaimed. Followers will apply mutations directly without
constructing undo records. Leaders will classify and enqueue requests under a
short ordering lock, then wait for Raft and duplicate dependencies outside the
lock.

## Why

The current `_undo_history` and uncompacted metadata side log are unbounded.
The current write lock also prevents Raft batching for plain producers, while
the undo builder can perform tens of millions of comparisons for a large
batch. These properties make the feature unsafe for compacted, high-throughput,
or long-retention topics.

## How

Implementation is split into independently testable milestones:

1. Add observability and replace quadratic undo construction with linear
   mutation collection.
2. Introduce a versioned forward-mutation format.
3. Add replicated checkpoints and reconstruct snapshots by checkpoint plus
   forward replay.
4. Remove `_undo_history` from memory and local snapshots.
5. Add checkpoint-aware metadata reclamation.
6. Replace the full speculative map with an overlay and pipeline produce
   requests.
7. Add failure, scale, compaction, and recovery coverage.

## Impact

The wire representation of dedup metadata and the persisted local snapshot
format change. Readers remain backward compatible during the transition, while
new writers are activated only after the cluster feature gate is active.
Checkpoint creation adds periodic network and disk traffic proportional to the
active dedup index. This is bounded and configurable, replacing unbounded
per-write historical storage.

# Motivation

## Why are we doing this?

`dedup_stm::apply_update()` appends one undo record per metadata update.
`take_local_snapshot()` deep-copies and serializes the complete undo history.
History is pruned only when the Raft start offset advances. Compacted-only
topics may never advance that offset, so both resident and persisted state grow
for the lifetime of the topic.

`dedup_state_update` is an offset-translator batch type and is therefore
currently non-filterable by compaction. This protects recovery correctness, but
it creates a permanent side log containing copies of admitted keys.

The leader holds `_enqueue_mutex` through `replicate_finished`, limiting plain
produce throughput to one completed request per partition. Followers also use
linear searches to make undo entries unique, giving large batches quadratic
apply cost.

## What use cases does it support?

- Compacted topics with no delete retention.
- Topics with long or infinite retention.
- High-cardinality deduplication windows.
- High-throughput plain producers that depend on Raft batching.
- Large record batches that cross the 10,000-insert eviction threshold.
- Leadership changes, local restarts, Raft snapshot installation, and learner
  recovery without rebuilding unbounded reverse history.

## What is the expected outcome?

- Resident dedup memory is `O(active keys + bounded in-flight mutations)`.
- Local snapshot size is `O(active keys)`.
- Retained dedup metadata is bounded by the latest required checkpoint plus a
  configured mutation allowance.
- Follower apply is expected `O(mutations + evictions)`.
- Independent plain produces can be in flight concurrently.
- Existing first-wins, timestamp, null-key, generation, and producer-mode
  semantics remain unchanged.

# Guide-level explanation

The committed dedup map is the authoritative state applied from Raft. A
replicated checkpoint periodically captures the complete map and its
non-key state:

- window,
- generation,
- maximum observed timestamp, and
- insertion count used to schedule eviction.

After a checkpoint, each admitted produce emits a forward mutation before its
data batch in the same Raft replication request. A mutation contains the keys
that were admitted and the keys that were removed by eviction. Applying a
checkpoint followed by all later mutations produces exactly the same state as
the leader.

Checkpoints are chunked because an active index may be larger than a Raft
request. A checkpoint is usable only after its completion marker and checksum
are committed. An incomplete checkpoint is ignored, and the previous complete
checkpoint remains authoritative.

The leader does not copy the complete committed map. It keeps a small overlay
for changes introduced by requests that are enqueued but not yet reflected in
the committed STM. Lookup checks the overlay first and the committed map
second. A term change or replication failure discards the overlay.

# Reference-level explanation

## Required invariants

1. State metadata precedes its associated data in one ordered Raft request.
2. No follower may apply a data mutation not chosen by the leader.
3. A checkpoint is installed only when all chunks and its completion marker
   are committed and its checksum is valid.
4. Metadata needed by an exact pinned snapshot target cannot be reclaimed.
5. A request may not acknowledge a duplicate more strongly than the Raft entry
   that introduced the key.
6. A failure after speculative classification invalidates all dependent
   speculative decisions before another request can be admitted.
7. Idempotent and transactional producers continue to bypass this feature.
8. Null-key records are always admitted and never enter the index.

## The snapshot/compaction constraint

The system cannot provide all three of the following without coordination:

- snapshots at every historical retained offset,
- reclamation of all historical dedup mutations, and
- no external multi-version state.

Raft must therefore announce or pin exact snapshot targets before they become
historical. The STM exposes its oldest reconstructible offset. Learner recovery
prefers a current committed snapshot; retention postpones truncation rather
than requesting an offset whose state has already been reclaimed.

## Forward mutation format

Introduce a versioned mutation envelope with:

- `window_ms`,
- `generation`,
- ordered admitted key/timestamp puts,
- ordered evicted-key deletes,
- resulting maximum timestamp,
- resulting insertion counter, and
- an optional generation-reset marker.

Deletes are explicit because physical membership in the map is observable for
out-of-order timestamps. Reconstructing only the latest timestamp per key would
not preserve the current eviction semantics.

The existing update format remains readable. Writing the new format is guarded
by a cluster feature flag so mixed-version replicas do not disagree about
offset translation or state application.

## Checkpoint protocol

A checkpoint consists of:

1. a begin record containing checkpoint ID, covered state offset, state
   metadata, entry count, and expected checksum;
2. ordered entry chunks;
3. a completion record containing checkpoint ID, chunk count, and checksum.

Checkpoint records are enqueued contiguously under the same ordering mechanism
as produces. New produces may be enqueued after the checkpoint records without
waiting for checkpoint commitment. If preceding speculative writes fail, the
term is invalidated and the incomplete checkpoint is ignored.

Checkpoint triggers are:

- configured mutation bytes,
- configured mutation count,
- maximum checkpoint age,
- generation reset,
- approaching the retained metadata budget, and
- a Raft snapshot requirement that lacks a suitable checkpoint.

Only one checkpoint is built per partition at a time.

## Raft and local snapshots

`take_raft_snapshot(target)` selects the newest retained complete checkpoint at
or before `target`, then replays forward dedup mutations through `target`.
Replay uses a type-filtered log reader, yields periodically, and is covered by
the STM snapshot/apply lock.

If `target` equals the current applied offset, the STM serializes its current
committed state directly.

The local snapshot persists:

- current committed state,
- the latest complete checkpoint catalog/state required for reconstruction,
- its covered offset, and
- format version.

It does not persist per-update history.

## Metadata reclamation

Offset translation and compaction eligibility are separated. Dedup batches
remain invisible to Kafka offsets, but a dedup-specific compaction rule may
remove:

- checkpoints superseded by a newer complete retained checkpoint, and
- forward mutations covered by that retained checkpoint,

provided no pinned snapshot target needs them.

Incomplete checkpoints are never considered coverage boundaries. Removing a
batch must preserve Raft offset gaps using the existing compaction placeholder
mechanism.

The safe fallback is to retain metadata. Reclamation must never make a
previously advertised exact snapshot target unreconstructible.

## Pipelined leader path

The enqueue-order lock covers:

1. term synchronization,
2. speculative lookup and mutation,
3. dependency-token creation, and
4. ordered Raft enqueue.

It is released after `request_enqueued`.

Each speculative key references the request token that introduced its current
state. Duplicate requests wait outside the lock for their dependencies. If a
duplicate requests quorum acknowledgment but the introducing request used
leader or no acknowledgment, it additionally waits for the introducing offset
to commit and validates current leadership.

Queue acquisition, dependency waits, and checkpoint work honor the caller's
timeout and abort source. In-flight bytes and request count are bounded.

## Speculative overlay

The leader overlay stores key puts and deletes relative to committed state.
Lookup checks the overlay before `_state`. Applied committed mutations retire
overlay entries only when no later speculative mutation for the same key
exists. Term changes and replication failures discard the entire overlay.

This avoids maintaining two complete maps.

## Telemetry & Observability

Add metrics for:

- active committed keys,
- speculative overlay keys,
- mutations and bytes since checkpoint,
- checkpoint size, age, duration, offset, and failures,
- retained dedup metadata bytes,
- snapshot replay bytes and duration,
- produce queue depth and queued bytes,
- enqueue-lock wait duration,
- in-flight dependency count,
- apply and eviction duration, and
- metadata bytes reclaimed by compaction.

Logs should identify checkpoint ID, covered offset, completion offset,
generation, entry count, serialized size, and reason for checkpoint creation.

## Failure cases

### Crash during checkpoint construction

No completion marker exists, so recovery ignores the partial checkpoint and
uses the previous complete checkpoint.

### Crash after completion but before local snapshot

Recovery finds the checkpoint in Raft and replays later mutations.

### Replication failure after speculative classification

The leader steps down before accepting another dependent request. The next
leader reconstructs state from committed metadata.

### Snapshot target older than retained history

Learner recovery requests a current snapshot or falls back to log recovery.
Retention postpones truncation. State is never approximated.

### Generation change

A reset mutation clears the active state. No full previous-state undo record is
created. A checkpoint is scheduled so old-generation metadata can become
reclaimable.

## Implementation milestones

### Milestone 1: Linear mutations and observability

- Replace undo-vector linear searches with hash-based uniqueness tracking.
- Return evicted keys from leader filtering.
- Add mutation/apply/checkpoint placeholder metrics.
- Add a 10,000-key apply/eviction benchmark and correctness test.

Exit criteria:

- Apply cost scales linearly in the focused benchmark.
- No semantics or wire-format change yet.

### Milestone 2: Versioned forward mutations

- Add the new mutation envelope and compatibility reader.
- Apply puts/deletes/reset directly on followers.
- Gate new writes behind a feature flag.
- Differential-test old and new mutation application.

Exit criteria:

- Leader and followers converge across randomized batches, window changes, and
  generation changes.

### Milestone 3: Replicated checkpoints and bounded snapshots

- Implement checkpoint begin/chunk/end records.
- Implement current-state and checkpoint-plus-replay Raft snapshots.
- Version the local snapshot and stop writing undo history.
- Read legacy local snapshots during migration.
- Remove `_undo_history` after compatibility coverage passes.

Exit criteria:

- Resident history and local snapshot size remain constant as total writes
  increase with a fixed active-key population.
- Snapshot installation followed by leadership produces identical decisions.

### Milestone 4: Metadata reclamation

- Add the pinned-target/checkpoint retention contract to Raft STM management.
- Decouple offset-translation filtering from compaction eligibility.
- Add checkpoint-aware dedup metadata filtering.
- Preserve offset translation through placeholders.

Exit criteria:

- A compacted-only stress topic retains bounded dedup metadata.
- Crash, restart, snapshot installation, and leadership after compaction retain
  exact decisions.

### Milestone 5: Pipelining and speculative overlay

- Replace the full speculative map with an overlay.
- Release enqueue ordering after Raft enqueue.
- Track duplicate dependencies and acknowledgment strength.
- Add timeouts, cancellation, queue bounds, and failure invalidation.

Exit criteria:

- Multiple independent plain produces are concurrently in flight.
- Same-key requests still admit exactly one first record.
- Mixed acknowledgment levels cannot acknowledge an uncommitted dependency.

### Milestone 6: Scale and failure validation

- Add randomized differential tests against the sequential reference filter.
- Add crash injection at every checkpoint stage.
- Add snapshot target, truncation, and compaction tests.
- Add restart and leadership-change tests while checkpoints and writes are in
  flight.
- Add queue timeout, cancellation, and shutdown tests.

Exit criteria:

- Memory, local snapshots, and retained metadata are bounded by active state
  plus configured checkpoint/in-flight limits, not topic age.

## Commit and review sequence

Implementation should remain reviewable as the following commits:

1. `cluster: make dedup mutation application linear`
2. `cluster: add versioned dedup forward mutations`
3. `cluster: checkpoint replicated dedup state`
4. `raft: pin STM snapshot reconstruction targets`
5. `storage: reclaim checkpointed dedup metadata`
6. `cluster: pipeline dedup replication with speculative overlay`
7. `cluster: stress bounded dedup recovery and compaction`

Each commit must keep focused unit tests passing. Wire-format activation and
metadata reclamation must not occur before the corresponding feature gate is
active.

## Drawbacks

- Full checkpoints periodically replicate all active keys.
- Checkpoint creation and replay add implementation complexity.
- Raft snapshot target pinning affects recovery and retention coordination.
- Metadata compaction needs awareness beyond ordinary last-key-wins compaction.
- Pipelining requires explicit dependency tracking and more failure tests.

These costs replace unbounded resource growth and partition-wide write
serialization. Checkpoint thresholds and queue budgets provide operational
control over the trade-off.

## Rationale and Alternatives

### Keep bounded reverse history

A fixed-size undo ring cannot satisfy an arbitrary old snapshot request.
Silently dropping old undo records would create incorrect snapshots.

### Replay the complete retained log on every snapshot

This bounds memory but remains unbounded in time and I/O, and it conflicts with
reclaiming old metadata.

### Store all history in kvstore

This moves the unbounded problem from memory into local disk and complicates
replica recovery.

### Use only per-key compaction

Last-key-wins metadata is enough for the current logical timestamp per key, but
not for exact historical snapshots or the current observable eviction
semantics. Explicit checkpoints and deletes are required.

### Disable arbitrary-offset snapshots

This would disable fast reconfiguration for every user partition containing
the STM and does not by itself solve retention-driven snapshot creation.

## Unresolved questions

- Whether learner recovery can always install a newer current snapshot instead
  of the original learner-start offset.
- The exact API for pinning and releasing STM snapshot targets.
- Whether checkpoint-aware filtering belongs in generic compaction or in a
  dedup-specific compaction index.
- Default checkpoint byte, count, and age thresholds.
- Whether checkpoint chunk assembly should use bounded memory or a temporary
  kvstore value on followers.
- Which dedup metrics should be public versus internal-only.
