- Feature Name: Log-derived write-path deduplication state
- Status: in-progress
- Start Date: 2026-07-30
- Authors: @almostintuitive
- Issue: dream-faster/redpanda#2
- Supersedes: `20260730_bounded_raft_dedup_state.md` (removed on this branch;
  see git history)

# Executive Summary

The replicated dedup STM currently maintains a metadata side-channel in the
Raft log (`dedup_state_update` batches carrying admitted keys, forward
mutations, and chunked checkpoints) so that every replica can reconstruct the
dedup index byte-exactly at arbitrary historical offsets. That requirement is
the root of nearly all of the implementation's complexity and its open
problems: unbounded undo history, checkpoint assembly and retention
coordination, compaction-aware metadata reclamation, snapshot-target pinning,
and a leader-side speculative map with an in-flight dependency fence.

This RFC removes the requirement. The dedup index becomes a **deterministic
function of the data batches already in the Raft log**: followers derive it by
reading the replicated produce batches themselves, and byte-exact historical
reconstruction is replaced by **convergent** snapshots, which are safe because
the index is advisory — only the leader consults it, only before replication,
and divergence can never corrupt the log.

The metadata side-channel, the checkpoint protocol, the undo history, the
speculative map, and the enqueue mutex are all deleted. What remains is the
existing well-tested `dedup_window_filter` plus a thin
(`~250` line) `persisted_stm`.

# Why the advisory-state framing is sound

Three facts, none of which depend on implementation details:

1. **Every admitted record already lands in the replicated data batch**, with
   its key and timestamp — exactly the information the index stores. The
   metadata batches replicate a second copy of data every replica already
   receives.

2. **Only the leader consults the index, and only before replication.** A
   follower's copy has no effect until the moment it becomes leader, at which
   point `sync()` guarantees it has applied the committed log prefix. Filter
   divergence between replicas therefore cannot corrupt the log or violate
   Raft invariants; at worst it changes whether a duplicate near the window
   boundary is caught.

3. **The feature's own semantics are approximate.** First-wins deduplication
   within a *client-supplied-timestamp* time window is inherently best-effort:
   the window boundary is fuzzy by construction. Paying for byte-exact state
   reconstruction to defend a fuzzy boundary is complexity spent where the
   semantics cannot use it.

Consequence: `take_raft_snapshot(target)` may serialize the *current* index
state even when `target` is older than the applied offset. Because
`populate()` is idempotent (max-timestamp-wins per key), installing that
snapshot at `target` and replaying `(target, applied]` converges to exactly
the state the serializing node holds — the "extra" entries in the snapshot are
precisely the ones replay would re-insert. Divergence is limited to eviction
bookkeeping, which only shifts when expired entries are swept.

# Semantics contract

Guarantees (unchanged from the current branch):

- **G1** — On a stable leader, a keyed record is dropped iff a record with the
  same key and a timestamp within `dedup_window_ms` was previously admitted.
- **G2** — The index survives restarts, leadership changes, and snapshot
  installation: any replica that becomes leader first syncs to the committed
  log, and the log deterministically implies the index.
- **G3** — Log consistency and Kafka offset translation never depend on
  filter state. There are no dedup batches in the log at all.
- **G4** — Classification order does not imply log order (an introducing
  request inserts into the map before its batch is appended), so any request
  that observed a duplicate first waits on the **append-order fence**: a
  shared promise resolved when the most recently admitted request's batch has
  been appended. This is a conservative global fence, not per-key tracking.
  After it, a *partially* deduplicated request replicates its own batch,
  whose offset now follows the introducing appends — the Raft prefix
  property makes its quorum ack cover them.
- **G5** — A request that is *entirely* deduplicated (nothing to replicate)
  and requests `quorum_ack` waits, after the append fence, for the committed
  offset to reach the current dirty offset before acking. The introducing
  entries are therefore durable when the duplicate is acknowledged. This
  closes the "acked duplicate, introducing write lost" hole without per-key
  dependency tracking. The wait is against `consensus::events()`
  (`raft::event_manager::wait()`), which waits on the real commit index --
  not `visible_offset_monitor()`/`last_visible_index()`, which can advance
  past the true flush-durable commit point under relaxed-consistency
  traffic on the same partition (`consensus::maybe_update_last_visible_index()`
  can raise `last_visible_index()` to `_majority_replicated_index` --
  majority-*replicated*, not flush-durable -- once no quorum-with-flush
  write is pending, whereas `committed_offset()` stays clamped to
  `_flushed_offset`). `events().wait()` takes an `ss::abort_source&` by
  reference rather than `offset_monitor::wait()`'s optional one; `opts.as`
  (`do_replicate`'s own `replicate_options::as`) is used when the caller
  supplied one, falling back to a throwaway, never-triggered local
  `abort_source` otherwise -- equivalent to what passing `std::nullopt`
  through the optional-typed wait already meant, since the timeout is
  still enforced independently of the abort source either way.
- **G6** — Idempotent and transactional producers bypass the filter
  (unchanged). Null-key records are always admitted and never indexed
  (unchanged).

Best-effort boundaries (all bounded by one dedup window, all documented):

- **B1** — If replication of an admitted request fails, the leader reverts the
  request's index entries (compare-and-swap semantics, see below). If the
  process crashes between insertion and revert, stale entries persist locally
  until evicted; a later duplicate of a lost write may be dropped. Bounded by
  the window; disappears at the next leadership change (log-derived state has
  no such entries).
- **B2** — Historical Raft snapshots are convergent, not byte-exact: a learner
  installing one may briefly hold entries "from the future" relative to its
  replay position. Self-corrects by idempotent replay; affects only
  window-edge eviction timing.
- **B3** — Enabling dedup on an existing topic starts the index from the
  enable point *while the partition keeps running without a restart*:
  `do_apply()` only calls `populate()` once dedup is configured, so older
  batches applied before that point are never retroactively indexed. Across a
  restart (or any other STM re-instantiation), `get_initial_recovery_policy()`
  alone would have no notion of the offset dedup was actually enabled at —
  `read_everything` unconditionally means "replay from offset 0" (see
  `state_machine_manager::apply_initial_recovery_policy()`), which does not
  fit in a shard's memory budget for a large topic with a long retention
  window. `get_initial_recovery_start_offset()` (checked first, ahead of the
  read_everything/skip_to_end policy) closes this: it resolves the log offset
  nearest `now - dedup_window_ms` via a local timestamp index lookup
  (`consensus::timequery`, the same mechanism Kafka's ListOffsets uses) and
  starts recovery there instead of at offset 0. Recovery after a restart is
  therefore bounded to approximately one dedup window, matching the
  live-partition case above, rather than a topic's full retained history.
  `timequery()`'s result can land in the middle of a multi-record batch
  (`storage::batch_timequery` walks into a batch for the first record at or
  after the target timestamp), but `state_machine_manager`'s apply loop
  requires `next()` to sit exactly on a batch's base offset -- an unaligned
  `next()` reads the containing batch, sees its base offset below `next()`,
  and permanently skips it (`batch_applicator::apply_to_stm()`), stalling
  the STM. The resolved offset is rounded down to the nearest indexed batch
  base offset (`log::index_batch_base_offset_lower_bound()`) before being
  handed to `set_next()`, falling back to the log's start offset -- always a
  valid boundary -- when nothing is indexed yet.
  Like any timestamp-based offset lookup, this trusts client-supplied
  `CreateTime` to be roughly monotonic with offset; out-of-order timestamps
  near the cutoff can shift the resolved start offset by a similar margin
  (see B5), which the existing eviction sweep in `do_apply()`'s replay
  absorbs the same way it absorbs window-edge slop during normal operation.
  **Known gap:** the bounded replay window applies the *current* generation
  and key header uniformly to every batch it replays (`do_apply()` reads
  `ntp_config` fresh per call, not per historical offset). If the identity
  source changed within the replayed window -- key to header, or one header
  to another -- a restart can re-index pre-change records under the
  post-change identity source, which is exactly what the `dedup_generation`
  bump on that config change was meant to prevent (see B4). The log carries
  no per-offset record of which generation/header was in effect when a
  batch was originally written, so closing this fully would mean persisting
  that boundary somewhere queryable at replay time -- a real design change,
  not a small fix. Bounded to one dedup window's worth of possible
  mis-indexing, consistent with B4's own framing, but not eliminated by it.
- **B4** — A `dedup_generation` bump clears the index on each replica as its
  config propagates, not atomically at a log offset. Replicas converge within
  config propagation delay; residual divergence is again window-bounded.
- **B5** — The eviction watermark (`_max_ts`, the highest record timestamp
  observed) is a client-supplied `CreateTime` value with no ordering
  guarantee. A single anomalously-future-timestamped record — clock skew, a
  misbehaving producer, or simple reordering — can advance `_max_ts` and
  therefore the eviction cutoff (`_max_ts - window`) ahead of where it should
  be, evicting entries a subsequent, correctly-ordered duplicate should still
  have matched against. This predates log-derived state (the eviction
  strategy is unchanged) and applies identically on the leader's speculative
  path and the deterministic apply path, since both advance the same
  `_max_ts`. A wall-clock-based watermark would avoid this but was rejected:
  `populate()` runs on every replica, including during log replay at
  arbitrary wall-clock times, so using wall-clock time there would make
  eviction (and therefore index convergence, see B2) depend on replay timing
  rather than being purely a function of the log. Bounded by one dedup
  window, consistent with the feature's overall best-effort framing (point 3
  above).
- **B6** — Increasing `dedup_window_ms` on a live topic does not retroactively
  widen coverage. `topic_table`'s generation-bump logic only bumps
  `dedup_generation` on an enabled→disabled transition or an identity-source
  change (see `topic_table.cc`'s `was_enabled`/`is_enabled`/
  `identity_source_changed` matrix); a positive-to-larger-positive window
  change bumps neither. Entries already evicted under the old, narrower
  window are gone from `_map` and are not reconstructed, so duplicates that
  would fall inside the *new*, larger window are admitted (not caught) until
  a restart replays roughly the new window's worth of history (see B3) and
  rebuilds fuller coverage. Bumping the generation on every window change
  isn't a fix by itself -- `adopt_config()`'s generation-changed path only
  `clear()`s the index, which would throw away entries the *old*, narrower
  window had already caught, making coverage strictly worse until the next
  restart. A real fix needs the same kind of on-demand catch-up replay this
  PR's B3 fix does at startup, but triggered by a live config change instead
  of STM (re)instantiation -- a genuine feature addition, not a small patch.
- **B7** — Enabling dedup changes which STM mediates the write path for
  *all* plain (non-idempotent, non-transactional) produces on the
  partition: `partition.cc`'s routing sends them to
  `dedup_stm::replicate_in_stages` instead of `rm_stm::do_replicate` (the
  path they take when dedup is off, or when they're transactional/
  idempotent). This bypasses `rm_stm`'s `_state_lock` read-lock and gate
  unit, which fence plain writes while `rm_stm` is resetting producer state
  or applying a Raft snapshot. Whether this is safe depends on `rm_stm`
  invariants that this RFC's design didn't originally account for;
  wrapping `dedup_stm`'s replicate call in `rm_stm`'s read lock (or
  restructuring the routing so dedup-filtered batches still pass through
  `rm_stm`) needs verification against `rm_stm`'s locking model before
  changing it, since a wrong fix here risks a deadlock or a fencing hole
  that's harder to detect than the status quo.

# Design

## Follower/apply path

`do_apply(batch)`:

1. Return early unless `batch.header().type == raft_data`, the batch is not a
   control batch, and the partition's `ntp_config` currently has
   `dedup_window_ms` set. Topics without dedup: one branch per batch, nothing
   else.
2. If `ntp_config.dedup_generation() != _generation`: `clear()` and adopt the
   new generation.
3. `set_window(config window)`.
4. Decompress if compressed; for each record with a key:
   `populate(key, first_timestamp + timestamp_delta)`.

`populate()` gains eviction accounting (increments the insert counter and
triggers the existing opportunistic `evict_expired()` sweep) so follower maps
stay bounded without a separate mechanism.

The STM manager already reads every committed batch once and dispatches it to
all registered STMs (`rm_stm` consumes `raft_data` batches this way), so this
adds record iteration + decompression per replica but **no additional log
read**.

## Leader/produce path

`replicate_in_stages(batch, opts, window, generation)` →
`do_replicate` (no mutex, no speculative copy — one map):

1. Hold the gate; `sync(_sync_timeout())` — on becoming leader this waits
   until the committed prefix is applied, i.e. the map reflects the log.
2. Adopt generation and window as in apply.
3. `auto res = _state.filter_request(std::move(batch))` — classification and
   insertion are synchronous (single reactor task), so concurrent produces
   serialize naturally per shard with no lock. `res` carries the filtered
   batch (or `nullopt` if everything was a duplicate) and this request's undo
   list: `{key, applied_timestamp, previous_timestamp}` per admitted key.
4. If the request observed any duplicate, wait on the append-order fence
   (`_append_tail`, G4) with the sync timeout, so everything below is ordered
   after the appends that introduced the observed keys.
5. **All-duplicate**: resolve `enqueued` immediately. If
   `opts.consistency == quorum_ack`, capture `fence = dirty_offset()` (which
   now covers the introducing appends) and wait on
   `visible_offset_monitor().wait(fence, now + sync_timeout, as)` (G5); on
   timeout return `errc::timeout`. Ack with the committed offset and
   `replicated_record_count = 0`.
6. Otherwise `_raft->replicate_in_stages(std::move(*res.batch), opts)` — the
   data batch alone; there is no metadata batch to keep ordered, so Raft
   batching and pipelining across produce requests work exactly as for
   non-dedup topics. Install a fresh `_append_tail` promise and resolve it
   when `request_enqueued` completes (success or failure), so later
   duplicate-observers never hang.
7. On enqueue or replication failure: `_state.revert_request(res.undo)` and
   return the error. **No step-down** — the map is repaired locally, and
   log-derived state means the next leader is correct by construction.
8. On success: ack with `replicated_record_count = admitted` when records
   were dropped (feeding the existing `produce.cc` base-offset correction).

`revert_request` is compare-and-revert: an entry is restored/erased only if
the map still holds the timestamp this request wrote. If a subsequent request
has already overwritten the key, the newer state wins and the entry is left
alone. Eviction sweeps are never reverted (evicted entries were expired by
definition and cannot influence future decisions). `_max_ts` is not reverted
(monotonic advance only sharpens eviction; harmless).

## Snapshots

- `take_local_snapshot`: `evict_expired()`, then serialize
  `{generation, window_ms, max_timestamp, inserts_since_evict, entries}` at
  `last_applied_offset()`. O(active keys). No history of any kind.
- `apply_local_snapshot`: restore.
- `take_raft_snapshot(target)`: `evict_expired()`, serialize current state
  (convergent; see above).
- `apply_raft_snapshot`: clear, restore if non-empty.

The branch is unmerged, so the checkpoint/undo local-snapshot formats
(v0/v1) have no deployed readers and are simply removed; the new
`local_snapshot` restarts at serde version 0 with only the state payload.

## Recovery

`get_initial_recovery_policy()` returns `read_everything` when the partition's
config has dedup enabled at STM start and `skip_to_end` otherwise. First boot
of a dedup-enabled partition replays the log through `do_apply` (bounded
thereafter by local snapshot cadence). Follow-up (not in this change): seek
the initial replay to the timequery offset of `max_ts − window`, since older
entries are expired by definition.

# File-by-file changes

## Deleted concepts (net ≈ −2,000 lines against the current branch)

| File | Removal |
| --- | --- |
| `dedup_stm.h/.cc` | `state_update` (v0/v1/v2), `wire_mutation`, `state_update_kind`, checkpoint begin/chunk/end records, `checkpoint_assembly`, `snapshot_at_offset`, `undo_record`, `wire_undo_entry`, `replay_consumer`, `reconstruct_at`, `best_base_for`, `migrate_legacy_snapshot`, `_replay_base`, `_latest_checkpoint`, `_snapshot_cache`, `_speculative_state/_generation/_term`, `_inflight_tail`, `_enqueue_mutex`, `make_state_update_batch`, `make_checkpoint_batches`, checkpoint thresholds |
| `dedup_window_filter.h/.cc` | `dedup_index_mutation`, `dedup_index_undo`, `filter_with_updates`, `apply`, `apply_no_undo`, `apply_forward`, `apply_forward_no_undo`, `revert`, `undo_entry_map`, mutation/undo plumbing in `is_duplicate`/`maybe_evict`/`evict_expired` |
| `model/record_batch_types.h` | `dedup_state_update` batch type and its `offset_translator_batch_types()` registration (nothing dedup-related remains in the log format) |
| `cluster/tests/dedup_stm_test.cc` | checkpoint/undo/mutation tests replaced by log-derived STM tests |

## Kept unchanged

- Topic property plumbing: `dedup_window_ms` tristate + `dedup_generation` in
  `ntp_config`, `topic_properties`, `topic_configuration`, `topic_table`,
  `incremental_topic_updates` (v12), kafka config handlers, and their tests.
- `raft::replicate_result::replicated_record_count`,
  `cluster::kafka_result::replicated_record_count`, and the `produce.cc` /
  `replicated_partition.cc` base-offset handling. (Needed by any design that
  drops records before replication.)
- `partition.cc` dedup routing (plain produces only; the STM reads window and
  generation from `ntp_config` itself) and `partition_manager.cc` factory
  registration.
- `cluster/snapshot.h` kvstore snapshot key.

## Rewritten

- `dedup_window_filter.h/.cc` — public surface becomes:
  `filter(batch)`, `filter_request(batch) -> dedup_filter_result{batch?,
  undo}`, `revert_request(undo)`, `populate(key, ts)` (now with eviction
  accounting), `evict_expired()`, `snapshot()`, `restore()`, `clear()`,
  `set_window()`, accessors. `dedup_request_undo::entry = {key,
  applied_timestamp, previous_timestamp}`.
- `dedup_stm.h/.cc` — thin STM as specified above; serde types reduce to
  `wire_entry` and `state_snapshot`/`local_snapshot`.
- `cluster/tests/dedup_window_filter_test.cc` — drop mutation/undo apply
  tests; keep and extend filter tests; add `filter_request`/`revert_request`
  coverage (including compare-and-revert overlap cases).
- `cluster/tests/dedup_stm_test.cc` — new coverage, see below.

# Test plan

Unit (`dedup_window_filter_test.cc`):

- Existing filter semantics tests retained (first-wins, window expiry,
  compression preservation, null keys, rebuilt-batch metadata).
- `filter_request` returns correct undo for new keys, re-admitted keys, and
  mixed batches.
- `revert_request` restores previous timestamps, erases new keys, and leaves
  keys overwritten by a later request untouched (compare-and-revert).
- `populate` triggers eviction sweeps across the insert threshold.

STM (`dedup_stm_test.cc`, single-node raft fixture as today):

- Data batches applied through Raft populate the index; duplicates dropped on
  a second produce; window expiry admits again.
- All-duplicate produce acks with committed offset and
  `replicated_record_count = 0`; with `quorum_ack` the ack happens only after
  the fence offset commits.
- Replication-failure path reverts this request's entries (term is stepped
  down externally in the test).
- Local snapshot round-trip: snapshot, restart STM, state equal to pre-restart
  evicted state; apply resumes from snapshot offset.
- Raft snapshot: serialize, clear, install, replay tail → converged state.
- Generation bump clears state.
- Topics without dedup: `do_apply` leaves no state; recovery policy is
  `skip_to_end`.

# Performance expectations

- Leader produce path: unchanged vs. non-dedup topics except the synchronous
  filter pass (decompress + hash ops, ~100–200 ns/record). No serialization
  point: the current branch's mutex capped plain produce at one in-flight
  batch per partition per Raft commit RTT; this design has no such ceiling.
- Replication traffic: no metadata amplification (the current branch adds
  ~tens of bytes per admitted record, permanently uncompactable); dropped
  duplicates reduce traffic.
- Apply path: each replica pays decompress + record parse + map insert,
  ~3–7% of a core per 100k records/s of dedup-enabled ingest. This is the
  price of the design and is proportional to ingest on dedup topics only.
- Snapshots: O(active keys), same as before.
- The dominant cost at scale remains the index cardinality itself (window ×
  unique-key rate), which is inherent to the feature and unchanged by either
  design; bounding it (map size cap, eviction ring) is follow-up work.

# Commit sequence

1. `docs: plan log-derived dedup state` (this document; mark the previous RFC
   superseded).
2. `cluster: derive dedup index from data batches` — the rewrite: filter
   simplification, thin STM, batch-type removal, test replacement. Kept as a
   single commit because the intermediate states (metadata batches with no
   writer, checkpoint reader with no checkpoints) are not independently
   testable or shippable.

# Alternatives considered

Analyzed in the superseded RFC (in git history): bounded reverse history,
full-log replay per snapshot, kvstore history, per-key compaction, checkpoint
protocol. The checkpoint design remains the correct follow-up **if**
byte-exact historical state ever becomes a requirement (e.g. strict
learner-recovery semantics); nothing in this design forecloses it.
