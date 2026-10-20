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

Guarantees:

- **G1** — On a stable leader, a keyed record is dropped iff a record with the
  same key and a timestamp within `dedup_window_ms` was previously admitted.
- **G2** — The index survives restarts, leadership changes, and snapshot
  installation: any replica that becomes leader first syncs to the committed
  log, and the log deterministically implies the index.
- **G3** — Log consistency and Kafka offset translation never depend on
  filter state. There are no dedup batches in the log at all.
- **G4** — Classification order does not imply log order (an introducing
  request inserts into the map before its batch is appended), so the
  identities a request introduces are **pending** -- `_pending` maps each to
  the in-flight request that introduced it -- until raft confirms the batch
  appended. A request that drops a record against a pending identity waits
  for exactly that introducer to resolve: appended, and it proceeds (its own
  batch, if any, is replicated after the introducing append, so the Raft
  prefix property makes its quorum ack cover it); never appended (the
  introducer's entries are reverted), or unresolved within the sync timeout,
  and it fails with a retriable error after reverting its own insertions, so
  the producer's retry is classified fresh. A request that drops nothing
  waits on nobody. A raft timeout leaves a batch's fate open -- the batcher
  may have appended it before the timer fired -- so its identities stay
  pending until this replica has applied past the dirty offset read at that
  point, and whether `do_apply()` saw the batch decides. Snapshots leave
  pending identities out (see Snapshots).
- **G5** — A request that is *entirely* deduplicated (nothing to replicate)
  and requests `quorum_ack` waits, once its introducers are appended, for the committed
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
- **G7** — Each partition indexes at most
  `dedup_max_entries_per_partition` distinct identities (one million by
  default). A sweep cursor advancing a few entries per new identity keeps the
  index under that ceiling, trimming the oldest entries a completed pass found
  from a soft limit a tenth below it. Should the index still reach the hard
  ceiling, one bounded extra step is taken, and if it frees nothing the new
  identity is admitted without being indexed. This fail-open behavior keeps
  produce available and bounds both steady-state index memory and future
  snapshot size even when producer timestamps do not advance or the configured
  window and identity cardinality are unexpectedly large. Changing
  the cluster property requires a broker restart, and that restart is not
  atomic across the cluster -- mid-rollout, replicas of the same partition
  enforce different ceilings, which the property's own description now spells
  out. Because a saturated index degrades dedup coverage silently, every
  record admitted unindexed is counted in the `dedup:partition` metric group's
  `records_unindexed` counter, and the condition is logged at warn once per
  five minutes per shard. Live identities dropped to make room are counted
  separately in `capacity_evicted_entries`: non-zero there means the effective
  window is narrower than the configured one.
  A snapshot carrying more identities than this broker's ceiling is reduced to
  the **most recent** ones, not to a prefix: the index is a dense table
  iterated in insertion order, so keeping a prefix kept the entries closest to
  expiry. `dedup_window_filter::restore()` owns that selection outright --
  `dedup_stm::restore_snapshot()` deliberately does not pre-trim, so the policy
  has one implementation -- and the identities dropped are counted in
  `snapshot_entries_truncated`. A consequence worth having: two replicas with
  different ceilings now retain a prefix relationship (the smaller keeps a
  subset of the larger) rather than arbitrary disjoint subsets.

  The per-partition ceiling does not bound a shard: partitions are placed on
  shards by count, not by index size, so several saturated indices can share
  one shard's memory. `dedup_max_entries_per_shard` (0, the default, derives it
  from 15% of shard memory at ~52 bytes an entry) caps the sum. Every index on
  the shard reports its size into one shared `dedup_shard_budget`; the same
  cursor trims a partition that inserts while the shard is within a tenth of
  its limit, and at the limit a new identity with nothing freeable in its own
  partition is admitted unindexed. Before anything live is trimmed, expired
  entries elsewhere go: from a fifth below the limit -- before trimming starts
  at a tenth -- every new identity on the shard also advances another index's
  cursor, round-robin, by a step that drops only expired entries
  (`expired_entries_reclaimed`). Started only at the trim margin, it stopped
  as soon as the shard was back under it, so expired entries were never
  drained and held the shard at the margin, where a step that found nothing
  let the largest index trim a live entry. A cursor
  moves only on its own index's inserts, so without this an index takes a
  sizeable fraction of its window per pass, and the shard trimmed live
  entries from the largest index while its neighbours held more expired ones
  than it was short. That step judges expiry by the index's own watermark
  moved on by however far the shard's newest record has advanced since the
  index last saw one, so an index whose partition went quiet still ages and
  does not hold its share of the shard until traffic returns, while one whose
  producers merely stamp records behind its neighbours' keeps its entries as
  long as they keep arriving. The shard total is exported as the
  `dedup:shard` gauges `index_entries` and `max_index_entries`. Effective
  capacity now depends on which partitions share a shard, which differs per
  replica, so under shard pressure replicas trim differently -- the same
  best-effort divergence capacity eviction already has.

- **G8** — The two ways the filter can stop deduplicating without failing a
  request -- entry-limit saturation (G7) and producer timestamp skew (B5) --
  are both counted per partition, alongside the drop count that makes them
  readable. The `dedup:partition` metric group carries `records_dropped`,
  `records_skew_clamped`, `records_unindexed`, `capacity_evicted_entries`,
  `expired_entries_reclaimed`,
  `snapshot_entries_truncated` and `requests_rebuilt` counters plus an
  `index_entries` gauge, labelled by namespace/topic/partition like
  `tx:partition`. Internal metrics only; per-partition cardinality is too high
  for the public endpoint.

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
  guarantee, so record timestamps are clamped to the broker's clock
  (`clamp_to_broker_time()`) before they reach the index.
  **This paragraph previously claimed the unclamped damage was "bounded by one
  dedup window". That was wrong, and the gap was reachable with entirely legal
  traffic.** Redpanda admits records up to `log_message_timestamp_after_max_ms`
  ahead of the broker -- one hour by default, under the default `relaxed`
  validation mode -- and `_max_ts` is a running maximum, so one such record
  pushed the cutoff (`_max_ts - window`) an hour past every entry in the index.
  The next sweep emptied the index, and every entry inserted afterwards was
  already below the cutoff and died at the following sweep: dedup stayed off
  for the partition, since `_max_ts` only moves forward and `clear()` is
  reachable only via a `dedup_generation` bump. `restore()` took
  `snapshot.max_timestamp` unvalidated, so the poisoned watermark also survived
  restarts. Unbounded in both time and entry count, not window-bounded.
  The clamp closes this without reintroducing the replay-timing dependence that
  a wall-clock *watermark* would have. That earlier rejection was of setting
  `_max_ts = now()`, which would make eviction depend on when a replica happens
  to replay. A clamp is a different construction: `min(ts, now)` is the
  identity for any record at or behind the broker's clock, which is every
  record seen during historical replay, so `populate()`'s behavior there is
  unchanged and the index stays a deterministic function of the log. Only
  records ahead of the broker's clock are affected, and those are anomalous by
  definition. Max-wins idempotency (B2) is preserved because the clamp is a
  per-record function. Leader/follower divergence is bounded by the clock
  difference between classification and apply -- milliseconds, well inside the
  window-edge slop B2 and B4 already accept.
  The clamp applies to the stored entry timestamp as well as the watermark, not
  only the latter: clamping the watermark alone would leave the mirror-image
  bug, where systemic forward skew stores every entry above the cutoff,
  eviction never fires, and the index runs to the G7 ceiling instead.
  `restore()` clamps too, so a partition already carrying a poisoned snapshot
  heals on restart. Clamped records are counted in
  the `dedup:partition` group's `records_skew_clamped` counter (see G8); a
  non-zero value means producer clock skew is reaching the index.
  What the clamp does *not* fix is reordering *within* the window: timestamps
  behind the watermark are untouched, so window-edge eviction remains
  approximate, consistent with the feature's best-effort framing (point 3
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

- **B8** — Capacity eviction (G7) makes eviction timing decision-affecting.
  The expired sweep only ever removed entries that could never match again, so
  replicas sweeping at different points were indistinguishable. Dropping the
  oldest *live* entries is different: a replica that evicts earlier forgets
  identities another replica still remembers, so the two can answer the same
  duplicate differently, including after a failover to an index built purely
  by `populate()` replay. Both paths run the identical cursor at the identical
  point relative to their insert, so a replica that saw the same identity
  sequence reaches the same contents (see B10 for the one exception). It also
  removes the larger part of the `_inserts_since_evict` divergence it would
  otherwise have amplified:
  that double count occurred when the leader's `is_duplicate()` bumped the
  counter for a record it then failed open on, and at capacity the leader now
  makes room and indexes the identity instead, so the follower's `populate()`
  finds it present and does not bump again.

- **B9** — A request with *some* duplicates is the feature's only expensive
  case: the batch is rebuilt from the surviving records and recompressed with
  the producer's codec, which is work stock Redpanda never does on the produce
  path. `requests_rebuilt` (G8) is the rate, and it is the number to watch
  when sizing a cluster for this feature. For zstd that recompression also
  used to allocate a fresh multi-megabyte *contiguous* workspace per batch,
  because `compression::stream_zstd` created a `ZSTD_CCtx` per call; on a
  fragmented shard heap that allocation fails and aborts the reactor while the
  shard still reports most of its memory free. Compression now runs on a
  static per-shard workspace allocated at startup and sized for zstd's
  worst-case parameters, so zstd neither grows it for a large batch nor shrinks
  it after a run of small ones (`ZSTD_WORKSPACETOOLARGE_*`) -- a reused
  heap-owned context still did both, at a low but steady rate. This matches
  what the same class already did for decompression and what
  `async_stream_zstd` does for both. Rebuilding
  uncompressed instead was considered and rejected: it would inflate those
  batches on disk and on the wire, and let them past the `batch_max_bytes`
  check, which the produce handler applies to the compressed request before
  dedup ever sees it.

- **B10** — Bounding the sweep makes the index's dense-array *layout*, and
  not only its contents, part of what replicas have to agree on. The cursor is a
  position in that array; the array is laid out by the order identities were
  inserted and erased, and the map erases by moving its last entry into the
  freed slot. Replicas applying the same identity sequence lay it out
  identically, so they sweep identically -- that is the test. The exception is
  `revert_request()`: it erases entries a follower never inserted, which
  permutes the leader's array, and the two then sweep set-equal maps in
  different orders. The consequence is a different choice of *which* live entry
  capacity trimming drops, which needs the index above its soft limit and is
  already best-effort under B8, but it is a divergence the whole-index passes
  this replaced did not have. It is inherent to the bound rather than a
  shortcoming of this particular cursor: any scheme doing O(1) work per record
  carries a position in some layout.

  The bound is what the correlated-stall failure mode demands. Sweeps fire at
  the same log position on every replica, so a full pass over a multi-million
  entry index stalled the leader's reactor and then every follower's roughly
  half a second later, leaving nobody free to complete an `acks=all` quorum.
  Local snapshotting still copies and scans the whole index, but each replica
  snapshots on its own schedule, so that cost is not correlated across the
  quorum.

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
4. On the leader, skip the batch if this STM filtered it: `filter_request()`
   already indexed its identities with these timestamps, and `populate()` is
   max-wins, so applying it again would only cost the parse. The leader
   records the batch's crc and record count before handing it to Raft (the
   crc covers the records, attributes, timestamps and count, not the base
   offset Raft assigns), and `do_apply()` consumes that record. It is dropped
   when the request fails and its entries are reverted, cleared with the
   index on a generation change, and bounded, so a batch Raft never reports
   on ages out and is simply indexed again. A crc collision between two
   batches in flight on one partition leaves the other's identities
   unindexed on this replica alone: a 2^-32 chance of a bounded coverage
   loss of the kind the index already tolerates.
5. Decompress if compressed; `for_each_dedup_identity()` walks the records
   reading only what dedup needs -- the key, or in header mode the first
   occurrence of the configured header, digested straight from the batch's
   buffer, and the timestamp -- and skips the rest of each record by its
   length. Nothing is copied or shared per record. For each identity:
   `populate(digest, first_timestamp + timestamp_delta)` (a saturating add:
   the delta is client-controlled). `populate()` never lets the index exceed
   its hard per-partition entry limit; when a bounded step cannot free a
   slot, the identity remains unindexed.
6. A batch whose records cannot be read -- the walk or the decompression
   throws -- is skipped and counted (`batches_skipped`), with a rate-limited
   warning. The STM manager treats an exception from apply as transient and
   retries the batch forever, which would also hold up every raft snapshot
   on the partition and with it the log's prefix truncation; the index is
   advisory, and an unreadable batch is one the leader could not have
   filtered either. `std::bad_alloc` is the exception: it propagates.

`populate()` advances the same sweep cursor the leader's classification path
does, at the same point relative to its insert, so follower maps stay bounded
by the same mechanism and reach the same contents (see B10). With the leader
skipping its own batches, each replica now sweeps once per record, where the
leader used to sweep twice.

The STM manager already reads every committed batch once and dispatches it to
all registered STMs (`rm_stm` consumes `raft_data` batches this way), so this
adds the identity walk + decompression per replica but **no additional log
read**.

## Leader/produce path

`replicate_in_stages(batch, opts, window, generation)` →
`do_replicate` (no mutex, no speculative copy — one map):

1. Hold the gate; `sync(_sync_timeout())` — on becoming leader this waits
   until the committed prefix is applied, i.e. the map reflects the log.
2. Adopt generation and window as in apply.
3. `auto res = _state.filter_request(std::move(batch))` — classification and
   insertion are synchronous (single reactor task), so concurrent produces
   serialize naturally per shard with no lock. The identities come from the
   same `for_each_dedup_identity()` walk the apply path uses; what survives
   the pass is 32 bytes per record, not the records. `res` carries the
   filtered batch (or `nullopt` if everything was a duplicate) and this
   request's undo list: `{key, applied_timestamp, previous_timestamp}` per
   admitted key.
4. Look the dropped identities up in `_pending` (G4) and, if anything was
   admitted, register this request's insertions there under a fresh
   `inflight_request` -- before the first suspension, so a request classified
   behind this one finds them. Then wait on each distinct introducer with the
   sync timeout: one that never appended fails this request with
   `replication_error`, one that does not resolve in time with `timeout`,
   either after reverting its own insertions.
5. **All-duplicate**: resolve `enqueued` immediately. If
   `opts.consistency == quorum_ack`, capture `fence = dirty_offset()` (which
   now covers the introducing appends) and wait on
   `events().wait(fence, now + sync_timeout, as)` (G5); on timeout return
   `errc::timeout`; if the term changed meanwhile return `errc::not_leader`.
   Ack with the committed offset and `replicated_record_count = 0`.
6. Otherwise `_raft->replicate_in_stages(std::move(*res.batch), opts)` — the
   data batch alone; there is no metadata batch to keep ordered, so Raft
   batching and pipelining across produce requests work exactly as for
   non-dedup topics.
7. On enqueue or replication failure, `settle(appended = false)`:
   `_state.revert_request(res.undo)`, forget the self-filtered key, drop the
   request's `_pending` registrations and resolve its promise with `false`,
   so requests waiting on it fail for retry. **No step-down** — the map is
   repaired locally, and log-derived state means the next leader is correct
   by construction. `raft::errc::timeout` is the one result that says
   nothing about the batch: `resolve_after_timeout()` keeps its identities
   pending, waits until this replica has applied past the dirty offset (or
   leadership is lost), and settles by whether `do_apply()` saw the batch --
   its key moved out of the bounded self-filtered set for the duration, so
   load cannot age it out. If the key was already gone and the set did age
   entries out meanwhile, the answer is unknowable and the request settles
   as never appended: the retry may then land beside an appended batch (a
   duplicate, which is what a timed-out produce means to a Kafka client
   anyway) rather than be acknowledged against a write that may not exist.
8. On success: ack with `replicated_record_count = admitted` when records
   were dropped (feeding the existing `produce.cc` base-offset correction).

`revert_request` is compare-and-revert: an entry is restored/erased only if
the map still holds the timestamp this request wrote. If a subsequent request
has already overwritten the key, the newer state wins and the entry is left
alone. Eviction sweeps are never reverted (evicted entries were expired by
definition and cannot influence future decisions). `_max_ts` is not reverted
(monotonic advance only sharpens eviction; harmless).

## Snapshots

Local snapshots are files in the partition directory (`persisted_stm<>`'s
default file backend), not kvstore values. The kvstore holds every value in
memory for as long as it exists and copies all of them each time it rolls a
segment, which a value larger than `kvstore_max_segment_size` forces on nearly
every write; with several saturated indices on a shard, that cost a multiple
of the indices themselves and ran the shard out of memory. `start()` removes
the key an earlier build left in the kvstore; that snapshot is not migrated,
and the index rebuilds from the log within one window.

The payload (`state_snapshot` version 3) carries the entries as one packed blob
of 20-byte records (digest lo, the low 32 bits of digest hi, timestamp;
little-endian), written straight from the index. A snapshot therefore costs one
serialized copy of the live entries. Version 2 snapshots (24-byte records with
all 64 bits of digest hi) are still read, keeping the 96 bits the index uses.
Versions 0 and 1 are refused on compat version, and the index is rebuilt from
the log; a version 2 binary refuses version 3 the same way, rather than
restoring 96-bit digests it would never match against its 128-bit ones.

The index (`dedup_compact_index`) keeps an entry in 14 bytes: an 80-bit digest
and a 32-bit timestamp relative to the base of its block of 4096 entries, in a
dense array in insertion order whose erase moves the last entry into the hole
-- the layout the sweep cursor and the snapshot walk depend on. Positions are
found through a robin-hood table of 4-byte slots (24-bit position, 8-bit probe
distance) split into segments of at most 4096 slots under a directory indexed
by the digest's top bits, so a full segment splits on its own and the table
never rehashes as a whole. That is about 20 bytes an entry against the 50 of
the general-purpose map it replaced, and no call does work proportional to the
index: a segment split, the largest step, re-inserts at most one segment's
keys. A block's timestamps span 2^32 - 2 ms exactly (~49.7 days); a write that
does not fit re-bases the block in one bounded pass, and entries more than
~37 days behind the block's newest are kept as expired. Expired only ever
stops an entry matching, so with a window up to 37 days no decision changes;
beyond that, coverage of the oldest identities can be lost, never turned into
a drop.

Identities introduced by requests still in flight (`_pending`, G4) are left
out of both local and raft snapshots: they are not in the log yet, and a
snapshot holding one would, after a crash, drop the producer's retry of a
record no replica ever wrote. The next snapshot carries them once their
batches have applied.

Raft snapshot installs take the shard's `restore_one_at_a_time` semaphore,
as startup restores do: a node rejoining after a long absence installs one
for every partition it fell behind on.

The write yields. `for_each_live_entry_async()` walks the map's value array in
chunks of 4096 entries with a preemption check between them, while the produce
path keeps mutating the index (the apply lock is held for a local snapshot,
but the leader's `filter_request()` runs without it). Rather than forbid that,
the walk is made exact under it: an insert appends to the array and is
emitted or not, which a convergent snapshot tolerates; an erase moves the
array's last entry into the hole, and when the hole is behind the walk's
cursor that entry would never be reached, so every erase records it for each
walk in progress and the walk emits those at the end. A clear or restore under
the walk invalidates it and the serializer starts over. The restore keeps the
newer timestamp when a snapshot carries an identity twice, which an index that
erased and re-admitted it during the walk can produce. The longest stretch
the snapshot write holds the reactor is therefore one chunk, however large the
index; the one-pass write it replaced held it for hundreds of milliseconds at
the shard limit, on every segment roll.

Startup is where a memory shortfall does the most damage, because a broker
that cannot start cannot shed load either. Three paths were bounded:

- **kvstore recovery** runs before any partition starts, so it would load an
  earlier build's dedup snapshots before `dedup_stm::start()` could remove
  them. The kvstore is configured with `discarded_stm_snapshots =
  {dedup.snapshot}`: recovery drops those keys from the snapshot and the
  replayed segments without retaining them, and rewrites its snapshot so the
  next start never reads them. The snapshot is now restored record by record
  straight from the buffer read off disk, rather than being decoded into a
  batch (a re-encoded second copy) and walked with a copying iterator; the
  batch checksum is accumulated record by record over the same encoding
  (`model::crc_extend_record()`), so integrity checking is unchanged.
- **Snapshot load** is serialized per shard: `persisted_stm::start()` reads
  the whole snapshot file before the restore trims it, and partition starts
  are not otherwise bounded, so `dedup_stm::start()` holds a one-unit
  semaphore in the shard budget around it and at most one snapshot file is in
  memory per shard at a time.
- **Snapshot restore** is bounded by the shard budget's headroom as well as by
  the partition ceiling, keeping the newest identities. It reads the packed
  entries in place, in passes, so it costs at most 8 bytes an entry beyond the
  index (and only when trimming). It measures the capacity cutoff over what it
  restores and inserts the entries below it first, where the sweep cursor
  starts, so a restart into a full shard can trim at once.
- **Log replay** was already bounded: every insert goes through the same
  capacity checks as the produce path.

Under shard pressure the indices within a tenth of the largest one's size trim
themselves; a smaller partition at the shard limit takes one bounded cursor
step on the largest index instead. Without it, partitions that filled the
budget and then went idle -- and so never trim themselves -- would leave a
newly busy partition without coverage for as long as they sat there. When a
bounded step at the shard limit finds nothing below the cutoff, it replaces the
oldest entry just ahead of the cursor if that is older than the incoming
identity, so a pass whose trimmable entries were used up early (by log replay
after a restart, say) keeps indexing new identities instead of admitting them
unindexed until the cursor wraps.

The original design notes follow.


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
config has dedup enabled at STM start and `skip_to_end` otherwise, and
`get_initial_recovery_start_offset()` bounds the first boot of a dedup-enabled
partition to one window: the timequery offset of `now − window`, since older
entries are expired by definition. The STM manager applies that bound only on
the partition's first start and afterwards resumes from the offset it recorded
then, so `dedup_stm::start()` applies the same bound itself whenever no local
snapshot was restored (the file was lost, or discarded on a version break);
otherwise such a restart would replay the whole retained log, with produce
requests waiting on `sync()` meanwhile. The manager clamps with `max`, so the
two agree.

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
- `populate` advances the sweep cursor on every new-identity attempt, and the
  per-call sweep cost is bounded regardless of index size -- including when the
  index is saturated with nothing freeable, which is the case that previously
  rescanned the whole index per record.
- Leader (`is_duplicate`) and follower (`populate`) given the same identity
  sequence end with identical index contents, dense-array layout, and
  counters, including across the entry ceiling.
- The sweep cursor and its histogram reset on `restore()` and `clear()`.

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
  filter pass (decompress if compressed, one identity walk over the batch's
  buffer, a digest and a map insert per record). No serialization point: the
  current branch's mutex capped plain produce at one in-flight batch per
  partition per Raft commit RTT; this design has no such ceiling. The leader
  does not apply its own batches a second time.
- Replication traffic: no metadata amplification (the current branch adds
  ~tens of bytes per admitted record, permanently uncompactable); dropped
  duplicates reduce traffic.
- Apply path: each follower pays decompress + identity walk + map insert.
  This is the price of the design and is proportional to ingest on dedup
  topics only. Compressed batches are the expensive case: the walk cannot
  read identities out of a compressed batch, so every replica decompresses
  every committed batch of a dedup topic.
- Snapshots: O(active keys), 20 bytes an entry, written in chunks with the
  reactor yielded between them.
- Measured: `//src/v/cluster/tests:dedup_window_filter_rpbench` (per-record
  cost of each path, run in the `unit-tests` workflow) and
  `tests/dedup_acceptance/test_bench.py` (end-to-end records/s and broker CPU
  per million records against a topic without dedup; results in
  `tests/dedup_acceptance/BENCHMARK.md`).
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
