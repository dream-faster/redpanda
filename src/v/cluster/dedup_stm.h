// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cluster/dedup_window_filter.h"
#include "cluster/state_machine_registry.h"
#include "cluster/types.h"
#include "container/chunked_hash_map.h"
#include "metrics/metrics.h"
#include "model/fundamental.h"
#include "raft/persisted_stm.h"
#include "serde/envelope.h"
#include "serde/rw/envelope.h"
#include "serde/rw/iobuf.h"
#include "serde/rw/scalar.h"
#include "utils/available_promise.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>

namespace cluster {

struct dedup_stm_test_accessor;

/// Replicated state machine for write-path, first-wins deduplication.
///
/// The dedup index is a deterministic function of the data batches already in
/// the Raft log: do_apply() reads each committed raft_data batch and records
/// its keyed records in the window filter. Nothing dedup-specific is ever
/// written to the log.
///
/// The index is advisory: only the leader consults it, and only before
/// replication, so index state can never affect log consistency. Snapshots
/// are therefore convergent rather than byte-exact — serializing the current
/// state for an older target offset is safe because populate() is idempotent
/// (max-timestamp-wins), so snapshot-plus-replay converges to the same state.
///
/// Local snapshots are files in the partition directory, not kvstore values.
/// A saturated index snapshots to tens of MiB, and the kvstore holds every
/// value in memory for as long as it exists and copies all of them each time
/// it rolls a segment -- which a value larger than kvstore_max_segment_size
/// forces on nearly every write. On a shard hosting several saturated indices
/// that cost several times the indices themselves.
class dedup_stm : public raft::persisted_stm<> {
public:
    static constexpr std::string_view name = "dedup_stm";
    /// Bytes one index entry occupies in a snapshot: digest lo, the 32 bits
    /// of digest hi the index keeps, and the timestamp as its age below the
    /// snapshot's max_timestamp. See state_snapshot.
    static constexpr size_t packed_entry_size = sizeof(uint64_t)
                                                + 2 * sizeof(uint32_t);
    /// The timestamp in full instead: written when the window is longer than
    /// a 32-bit age spans, and read from a snapshot of an earlier build.
    static constexpr size_t wide_entry_size = 2 * sizeof(uint64_t)
                                              + sizeof(uint32_t);

    dedup_stm(
      raft::consensus*,
      ss::logger&,
      storage::kvstore&,
      config::binding<std::chrono::milliseconds> sync_timeout,
      size_t max_entries,
      ss::lw_shared_ptr<dedup_shard_budget> shard_budget = nullptr);

    /// Filter and replicate one plain produce batch. The dedup window and
    /// generation are read from the partition's ntp_config, the same source
    /// the apply path uses.
    kafka_stages
      replicate_in_stages(model::record_batch, raft::replicate_options);

    size_t map_size() const { return _state.map_size(); }

    /// Partitions without dedup configured skip log recovery entirely; see
    /// get_initial_recovery_start_offset() for the dedup-configured case,
    /// which takes precedence over this policy.
    raft::stm_initial_recovery_policy get_initial_recovery_policy() const final;

    /// Bounds recovery to approximately one dedup window instead of the
    /// whole log: records older than `now - dedup_window_ms` can never
    /// again cause a drop (see dedup_window_filter's eviction comment), so
    /// replaying them just to immediately evict them wastes the read, the
    /// decompression, and -- worse -- can transiently hold a full topic
    /// history in memory while recovery catches up. Looks up the log offset
    /// nearest that cutoff via a local timestamp index query (the same
    /// mechanism Kafka's ListOffsets uses) and starts recovery there.
    /// Returns std::nullopt when dedup isn't configured, deferring to
    /// get_initial_recovery_policy()'s skip_to_end.
    ss::future<std::optional<model::offset>>
    get_initial_recovery_start_offset() final;

    ss::future<iobuf> take_raft_snapshot(model::offset) final;

protected:
    ss::future<> start() override;
    ss::future<> stop() override;

    ss::future<> do_apply(const model::record_batch&) final;
    ss::future<> apply_raft_snapshot(const iobuf&) final;

    ss::future<raft::local_snapshot_applied>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&&) final;

    ss::future<raft::stm_snapshot>
    take_local_snapshot(ssx::semaphore_units apply_units) final;

private:
    friend struct dedup_stm_test_accessor;

    /// Version history, each a compat break: a reader that cannot read a
    /// version discards the snapshot and rebuilds the index from the log (see
    /// try_restore_snapshot()); the index is advisory and log-derived, so that
    /// is correct, only slower. This build reads version 4 only:
    ///
    /// - 0: full identity bytes per entry.
    /// - 1: a 128-bit digest per entry, each its own serde envelope.
    /// - 2: the entries as one packed blob of fixed 24-byte records (digest
    ///   hi, digest lo, timestamp; each little-endian), written in one pass
    ///   straight from the index.
    /// - 3: digests are 96 bits (dedup_canonical_digest()), and an entry packs
    ///   into wide_entry_size bytes: digest lo, the low 32 bits of digest
    ///   hi, timestamp. `entry_size` names the layout.
    /// - 4: the layout of version 3, with digests taken by one XXH3-128 pass
    ///   rather than two seeded xxhash64 passes (dedup_digester). A digest of
    ///   either kind never matches one of the other, so earlier snapshots
    ///   cannot be restored, and this build refuses them on compat version.
    ///   Written with wide_entry_size entries at first, then with
    ///   packed_entry_size ones: an entry's timestamp as a 32-bit age below
    ///   max_timestamp, which covers every live entry of a window up to
    ///   2^32 - 1 ms (49.7 days). A longer window is still written wide.
    ///   Both are read; a build that reads only the first refuses the second
    ///   on entry_size, and rebuilds as above.
    ///
    /// The break runs both ways: an older binary rejects a newer snapshot on
    /// compat version the same way, so a raft snapshot crossing a rolling
    /// upgrade in either direction never fails the partition. It is not
    /// rebuilt from, though: a raft snapshot installs where the log before
    /// its offset is gone, so the identities only it held are lost on that
    /// replica, which indexes from the snapshot offset on. Each replica
    /// digests what it applies itself, so replicas on different builds never
    /// compare digests otherwise. Version 1 snapshots only ever lived in the
    /// kvstore, which this STM no longer reads.
    struct state_snapshot
      : serde::envelope<
          state_snapshot,
          serde::version<4>,
          serde::compat_version<4>> {
        int64_t window_ms{0};
        int64_t generation{0};
        model::timestamp max_timestamp{model::timestamp::min()};
        iobuf entries;
        /// Bytes per record in `entries`: packed_entry_size, or
        /// wide_entry_size for a long window or an earlier build.
        uint32_t entry_size{packed_entry_size};

        auto serde_fields() {
            return std::tie(
              window_ms, generation, max_timestamp, entries, entry_size);
        }
    };

    /// Deserialize a snapshot. Throws serde_exception for one this build
    /// does not read.
    static state_snapshot decode_snapshot(iobuf);

    ss::future<result<kafka_result>> do_replicate(
      model::record_batch,
      raft::replicate_options,
      ss::lw_shared_ptr<available_promise<>>);

    /// A request whose batch this leader admitted but raft has not yet
    /// confirmed appended. The identities it introduced are in the index, so
    /// the requests behind it see them, but not yet in the log. A request
    /// that drops a record against one of them waits for the outcome before
    /// it is acknowledged, and a snapshot leaves them out. `appended` resolves
    /// true once the batch is appended at the request's consistency level,
    /// false once it is known never to have been and its entries are reverted.
    struct inflight_request {
        ss::shared_promise<bool> appended;
    };
    using inflight_ptr = ss::lw_shared_ptr<inflight_request>;
    using pending_identities = chunked_hash_map<
      dedup_identity_digest,
      inflight_ptr,
      dedup_identity_digest_hash>;

    /// Serialize the index's live entries, yielding between chunks. The
    /// produce path mutates the index meanwhile; the filter's walk accounts
    /// for that (dedup_window_filter::for_each_live_entry_async()), and a walk
    /// the index was cleared or replaced under starts over. \p generation is
    /// read once the entries are written, so the two describe the same index.
    /// Identities in \p pending -- introduced by requests still in flight --
    /// are left out: they are not in the log yet, and a snapshot that held one
    /// of them would, after a crash, drop the producer's retry of a record no
    /// replica ever wrote.
    static ss::future<iobuf> serialize_snapshot(
      dedup_window_filter&,
      const int64_t& generation,
      const pending_identities* pending = nullptr);
    /// Install a snapshot, yielding between chunks of its entries: a restore
    /// of millions of entries at startup would otherwise hold the reactor,
    /// and every other partition on the shard waiting to restore behind it.
    static ss::future<>
    restore_snapshot(dedup_window_filter&, int64_t& generation, state_snapshot);
    /// Deserialize and install a snapshot, or reset to an empty index if the
    /// buffer cannot be read. Returns whether the snapshot was applied.
    ss::future<bool> try_restore_snapshot(iobuf);

    void setup_metrics();

    /// Wait until every request in \p introducers is known appended. An
    /// error if one never was, or did not resolve within the sync timeout or
    /// before \p as fired: the identities a request dropped against it may
    /// have no durable copy.
    ss::future<std::optional<errc>> wait_for_introducers(
      const std::vector<inflight_ptr>& introducers,
      std::optional<std::reference_wrapper<ss::abort_source>> as);

    /// Resolve an in-flight request: drop its identities from the pending
    /// set, revert its insertions if the batch was never appended, and
    /// release the requests waiting on it.
    void settle(
      const inflight_ptr&,
      const dedup_request_undo&,
      bool appended,
      std::optional<uint64_t> self_key);
    /// Raft reported a timeout for the request's batch, which says nothing
    /// about whether the batch was appended: the batcher may have flushed it
    /// before the timer fired. Keep its identities pending until the question
    /// settles, then settle() accordingly.
    ss::future<> resolve_after_timeout(
      inflight_ptr,
      dedup_request_undo,
      std::optional<uint64_t> self_key,
      model::term_id,
      uint64_t evictions_before);

    void adopt_config(
      std::chrono::milliseconds window,
      int64_t generation,
      const std::optional<ss::sstring>& key_header);
    kafka::offset from_log_offset(model::offset) const;

    /// Batches this leader filtered and handed to raft, by their record crc
    /// and count, so do_apply() can skip indexing what filter_request()
    /// already indexed; see self_filtered_key(). Bounded: an entry is
    /// removed when its batch applies or its request fails, and a batch that
    /// never does either (a request still in flight at a leadership change,
    /// before raft reports it) ages out.
    static uint64_t self_filtered_key(const model::record_batch_header&);
    void remember_self_filtered(uint64_t key);
    /// Returns whether the key was still remembered.
    bool forget_self_filtered(uint64_t key);
    bool take_self_filtered(uint64_t key);
    static constexpr size_t self_filtered_max = 4096;
    chunked_hash_map<uint64_t, uint32_t> _self_filtered;
    ss::circular_buffer<uint64_t> _self_filtered_order;
    /// Keys aged out of _self_filtered while their batch was still in flight.
    /// A key that is gone was either taken by do_apply() -- its batch applied
    /// -- or aged out; this count tells the two apart.
    uint64_t _self_filtered_evictions{0};
    /// Keys of batches raft timed out on, mapped to whether do_apply() has
    /// seen the batch since. Unlike _self_filtered these never age out, so a
    /// resolution that waits on them cannot be fooled by load; see
    /// resolve_after_timeout(). Bounded by the timed-out requests still
    /// unresolved.
    chunked_hash_map<uint64_t, bool> _resolving;

    /// Only to drop the snapshot an earlier build kept in the kvstore.
    storage::kvstore& _kvstore;
    config::binding<std::chrono::milliseconds> _sync_timeout;
    dedup_window_filter _state;
    int64_t _generation{0};
    /// Identities introduced by requests still in flight on this leader; see
    /// inflight_request. Bounded by the records in flight.
    pending_identities _pending;
    // Requests admitted and not yet settled. While any is, the lookup table
    // stays: settle() may have to revert one (see do_apply()).
    size_t _unsettled{0};
    /// Last offset, in _relaxed_admitted_term, of a batch this leader admitted
    /// and raft reported done before it committed: one with weaker acks than
    /// quorum_ack, or a quorum-ack one under write caching, which raft
    /// reports once a majority appended it, flushed or not. Every other
    /// identity in the index that is not pending is committed, so this is
    /// all an all-duplicate quorum-ack request has to wait to commit; see
    /// do_replicate().
    model::offset _relaxed_admitted_offset{model::offset::min()};
    /// A later term may have truncated what an earlier one admitted, and its
    /// sync committed whatever survived, so the offset above only fences
    /// requests in the term it was recorded in.
    model::term_id _relaxed_admitted_term;
    /// Committed batches do_apply() could not read; see do_apply().
    uint64_t _batches_skipped{0};
    uint64_t _lookup_builds{0};
    // Builds of at least this many entries are logged at info.
    static constexpr size_t lookup_build_log_threshold = 100'000;
    /// Whether start() applied a local snapshot; see start().
    bool _local_snapshot_applied{false};
    metrics::internal_metric_groups _metrics;
};

class dedup_stm_factory : public state_machine_factory {
public:
    /// One factory per shard, so the budget it hands its STMs is the shard's.
    /// max_entries_per_shard of 0 derives the limit from shard memory. Both
    /// limits are watched: a change applies to every index on the shard
    /// without a restart (see dedup_window_filter::set_max_entries()).
    dedup_stm_factory(
      storage::kvstore&,
      config::binding<std::chrono::milliseconds> sync_timeout,
      config::binding<size_t> max_entries,
      config::binding<size_t> max_entries_per_shard);

    bool is_applicable_for(const storage::ntp_config&) const final;

    void create(
      raft::state_machine_manager_builder&,
      raft::consensus*,
      const cluster::stm_instance_config&) final;

private:
    void setup_metrics();
    void apply_partition_limit();
    void apply_shard_limit();

    storage::kvstore& _kvstore;
    config::binding<std::chrono::milliseconds> _sync_timeout;
    config::binding<size_t> _max_entries;
    config::binding<size_t> _max_entries_per_shard;
    ss::lw_shared_ptr<dedup_shard_budget> _shard_budget;
    metrics::internal_metric_groups _metrics;
};

} // namespace cluster
