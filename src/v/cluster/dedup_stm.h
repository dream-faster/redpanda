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
#include "metrics/metrics.h"
#include "model/fundamental.h"
#include "raft/persisted_stm.h"
#include "serde/envelope.h"
#include "serde/rw/vector.h"
#include "utils/available_promise.h"

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
class dedup_stm
  : public raft::persisted_stm<raft::kvstore_backed_stm_snapshot> {
public:
    static constexpr std::string_view name = "dedup_stm";

    dedup_stm(
      raft::consensus*,
      ss::logger&,
      storage::kvstore&,
      config::binding<std::chrono::milliseconds> sync_timeout,
      size_t max_entries);

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
    ss::future<> do_apply(const model::record_batch&) final;
    ss::future<> apply_raft_snapshot(const iobuf&) final;

    ss::future<raft::local_snapshot_applied>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&&) final;

    ss::future<raft::stm_snapshot>
    take_local_snapshot(ssx::semaphore_units apply_units) final;

private:
    friend struct dedup_stm_test_accessor;

    /// The identity digest is written as its two halves rather than as a
    /// nested envelope, which would add a second serde header per entry.
    ///
    /// Version 1 replaced the full identity bytes with the digest. The
    /// compat version moves with it: a version 0 snapshot cannot be
    /// re-derived into digests without the identities it no longer carries.
    /// Discarding such a snapshot is safe -- the index is advisory and
    /// log-derived, so it rebuilds on replay -- and apply_local_snapshot()
    /// does exactly that, reporting local_snapshot_applied::no so the
    /// rebuild actually happens, rather than failing to start.
    ///
    /// That tolerance only runs forward, and only matters for clusters
    /// running an in-flight build of this branch: dedup is unreleased, so no
    /// released version can hold a version 0 snapshot. Downgrading past this
    /// commit is the direction that is not safe -- an older binary reading a
    /// version 1 snapshot throws out of its own apply_local_snapshot(),
    /// which persisted_stm does not guard, and the STM fails to start.
    /// Removing the local snapshot lets such a node rebuild from the log.
    struct wire_entry
      : serde::
          envelope<wire_entry, serde::version<1>, serde::compat_version<1>> {
        uint64_t identity_hi{0};
        uint64_t identity_lo{0};
        model::timestamp timestamp;

        auto serde_fields() {
            return std::tie(identity_hi, identity_lo, timestamp);
        }
    };

    struct state_snapshot
      : serde::envelope<
          state_snapshot,
          serde::version<1>,
          serde::compat_version<1>> {
        int64_t window_ms{0};
        int64_t generation{0};
        model::timestamp max_timestamp{model::timestamp::min()};
        uint64_t inserts_since_evict{0};
        chunked_vector<wire_entry> entries;

        auto serde_fields() {
            return std::tie(
              window_ms,
              generation,
              max_timestamp,
              inserts_since_evict,
              entries);
        }
    };

    ss::future<result<kafka_result>> do_replicate(
      model::record_batch,
      raft::replicate_options,
      ss::lw_shared_ptr<available_promise<>>);

    static state_snapshot
    to_snapshot(const dedup_window_filter&, int64_t generation);
    static void restore_snapshot(
      dedup_window_filter&, int64_t& generation, const state_snapshot&);
    /// Deserialize and install a snapshot, or reset to an empty index if the
    /// buffer cannot be read. Returns whether the snapshot was applied.
    bool try_restore_snapshot(iobuf);

    void setup_metrics();

    void adopt_config(
      std::chrono::milliseconds window,
      int64_t generation,
      const std::optional<ss::sstring>& key_header);
    kafka::offset from_log_offset(model::offset) const;

    config::binding<std::chrono::milliseconds> _sync_timeout;
    dedup_window_filter _state;
    int64_t _generation{0};
    // Append-order fence: resolves once the most recently admitted request's
    // batch has been appended to the leader log. A request that observed a
    // duplicate waits on it before proceeding, so its own (possibly empty)
    // replication is ordered after the append that introduced its keys and
    // the Raft prefix property makes its acknowledgment safe.
    // Resolved with true once the fenced request's batch is durably
    // enqueued to the local log, false if that enqueue failed -- so a
    // waiter fenced on it can tell "safe to proceed" apart from "the
    // introducing write never landed" instead of treating both as success.
    ss::lw_shared_ptr<ss::shared_promise<bool>> _append_tail;
    metrics::internal_metric_groups _metrics;
};

class dedup_stm_factory : public state_machine_factory {
public:
    dedup_stm_factory(
      storage::kvstore&,
      config::binding<std::chrono::milliseconds> sync_timeout,
      size_t max_entries);

    bool is_applicable_for(const storage::ntp_config&) const final;

    void create(
      raft::state_machine_manager_builder&,
      raft::consensus*,
      const cluster::stm_instance_config&) final;

private:
    storage::kvstore& _kvstore;
    config::binding<std::chrono::milliseconds> _sync_timeout;
    size_t _max_entries;
};

} // namespace cluster
