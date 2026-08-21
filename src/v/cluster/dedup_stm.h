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
#include "model/fundamental.h"
#include "raft/persisted_stm.h"
#include "serde/envelope.h"
#include "serde/rw/bytes.h"
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
      config::binding<std::chrono::milliseconds> sync_timeout);

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

    struct wire_entry
      : serde::
          envelope<wire_entry, serde::version<0>, serde::compat_version<0>> {
        bytes key;
        model::timestamp timestamp;

        auto serde_fields() { return std::tie(key, timestamp); }
    };

    struct state_snapshot
      : serde::envelope<
          state_snapshot,
          serde::version<0>,
          serde::compat_version<0>> {
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

    void adopt_config(
      std::chrono::milliseconds window,
      int64_t generation,
      const std::optional<ss::sstring>& key_header);
    kafka::offset from_log_offset(model::offset) const;

    config::binding<std::chrono::milliseconds> _sync_timeout;
    dedup_window_filter _state{std::chrono::milliseconds{0}};
    int64_t _generation{0};
    // Append-order fence: resolves once the most recently admitted request's
    // batch has been appended to the leader log. A request that observed a
    // duplicate waits on it before proceeding, so its own (possibly empty)
    // replication is ordered after the append that introduced its keys and
    // the Raft prefix property makes its acknowledgment safe.
    ss::lw_shared_ptr<ss::shared_promise<>> _append_tail;
};

class dedup_stm_factory : public state_machine_factory {
public:
    dedup_stm_factory(
      storage::kvstore&,
      config::binding<std::chrono::milliseconds> sync_timeout);

    bool is_applicable_for(const storage::ntp_config&) const final;

    void create(
      raft::state_machine_manager_builder&,
      raft::consensus*,
      const cluster::stm_instance_config&) final;

private:
    storage::kvstore& _kvstore;
    config::binding<std::chrono::milliseconds> _sync_timeout;
};

} // namespace cluster
