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
#include "serde/rw/chrono.h"
#include "serde/rw/optional.h"
#include "serde/rw/vector.h"
#include "utils/available_promise.h"

#include <seastar/core/shared_ptr.hh>

namespace cluster {

struct dedup_stm_test_accessor;

/// Replicated state machine for write-path, first-wins deduplication.
class dedup_stm
  : public raft::persisted_stm<raft::kvstore_backed_stm_snapshot> {
public:
    static constexpr std::string_view name = "dedup_stm";

    dedup_stm(
      raft::consensus*,
      ss::logger&,
      storage::kvstore&,
      config::binding<std::chrono::milliseconds> sync_timeout);

    kafka_stages replicate_in_stages(
      model::record_batch,
      raft::replicate_options,
      std::chrono::milliseconds window,
      int64_t generation);

    size_t map_size() const { return _state.map_size(); }

    raft::stm_initial_recovery_policy
    get_initial_recovery_policy() const final {
        return raft::stm_initial_recovery_policy::read_everything;
    }

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

    struct state_update
      : serde::
          envelope<state_update, serde::version<0>, serde::compat_version<0>> {
        int64_t window_ms{0};
        int64_t generation{0};
        chunked_vector<wire_entry> admitted;

        auto serde_fields() {
            return std::tie(window_ms, generation, admitted);
        }
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

    struct wire_undo_entry
      : serde::envelope<
          wire_undo_entry,
          serde::version<0>,
          serde::compat_version<0>> {
        bytes key;
        std::optional<model::timestamp> previous_timestamp;

        auto serde_fields() { return std::tie(key, previous_timestamp); }
    };

    struct undo_record
      : serde::
          envelope<undo_record, serde::version<0>, serde::compat_version<0>> {
        model::offset offset;
        int64_t previous_window_ms{0};
        model::timestamp previous_max_timestamp{model::timestamp::min()};
        uint64_t previous_inserts_since_evict{0};
        chunked_vector<wire_undo_entry> entries;
        std::optional<state_snapshot> reset_state;

        auto serde_fields() {
            return std::tie(
              offset,
              previous_window_ms,
              previous_max_timestamp,
              previous_inserts_since_evict,
              entries,
              reset_state);
        }
    };

    struct local_snapshot
      : serde::envelope<
          local_snapshot,
          serde::version<0>,
          serde::compat_version<0>> {
        state_snapshot state;
        chunked_vector<undo_record> undo_history;

        auto serde_fields() { return std::tie(state, undo_history); }
    };

    ss::future<result<kafka_result>> do_replicate(
      model::record_batch,
      raft::replicate_options,
      std::chrono::milliseconds,
      int64_t,
      ss::lw_shared_ptr<available_promise<>>);

    static model::record_batch
      make_state_update_batch(state_update, model::timestamp);
    static state_snapshot
    to_snapshot(const dedup_window_filter&, int64_t generation);
    static state_snapshot copy_snapshot(const state_snapshot&);
    static undo_record copy_undo(const undo_record&);
    static void restore_snapshot(
      dedup_window_filter&, int64_t& generation, const state_snapshot&);
    static dedup_index_undo from_wire(const undo_record&);
    static undo_record to_wire(model::offset, dedup_index_undo);
    static chunked_vector<dedup_index_entry>
    from_wire(const chunked_vector<wire_entry>&);
    static chunked_vector<wire_entry>
    to_wire(const chunked_vector<dedup_index_entry>&);

    void apply_update(const state_update&, model::offset);
    kafka::offset from_log_offset(model::offset) const;

    config::binding<std::chrono::milliseconds> _sync_timeout;
    dedup_window_filter _state{std::chrono::milliseconds{0}};
    int64_t _generation{0};
    chunked_vector<undo_record> _undo_history;

    dedup_window_filter _speculative_state{std::chrono::milliseconds{0}};
    int64_t _speculative_generation{0};
    model::term_id _speculative_term{model::term_id{-1}};
    ssx::mutex _enqueue_mutex{"c/dedup_stm::enqueue_mutex"};
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
