// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "base/units.h"
#include "cluster/dedup_window_filter.h"
#include "cluster/state_machine_registry.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "raft/persisted_stm.h"
#include "serde/envelope.h"
#include "serde/rw/bytes.h"
#include "serde/rw/chrono.h"
#include "serde/rw/optional.h"
#include "serde/rw/uuid.h"
#include "serde/rw/vector.h"
#include "utils/available_promise.h"
#include "utils/uuid.h"

#include <seastar/core/shared_future.hh>
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

    struct wire_mutation
      : serde::
          envelope<wire_mutation, serde::version<0>, serde::compat_version<0>> {
        bytes key;
        std::optional<model::timestamp> timestamp;

        auto serde_fields() { return std::tie(key, timestamp); }
    };

    enum class state_update_kind : int8_t {
        mutation = 0,
        checkpoint_begin = 1,
        checkpoint_chunk = 2,
        checkpoint_end = 3,
    };

    struct state_update
      : serde::
          envelope<state_update, serde::version<2>, serde::compat_version<0>> {
        int64_t window_ms{0};
        int64_t generation{0};
        // Retained in v1 so a v0 reader can deterministically apply the update.
        chunked_vector<wire_entry> admitted;
        bool has_forward_mutations{false};
        bool reset{false};
        chunked_vector<wire_mutation> mutations;
        model::timestamp resulting_max_timestamp{model::timestamp::min()};
        uint64_t resulting_inserts_since_evict{0};
        state_update_kind kind{state_update_kind::mutation};
        uuid_t checkpoint_id{};
        uint32_t checkpoint_sequence{0};
        uint32_t checkpoint_chunk_count{0};
        uint64_t checkpoint_entry_count{0};
        uint32_t checkpoint_checksum{0};
        chunked_vector<wire_entry> checkpoint_entries;

        auto serde_fields() {
            return std::tie(
              window_ms,
              generation,
              admitted,
              has_forward_mutations,
              reset,
              mutations,
              resulting_max_timestamp,
              resulting_inserts_since_evict,
              kind,
              checkpoint_id,
              checkpoint_sequence,
              checkpoint_chunk_count,
              checkpoint_entry_count,
              checkpoint_checksum,
              checkpoint_entries);
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

    struct snapshot_at_offset
      : serde::envelope<
          snapshot_at_offset,
          serde::version<0>,
          serde::compat_version<0>> {
        model::offset offset;
        state_snapshot state;

        auto serde_fields() { return std::tie(offset, state); }
    };

    struct local_snapshot
      : serde::envelope<
          local_snapshot,
          serde::version<1>,
          serde::compat_version<0>> {
        state_snapshot state;
        // V0 snapshots stored reverse deltas here. V1 keeps the field empty so
        // old readers remain able to restore the current state.
        chunked_vector<undo_record> undo_history;
        std::optional<snapshot_at_offset> replay_base;
        std::optional<snapshot_at_offset> latest_checkpoint;
        std::optional<snapshot_at_offset> snapshot_cache;
        uint64_t mutations_since_checkpoint{0};
        uint64_t mutation_bytes_since_checkpoint{0};

        auto serde_fields() {
            return std::tie(
              state,
              undo_history,
              replay_base,
              latest_checkpoint,
              snapshot_cache,
              mutations_since_checkpoint,
              mutation_bytes_since_checkpoint);
        }
    };

    struct checkpoint_assembly {
        uuid_t id;
        state_snapshot state;
        uint32_t expected_chunks{0};
        uint64_t expected_entries{0};
        uint32_t expected_checksum{0};
        uint32_t next_sequence{0};
    };

    // A conservative dependency fence for requests classified from the
    // speculative state. It is global for now: that may wait for an unrelated
    // preceding request, but it never lets a duplicate acknowledge before the
    // Raft request that made the speculative decision durable.
    struct inflight_request {
        ss::shared_promise<std::optional<model::offset>> finished;
        raft::consistency_level consistency{raft::consistency_level::no_ack};
        model::term_id term{model::term_id{-1}};
    };

    class replay_consumer {
    public:
        replay_consumer(dedup_window_filter&, int64_t&);

        ss::future<ss::stop_iteration> operator()(model::record_batch&);
        void end_of_stream() {}

    private:
        dedup_window_filter& _state;
        int64_t& _generation;
    };

    ss::future<result<kafka_result>> do_replicate(
      model::record_batch,
      raft::replicate_options,
      std::chrono::milliseconds,
      int64_t,
      ss::lw_shared_ptr<available_promise<>>);

    static model::record_batch
      make_state_update_batch(state_update, model::timestamp);
    static chunked_vector<model::record_batch>
      make_checkpoint_batches(state_snapshot, model::timestamp);
    static state_snapshot
    to_snapshot(const dedup_window_filter&, int64_t generation);
    static state_snapshot copy_snapshot(const state_snapshot&);
    static snapshot_at_offset copy_snapshot_at(const snapshot_at_offset&);
    static uint32_t snapshot_checksum(const state_snapshot&);
    static std::pair<size_t, size_t> mutation_usage(const state_update&);
    static void restore_snapshot(
      dedup_window_filter&, int64_t& generation, const state_snapshot&);
    static dedup_index_undo from_wire(const undo_record&);
    static chunked_vector<dedup_index_entry>
    from_wire(const chunked_vector<wire_entry>&);
    static chunked_vector<wire_entry>
    to_wire(const chunked_vector<dedup_index_entry>&);
    static chunked_vector<dedup_index_mutation>
    from_wire(const chunked_vector<wire_mutation>&);
    static chunked_vector<wire_mutation>
    to_wire(const chunked_vector<dedup_index_mutation>&);

    void apply_update(const state_update&, model::offset);
    static void apply_mutation(
      dedup_window_filter&, int64_t& generation, const state_update&);
    void apply_checkpoint_record(const state_update&, model::offset);
    ss::future<state_snapshot> reconstruct_at(model::offset);
    const snapshot_at_offset* best_base_for(model::offset) const;
    snapshot_at_offset migrate_legacy_snapshot(
      const state_snapshot&,
      const chunked_vector<undo_record>&,
      model::offset target) const;
    kafka::offset from_log_offset(model::offset) const;

    static constexpr size_t checkpoint_after_mutations = 10'000;
    static constexpr size_t checkpoint_after_bytes = 16_MiB;
    static constexpr size_t checkpoint_chunk_bytes = 512_KiB;

    config::binding<std::chrono::milliseconds> _sync_timeout;
    dedup_window_filter _state{std::chrono::milliseconds{0}};
    int64_t _generation{0};
    std::optional<snapshot_at_offset> _replay_base;
    std::optional<snapshot_at_offset> _latest_checkpoint;
    std::optional<snapshot_at_offset> _snapshot_cache;
    std::optional<checkpoint_assembly> _checkpoint_assembly;

    dedup_window_filter _speculative_state{std::chrono::milliseconds{0}};
    int64_t _speculative_generation{0};
    model::term_id _speculative_term{model::term_id{-1}};
    size_t _mutations_since_checkpoint{0};
    size_t _mutation_bytes_since_checkpoint{0};
    ss::lw_shared_ptr<inflight_request> _inflight_tail;
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
