// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/dedup_stm.h"

#include "cluster/logger.h"
#include "cluster/snapshot.h"
#include "model/namespace.h"
#include "model/record_batch_types.h"
#include "raft/consensus.h"
#include "serde/rw/bytes.h"
#include "serde/rw/vector.h"
#include "storage/record_batch_builder.h"

#include <seastar/coroutine/as_future.hh>

namespace cluster {

dedup_stm::dedup_stm(
  raft::consensus* raft,
  ss::logger& logger,
  storage::kvstore& kvstore,
  config::binding<std::chrono::milliseconds> sync_timeout)
  : raft::persisted_stm<raft::kvstore_backed_stm_snapshot>(
      dedup_stm_snapshot, logger, raft, kvstore)
  , _sync_timeout(std::move(sync_timeout)) {}

chunked_vector<dedup_index_entry>
dedup_stm::from_wire(const chunked_vector<wire_entry>& entries) {
    chunked_vector<dedup_index_entry> result;
    result.reserve(entries.size());
    for (const auto& entry : entries) {
        result.push_back({entry.key, entry.timestamp});
    }
    return result;
}

chunked_vector<dedup_stm::wire_entry>
dedup_stm::to_wire(const chunked_vector<dedup_index_entry>& entries) {
    chunked_vector<wire_entry> result;
    result.reserve(entries.size());
    for (const auto& entry : entries) {
        result.push_back({.key = entry.key, .timestamp = entry.timestamp});
    }
    return result;
}

dedup_stm::state_snapshot
dedup_stm::to_snapshot(const dedup_window_filter& state, int64_t generation) {
    auto snapshot = state.snapshot();
    return {
      .window_ms = state.window().count(),
      .generation = generation,
      .max_timestamp = snapshot.max_timestamp,
      .inserts_since_evict = static_cast<uint64_t>(
        snapshot.inserts_since_evict),
      .entries = to_wire(snapshot.entries)};
}

dedup_stm::state_snapshot
dedup_stm::copy_snapshot(const state_snapshot& snapshot) {
    state_snapshot result{
      .window_ms = snapshot.window_ms,
      .generation = snapshot.generation,
      .max_timestamp = snapshot.max_timestamp,
      .inserts_since_evict = snapshot.inserts_since_evict};
    result.entries.reserve(snapshot.entries.size());
    for (const auto& entry : snapshot.entries) {
        result.entries.push_back(
          {.key = entry.key, .timestamp = entry.timestamp});
    }
    return result;
}

dedup_stm::undo_record dedup_stm::copy_undo(const undo_record& undo) {
    undo_record result{
      .offset = undo.offset,
      .previous_window_ms = undo.previous_window_ms,
      .previous_max_timestamp = undo.previous_max_timestamp,
      .previous_inserts_since_evict = undo.previous_inserts_since_evict};
    result.entries.reserve(undo.entries.size());
    for (const auto& entry : undo.entries) {
        result.entries.push_back(
          {.key = entry.key, .previous_timestamp = entry.previous_timestamp});
    }
    if (undo.reset_state) {
        result.reset_state = copy_snapshot(*undo.reset_state);
    }
    return result;
}

void dedup_stm::restore_snapshot(
  dedup_window_filter& state,
  int64_t& generation,
  const state_snapshot& snapshot) {
    state.set_window(std::chrono::milliseconds{snapshot.window_ms});
    state.restore(
      dedup_index_snapshot{
        .entries = from_wire(snapshot.entries),
        .max_timestamp = snapshot.max_timestamp,
        .inserts_since_evict = static_cast<size_t>(
          snapshot.inserts_since_evict)});
    generation = snapshot.generation;
}

dedup_stm::undo_record
dedup_stm::to_wire(model::offset offset, dedup_index_undo undo) {
    undo_record result{
      .offset = offset,
      .previous_window_ms = undo.previous_window.count(),
      .previous_max_timestamp = undo.previous_max_timestamp,
      .previous_inserts_since_evict = static_cast<uint64_t>(
        undo.previous_inserts_since_evict)};
    result.entries.reserve(undo.entries.size());
    for (auto& entry : undo.entries) {
        result.entries.push_back(
          {.key = std::move(entry.key),
           .previous_timestamp = entry.previous_timestamp});
    }
    return result;
}

dedup_index_undo dedup_stm::from_wire(const undo_record& undo) {
    dedup_index_undo result{
      .previous_window = std::chrono::milliseconds{undo.previous_window_ms},
      .previous_max_timestamp = undo.previous_max_timestamp,
      .previous_inserts_since_evict = static_cast<size_t>(
        undo.previous_inserts_since_evict)};
    result.entries.reserve(undo.entries.size());
    for (const auto& entry : undo.entries) {
        result.entries.push_back(
          {.key = entry.key, .previous_timestamp = entry.previous_timestamp});
    }
    return result;
}

void dedup_stm::apply_update(const state_update& update, model::offset offset) {
    std::optional<state_snapshot> reset_state;
    if (_generation != update.generation) {
        reset_state = to_snapshot(_state, _generation);
        _state.clear();
        _generation = update.generation;
    }
    auto undo = _state.apply(
      from_wire(update.admitted), std::chrono::milliseconds{update.window_ms});
    auto record = to_wire(offset, std::move(undo));
    record.reset_state = std::move(reset_state);
    _undo_history.push_back(std::move(record));
}

ss::future<> dedup_stm::do_apply(const model::record_batch& batch) {
    if (batch.header().type != model::record_batch_type::dedup_state_update) {
        co_return;
    }

    batch.for_each_record(
      [this, offset = batch.last_offset()](model::record r) {
          auto update = serde::from_iobuf<state_update>(r.release_value());
          apply_update(update, offset);
      });
}

model::record_batch dedup_stm::make_state_update_batch(
  state_update update, model::timestamp timestamp) {
    storage::record_batch_builder builder(
      model::record_batch_type::dedup_state_update, model::offset{0});
    builder.set_timestamp(timestamp);
    builder.add_raw_kv(std::nullopt, serde::to_iobuf(std::move(update)));
    return std::move(builder).build();
}

kafka::offset dedup_stm::from_log_offset(model::offset offset) const {
    return kafka::offset(_raft->log()->from_log_offset(offset));
}

kafka_stages dedup_stm::replicate_in_stages(
  model::record_batch batch,
  raft::replicate_options opts,
  std::chrono::milliseconds window,
  int64_t generation) {
    auto enqueued = ss::make_lw_shared<available_promise<>>();
    auto enqueued_f = enqueued->get_future();
    auto finished = do_replicate(
                      std::move(batch), opts, window, generation, enqueued)
                      .finally([enqueued] {
                          if (!enqueued->available()) {
                              enqueued->set_value();
                          }
                      });
    return {std::move(enqueued_f), std::move(finished)};
}

ss::future<result<kafka_result>> dedup_stm::do_replicate(
  model::record_batch batch,
  raft::replicate_options opts,
  std::chrono::milliseconds window,
  int64_t generation,
  ss::lw_shared_ptr<available_promise<>> enqueued) {
    auto gate_holder = _gate.hold();
    auto units = co_await _enqueue_mutex.get_units();

    if (!co_await sync(_sync_timeout())) {
        co_return errc::not_leader;
    }
    if (!opts.expected_term) {
        opts.expected_term = _insync_term;
    }
    if (_speculative_term != _insync_term) {
        _speculative_state.restore(_state.snapshot());
        _speculative_state.set_window(_state.window());
        _speculative_generation = _generation;
        _speculative_term = _insync_term;
    }
    if (_speculative_generation != generation) {
        _speculative_state.clear();
        _speculative_generation = generation;
    }
    _speculative_state.set_window(window);

    const auto original_count = batch.record_count();
    auto filtered = _speculative_state.filter_with_updates(std::move(batch));
    if (!filtered.batch) {
        enqueued->set_value();
        auto committed = from_log_offset(_raft->committed_offset());
        co_return kafka_result{committed, _raft->term(), 0};
    }

    const auto admitted_count = filtered.batch->record_count();
    chunked_vector<model::record_batch> batches;
    if (!filtered.admitted.empty()) {
        batches.push_back(make_state_update_batch(
          state_update{
            .window_ms = window.count(),
            .generation = generation,
            .admitted = to_wire(filtered.admitted)},
          filtered.batch->header().first_timestamp));
    }
    batches.push_back(std::move(*filtered.batch));

    auto stages = _raft->replicate_in_stages(std::move(batches), opts);
    auto enqueued_result = co_await ss::coroutine::as_future(
      std::move(stages.request_enqueued));
    if (enqueued_result.failed()) {
        if (_raft->is_leader() && _raft->term() == opts.expected_term.value()) {
            co_await _raft->step_down("dedup_stm enqueue failure");
        }
        co_return errc::replication_error;
    }

    enqueued->set_value();

    // Keep the enqueue mutex until replication finishes. In particular, a
    // following all-duplicate request must not be acknowledged from
    // speculative state until the request that introduced the key is durable.
    auto replicated = co_await ss::coroutine::as_future(
      std::move(stages.replicate_finished));
    if (replicated.failed()) {
        if (_raft->is_leader() && _raft->term() == opts.expected_term.value()) {
            co_await _raft->step_down("dedup_stm replication failure");
        }
        co_return errc::replication_error;
    }
    auto result = replicated.get();
    if (!result) {
        if (_raft->is_leader() && _raft->term() == opts.expected_term.value()) {
            co_await _raft->step_down("dedup_stm replication failure");
        }
        co_return result.error();
    }

    units.return_all();
    kafka_result response{
      .last_offset = from_log_offset(result.value().last_offset),
      .last_term = result.value().last_term};
    if (admitted_count != original_count) {
        response.replicated_record_count = admitted_count;
    }
    co_return response;
}

ss::future<iobuf>
dedup_stm::take_raft_snapshot(model::offset last_included_offset) {
    dedup_window_filter historical_state{std::chrono::milliseconds{0}};
    int64_t historical_generation = 0;
    restore_snapshot(
      historical_state,
      historical_generation,
      to_snapshot(_state, _generation));
    for (auto it = _undo_history.rbegin(); it != _undo_history.rend(); ++it) {
        if (it->offset <= last_included_offset) {
            break;
        }
        if (it->reset_state) {
            restore_snapshot(
              historical_state, historical_generation, *it->reset_state);
        } else {
            historical_state.revert(from_wire(*it));
        }
    }
    co_return serde::to_iobuf(
      to_snapshot(historical_state, historical_generation));
}

ss::future<> dedup_stm::apply_raft_snapshot(const iobuf& buffer) {
    _state.clear();
    _generation = 0;
    _undo_history.clear();
    if (!buffer.empty()) {
        auto snapshot = serde::from_iobuf<state_snapshot>(buffer.copy());
        restore_snapshot(_state, _generation, snapshot);
    }
    _speculative_term = model::term_id{-1};
    co_return;
}

ss::future<raft::local_snapshot_applied>
dedup_stm::apply_local_snapshot(raft::stm_snapshot_header, iobuf&& buffer) {
    auto snapshot = serde::from_iobuf<local_snapshot>(std::move(buffer));
    restore_snapshot(_state, _generation, snapshot.state);
    _undo_history = std::move(snapshot.undo_history);
    _speculative_term = model::term_id{-1};
    co_return raft::local_snapshot_applied::yes;
}

ss::future<raft::stm_snapshot>
dedup_stm::take_local_snapshot(ssx::semaphore_units apply_units) {
    const auto start_offset = _raft->start_offset();
    chunked_vector<undo_record> retained_history;
    retained_history.reserve(_undo_history.size());
    for (auto& undo : _undo_history) {
        if (undo.offset >= start_offset) {
            retained_history.push_back(std::move(undo));
        }
    }
    _undo_history = std::move(retained_history);
    chunked_vector<undo_record> snapshot_history;
    snapshot_history.reserve(_undo_history.size());
    for (const auto& undo : _undo_history) {
        snapshot_history.push_back(copy_undo(undo));
    }
    auto snapshot = local_snapshot{
      .state = to_snapshot(_state, _generation),
      .undo_history = std::move(snapshot_history)};
    auto offset = last_applied_offset();
    apply_units.return_all();
    co_return raft::stm_snapshot::create(
      0, offset, serde::to_iobuf(std::move(snapshot)));
}

dedup_stm_factory::dedup_stm_factory(
  storage::kvstore& kvstore,
  config::binding<std::chrono::milliseconds> sync_timeout)
  : _kvstore(kvstore)
  , _sync_timeout(std::move(sync_timeout)) {}

bool dedup_stm_factory::is_applicable_for(
  const storage::ntp_config& cfg) const {
    return model::is_user_topic(cfg.ntp());
}

void dedup_stm_factory::create(
  raft::state_machine_manager_builder& builder,
  raft::consensus* raft,
  const cluster::stm_instance_config&) {
    auto stm = builder.create_stm<dedup_stm>(
      raft, clusterlog, _kvstore, _sync_timeout);
    raft->log()->stm_hookset()->add_stm(stm);
}

} // namespace cluster
