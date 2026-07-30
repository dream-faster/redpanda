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
#include "hashing/crc32c.h"
#include "model/namespace.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "raft/consensus.h"
#include "serde/rw/bytes.h"
#include "serde/rw/vector.h"
#include "storage/record_batch_builder.h"
#include "storage/types.h"

#include <seastar/coroutine/as_future.hh>

#include <stdexcept>
#include <vector>

namespace cluster {

dedup_stm::dedup_stm(
  raft::consensus* raft,
  ss::logger& logger,
  storage::kvstore& kvstore,
  config::binding<std::chrono::milliseconds> sync_timeout)
  : raft::persisted_stm<raft::kvstore_backed_stm_snapshot>(
      dedup_stm_snapshot, logger, raft, kvstore)
  , _sync_timeout(std::move(sync_timeout)) {
    if (raft->start_offset() == model::offset{0}) {
        _replay_base = snapshot_at_offset{
          .offset = model::prev_offset(raft->start_offset()),
          .state = to_snapshot(_state, _generation)};
    }
}

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

chunked_vector<dedup_index_mutation>
dedup_stm::from_wire(const chunked_vector<wire_mutation>& mutations) {
    chunked_vector<dedup_index_mutation> result;
    result.reserve(mutations.size());
    for (const auto& mutation : mutations) {
        result.push_back({mutation.key, mutation.timestamp});
    }
    return result;
}

chunked_vector<dedup_stm::wire_mutation>
dedup_stm::to_wire(const chunked_vector<dedup_index_mutation>& mutations) {
    chunked_vector<wire_mutation> result;
    result.reserve(mutations.size());
    for (const auto& mutation : mutations) {
        result.push_back(
          {.key = mutation.key, .timestamp = mutation.timestamp});
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

dedup_stm::snapshot_at_offset
dedup_stm::copy_snapshot_at(const snapshot_at_offset& snapshot) {
    return {.offset = snapshot.offset, .state = copy_snapshot(snapshot.state)};
}

uint32_t dedup_stm::snapshot_checksum(const state_snapshot& snapshot) {
    auto encoded = serde::to_iobuf(copy_snapshot(snapshot));
    crc::crc32c checksum;
    crc_extend_iobuf(checksum, encoded);
    return checksum.value();
}

std::pair<size_t, size_t>
dedup_stm::mutation_usage(const state_update& update) {
    size_t bytes = 0;
    if (update.has_forward_mutations) {
        for (const auto& mutation : update.mutations) {
            bytes += mutation.key.size() + sizeof(model::timestamp);
        }
        return {update.mutations.size(), bytes};
    }
    for (const auto& entry : update.admitted) {
        bytes += entry.key.size() + sizeof(entry.timestamp);
    }
    return {update.admitted.size(), bytes};
}

void dedup_stm::restore_snapshot(
  dedup_window_filter& state,
  int64_t& generation,
  const state_snapshot& snapshot) {
    state.set_window(std::chrono::milliseconds{snapshot.window_ms});
    state.restore(dedup_index_snapshot{
      .entries = from_wire(snapshot.entries),
      .max_timestamp = snapshot.max_timestamp,
      .inserts_since_evict = static_cast<size_t>(
        snapshot.inserts_since_evict)});
    generation = snapshot.generation;
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

void dedup_stm::apply_mutation(
  dedup_window_filter& state, int64_t& generation, const state_update& update) {
    if (update.reset || generation != update.generation) {
        state.clear();
    }
    generation = update.generation;
    if (update.has_forward_mutations) {
        state.apply_forward_no_undo(
          from_wire(update.mutations),
          std::chrono::milliseconds{update.window_ms},
          update.resulting_max_timestamp,
          static_cast<size_t>(update.resulting_inserts_since_evict));
    } else {
        state.apply_no_undo(
          from_wire(update.admitted),
          std::chrono::milliseconds{update.window_ms});
    }
}

void dedup_stm::apply_checkpoint_record(
  const state_update& update, model::offset offset) {
    switch (update.kind) {
    case state_update_kind::checkpoint_begin: {
        checkpoint_assembly assembly{
          .id = update.checkpoint_id,
          .state = state_snapshot{
            .window_ms = update.window_ms,
            .generation = update.generation,
            .max_timestamp = update.resulting_max_timestamp,
            .inserts_since_evict = update.resulting_inserts_since_evict},
          .expected_chunks = update.checkpoint_chunk_count,
          .expected_entries = update.checkpoint_entry_count,
          .expected_checksum = update.checkpoint_checksum};
        if (assembly.expected_entries <= std::numeric_limits<size_t>::max()) {
            assembly.state.entries.reserve(
              static_cast<size_t>(assembly.expected_entries));
        }
        _checkpoint_assembly = std::move(assembly);
        return;
    }
    case state_update_kind::checkpoint_chunk:
        if (
          !_checkpoint_assembly
          || _checkpoint_assembly->id != update.checkpoint_id
          || _checkpoint_assembly->next_sequence != update.checkpoint_sequence
          || update.checkpoint_sequence
               >= _checkpoint_assembly->expected_chunks) {
            vlog(
              _log.warn,
              "discarding malformed dedup checkpoint chunk {} for {}",
              update.checkpoint_sequence,
              update.checkpoint_id);
            _checkpoint_assembly.reset();
            return;
        }
        for (const auto& entry : update.checkpoint_entries) {
            _checkpoint_assembly->state.entries.push_back(
              {.key = entry.key, .timestamp = entry.timestamp});
        }
        ++_checkpoint_assembly->next_sequence;
        if (
          _checkpoint_assembly->state.entries.size()
          > _checkpoint_assembly->expected_entries) {
            vlog(
              _log.warn,
              "discarding oversized dedup checkpoint {}",
              update.checkpoint_id);
            _checkpoint_assembly.reset();
        }
        return;
    case state_update_kind::checkpoint_end:
        if (
          !_checkpoint_assembly
          || _checkpoint_assembly->id != update.checkpoint_id
          || _checkpoint_assembly->next_sequence
               != _checkpoint_assembly->expected_chunks
          || _checkpoint_assembly->expected_chunks
               != update.checkpoint_chunk_count
          || _checkpoint_assembly->state.entries.size()
               != _checkpoint_assembly->expected_entries
          || _checkpoint_assembly->expected_entries
               != update.checkpoint_entry_count
          || _checkpoint_assembly->expected_checksum
               != update.checkpoint_checksum
          || snapshot_checksum(_checkpoint_assembly->state)
               != _checkpoint_assembly->expected_checksum) {
            vlog(
              _log.warn,
              "discarding incomplete or corrupt dedup checkpoint {}",
              update.checkpoint_id);
            _checkpoint_assembly.reset();
            return;
        }
        _latest_checkpoint = snapshot_at_offset{
          .offset = offset, .state = std::move(_checkpoint_assembly->state)};
        _mutations_since_checkpoint = 0;
        _mutation_bytes_since_checkpoint = 0;
        _checkpoint_assembly.reset();
        return;
    case state_update_kind::mutation:
        return;
    }
    vlog(
      _log.warn,
      "ignoring dedup state record with unknown kind {}",
      static_cast<int8_t>(update.kind));
}

void dedup_stm::apply_update(const state_update& update, model::offset offset) {
    if (update.kind == state_update_kind::mutation) {
        apply_mutation(_state, _generation, update);
        const auto [count, bytes] = mutation_usage(update);
        _mutations_since_checkpoint += count;
        _mutation_bytes_since_checkpoint += bytes;
    } else {
        apply_checkpoint_record(update, offset);
    }
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

chunked_vector<model::record_batch> dedup_stm::make_checkpoint_batches(
  state_snapshot snapshot, model::timestamp timestamp) {
    const auto checkpoint_id = uuid_t::create();
    const auto checksum = snapshot_checksum(snapshot);
    const auto entry_count = snapshot.entries.size();

    std::vector<chunked_vector<wire_entry>> chunks;
    chunked_vector<wire_entry> chunk;
    size_t chunk_bytes = 0;
    for (auto& entry : snapshot.entries) {
        const auto entry_bytes = entry.key.size() + sizeof(entry.timestamp);
        if (
          !chunk.empty()
          && chunk_bytes + entry_bytes > checkpoint_chunk_bytes) {
            chunks.push_back(std::move(chunk));
            chunk = {};
            chunk_bytes = 0;
        }
        chunk_bytes += entry_bytes;
        chunk.push_back(std::move(entry));
    }
    if (!chunk.empty()) {
        chunks.push_back(std::move(chunk));
    }

    chunked_vector<model::record_batch> batches;
    batches.reserve(chunks.size() + 2);
    batches.push_back(make_state_update_batch(
      state_update{
        .window_ms = snapshot.window_ms,
        .generation = snapshot.generation,
        .resulting_max_timestamp = snapshot.max_timestamp,
        .resulting_inserts_since_evict = snapshot.inserts_since_evict,
        .kind = state_update_kind::checkpoint_begin,
        .checkpoint_id = checkpoint_id,
        .checkpoint_chunk_count = static_cast<uint32_t>(chunks.size()),
        .checkpoint_entry_count = static_cast<uint64_t>(entry_count),
        .checkpoint_checksum = checksum},
      timestamp));
    for (size_t sequence = 0; sequence < chunks.size(); ++sequence) {
        batches.push_back(make_state_update_batch(
          state_update{
            .window_ms = snapshot.window_ms,
            .generation = snapshot.generation,
            .kind = state_update_kind::checkpoint_chunk,
            .checkpoint_id = checkpoint_id,
            .checkpoint_sequence = static_cast<uint32_t>(sequence),
            .checkpoint_entries = std::move(chunks[sequence])},
          timestamp));
    }
    batches.push_back(make_state_update_batch(
      state_update{
        .window_ms = snapshot.window_ms,
        .generation = snapshot.generation,
        .kind = state_update_kind::checkpoint_end,
        .checkpoint_id = checkpoint_id,
        .checkpoint_chunk_count = static_cast<uint32_t>(chunks.size()),
        .checkpoint_entry_count = static_cast<uint64_t>(entry_count),
        .checkpoint_checksum = checksum},
      timestamp));
    return batches;
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
    const bool reset = _speculative_generation != generation;
    if (_speculative_generation != generation) {
        _speculative_state.clear();
        _speculative_generation = generation;
    }
    _speculative_state.set_window(window);

    const auto original_count = batch.record_count();
    auto filtered = _speculative_state.filter_with_updates(std::move(batch));
    if (!filtered.batch) {
        auto dependency = _inflight_tail;
        const auto expected_term = opts.expected_term.value();
        units.return_all();
        enqueued->set_value();
        if (dependency) {
            auto dependency_result
              = co_await dependency->finished.get_shared_future();
            if (!dependency_result) {
                co_return errc::replication_error;
            }
            // A quorum duplicate must not be acknowledged solely because the
            // request that introduced its key used a weaker acknowledgment.
            // Waiting for STM application also implies Raft commitment.
            if (
              opts.consistency == raft::consistency_level::quorum_ack
              && dependency->consistency
                   != raft::consistency_level::quorum_ack) {
                try {
                    co_await wait(
                      *dependency_result,
                      model::timeout_clock::now() + _sync_timeout(),
                      opts.as);
                } catch (...) {
                    co_return errc::replication_error;
                }
            }
            if (
              !_raft->is_leader() || _raft->term() != expected_term
              || _raft->committed_offset() < *dependency_result) {
                co_return errc::not_leader;
            }
        }
        auto committed = from_log_offset(_raft->committed_offset());
        co_return kafka_result{committed, _raft->term(), 0};
    }

    const auto admitted_count = filtered.batch->record_count();
    const auto timestamp = filtered.batch->header().first_timestamp;
    chunked_vector<model::record_batch> batches;
    bool checkpoint = false;
    if (!filtered.admitted.empty()) {
        state_update update{
          .window_ms = window.count(),
          .generation = generation,
          .admitted = to_wire(filtered.admitted),
          .has_forward_mutations = true,
          .reset = reset,
          .mutations = to_wire(filtered.mutations),
          .resulting_max_timestamp = filtered.max_timestamp,
          .resulting_inserts_since_evict = static_cast<uint64_t>(
            filtered.inserts_since_evict)};
        const auto [mutation_count, mutation_bytes] = mutation_usage(update);
        checkpoint = reset
                     || _mutations_since_checkpoint + mutation_count
                          >= checkpoint_after_mutations
                     || _mutation_bytes_since_checkpoint + mutation_bytes
                          >= checkpoint_after_bytes;
        batches.push_back(
          make_state_update_batch(std::move(update), timestamp));
    }
    batches.push_back(std::move(*filtered.batch));
    if (checkpoint) {
        auto checkpoint_batches = make_checkpoint_batches(
          to_snapshot(_speculative_state, _speculative_generation), timestamp);
        for (auto& checkpoint_batch : checkpoint_batches) {
            batches.push_back(std::move(checkpoint_batch));
        }
    }

    auto stages = _raft->replicate_in_stages(std::move(batches), opts);
    auto enqueued_result = co_await ss::coroutine::as_future(
      std::move(stages.request_enqueued));
    if (enqueued_result.failed()) {
        if (_raft->is_leader() && _raft->term() == opts.expected_term.value()) {
            co_await _raft->step_down("dedup_stm enqueue failure");
        }
        co_return errc::replication_error;
    }

    auto inflight = ss::make_lw_shared<inflight_request>();
    inflight->consistency = opts.consistency;
    inflight->term = opts.expected_term.value();
    _inflight_tail = inflight;
    enqueued->set_value();
    // Classification and Raft enqueue remain ordered, but completion waits do
    // not hold the mutex: independent producers can now enter the Raft
    // batcher concurrently. All-duplicate requests wait on the dependency
    // fence captured above instead.
    units.return_all();
    auto replicated = co_await ss::coroutine::as_future(
      std::move(stages.replicate_finished));
    if (replicated.failed()) {
        inflight->finished.set_value(std::nullopt);
        if (_raft->is_leader() && _raft->term() == opts.expected_term.value()) {
            co_await _raft->step_down("dedup_stm replication failure");
        }
        co_return errc::replication_error;
    }
    auto result = replicated.get();
    if (!result) {
        inflight->finished.set_value(std::nullopt);
        if (_raft->is_leader() && _raft->term() == opts.expected_term.value()) {
            co_await _raft->step_down("dedup_stm replication failure");
        }
        co_return result.error();
    }

    inflight->finished.set_value(result.value().last_offset);
    kafka_result response{
      .last_offset = from_log_offset(result.value().last_offset),
      .last_term = result.value().last_term};
    if (admitted_count != original_count) {
        response.replicated_record_count = admitted_count;
    }
    co_return response;
}

dedup_stm::replay_consumer::replay_consumer(
  dedup_window_filter& state, int64_t& generation)
  : _state(state)
  , _generation(generation) {}

ss::future<ss::stop_iteration>
dedup_stm::replay_consumer::operator()(model::record_batch& batch) {
    if (batch.header().type != model::record_batch_type::dedup_state_update) {
        co_return ss::stop_iteration::no;
    }
    batch.for_each_record([this](model::record record) {
        auto update = serde::from_iobuf<state_update>(record.release_value());
        if (update.kind == state_update_kind::mutation) {
            apply_mutation(_state, _generation, update);
        }
    });
    co_return ss::stop_iteration::no;
}

const dedup_stm::snapshot_at_offset*
dedup_stm::best_base_for(model::offset target) const {
    const snapshot_at_offset* best = nullptr;
    const auto consider =
      [target, &best](const std::optional<snapshot_at_offset>& candidate) {
          if (
            candidate && candidate->offset <= target
            && (!best || candidate->offset > best->offset)) {
              best = &*candidate;
          }
      };
    consider(_replay_base);
    consider(_latest_checkpoint);
    consider(_snapshot_cache);
    return best;
}

ss::future<dedup_stm::state_snapshot>
dedup_stm::reconstruct_at(model::offset target) {
    if (target == last_applied_offset()) {
        co_return to_snapshot(_state, _generation);
    }

    const auto* base = best_base_for(target);
    if (!base) {
        throw std::runtime_error(fmt::format(
          "dedup state at offset {} is not reconstructible: no checkpoint at "
          "or before the target",
          target));
    }

    dedup_window_filter state{std::chrono::milliseconds{0}};
    int64_t generation = 0;
    restore_snapshot(state, generation, base->state);
    const auto replay_start = model::next_offset(base->offset);
    if (replay_start <= target) {
        if (replay_start < _raft->start_offset()) {
            throw std::runtime_error(fmt::format(
              "dedup state at offset {} is not reconstructible: base {} "
              "precedes Raft start offset {}",
              target,
              base->offset,
              _raft->start_offset()));
        }
        storage::local_log_reader_config config(replay_start, target);
        config.type_filter = model::record_batch_type::dedup_state_update;
        config.skip_batch_cache = true;
        auto reader = co_await _raft->make_reader(std::move(config));
        co_await std::move(reader).for_each_ref(
          replay_consumer{state, generation}, model::no_timeout);
    }
    co_return to_snapshot(state, generation);
}

dedup_stm::snapshot_at_offset dedup_stm::migrate_legacy_snapshot(
  const state_snapshot& current,
  const chunked_vector<undo_record>& undo_history,
  model::offset target) const {
    dedup_window_filter state{std::chrono::milliseconds{0}};
    int64_t generation = 0;
    restore_snapshot(state, generation, current);
    for (auto it = undo_history.rbegin(); it != undo_history.rend(); ++it) {
        if (it->offset <= target) {
            break;
        }
        if (it->reset_state) {
            restore_snapshot(state, generation, *it->reset_state);
        } else {
            state.revert(from_wire(*it));
        }
    }
    return {.offset = target, .state = to_snapshot(state, generation)};
}

ss::future<iobuf>
dedup_stm::take_raft_snapshot(model::offset last_included_offset) {
    auto snapshot = co_await reconstruct_at(last_included_offset);
    _snapshot_cache = snapshot_at_offset{
      .offset = last_included_offset, .state = copy_snapshot(snapshot)};
    co_return serde::to_iobuf(std::move(snapshot));
}

ss::future<> dedup_stm::apply_raft_snapshot(const iobuf& buffer) {
    _state.clear();
    _generation = 0;
    _latest_checkpoint.reset();
    _snapshot_cache.reset();
    _checkpoint_assembly.reset();
    _mutations_since_checkpoint = 0;
    _mutation_bytes_since_checkpoint = 0;
    state_snapshot snapshot;
    if (!buffer.empty()) {
        snapshot = serde::from_iobuf<state_snapshot>(buffer.copy());
        restore_snapshot(_state, _generation, snapshot);
    } else {
        snapshot = to_snapshot(_state, _generation);
    }
    _replay_base = snapshot_at_offset{
      .offset = model::prev_offset(_raft->start_offset()),
      .state = std::move(snapshot)};
    _speculative_term = model::term_id{-1};
    co_return;
}

ss::future<raft::local_snapshot_applied> dedup_stm::apply_local_snapshot(
  raft::stm_snapshot_header header, iobuf&& buffer) {
    auto snapshot = serde::from_iobuf<local_snapshot>(std::move(buffer));
    restore_snapshot(_state, _generation, snapshot.state);
    _replay_base = std::move(snapshot.replay_base);
    _latest_checkpoint = std::move(snapshot.latest_checkpoint);
    _snapshot_cache = std::move(snapshot.snapshot_cache);
    _mutations_since_checkpoint = static_cast<size_t>(
      snapshot.mutations_since_checkpoint);
    _mutation_bytes_since_checkpoint = static_cast<size_t>(
      snapshot.mutation_bytes_since_checkpoint);

    if (!_replay_base && !snapshot.undo_history.empty()) {
        _replay_base = migrate_legacy_snapshot(
          snapshot.state,
          snapshot.undo_history,
          model::prev_offset(_raft->start_offset()));
    }
    if (!_snapshot_cache) {
        _snapshot_cache = snapshot_at_offset{
          .offset = header.offset, .state = copy_snapshot(snapshot.state)};
    }
    if (!_replay_base && _raft->start_offset() == model::offset{0}) {
        dedup_window_filter empty{std::chrono::milliseconds{0}};
        _replay_base = snapshot_at_offset{
          .offset = model::prev_offset(_raft->start_offset()),
          .state = to_snapshot(empty, 0)};
    }
    _checkpoint_assembly.reset();
    _speculative_term = model::term_id{-1};
    co_return raft::local_snapshot_applied::yes;
}

ss::future<raft::stm_snapshot>
dedup_stm::take_local_snapshot(ssx::semaphore_units apply_units) {
    local_snapshot snapshot{
      .state = to_snapshot(_state, _generation),
      .replay_base = _replay_base
                       ? std::optional(copy_snapshot_at(*_replay_base))
                       : std::nullopt,
      .latest_checkpoint = _latest_checkpoint ? std::optional(copy_snapshot_at(
                                                  *_latest_checkpoint))
                                              : std::nullopt,
      .snapshot_cache = _snapshot_cache
                          ? std::optional(copy_snapshot_at(*_snapshot_cache))
                          : std::nullopt,
      .mutations_since_checkpoint = static_cast<uint64_t>(
        _mutations_since_checkpoint),
      .mutation_bytes_since_checkpoint = static_cast<uint64_t>(
        _mutation_bytes_since_checkpoint)};
    auto offset = last_applied_offset();
    apply_units.return_all();
    co_return raft::stm_snapshot::create(
      1, offset, serde::to_iobuf(std::move(snapshot)));
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
