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
#include "model/batch_compression.h"
#include "model/namespace.h"
#include "model/record_batch_types.h"
#include "raft/consensus.h"

#include <seastar/core/with_timeout.hh>
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

raft::stm_initial_recovery_policy
dedup_stm::get_initial_recovery_policy() const {
    return _raft->log()->config().dedup_window_ms()
             ? raft::stm_initial_recovery_policy::read_everything
             : raft::stm_initial_recovery_policy::skip_to_end;
}

dedup_stm::state_snapshot
dedup_stm::to_snapshot(const dedup_window_filter& state, int64_t generation) {
    auto snapshot = state.snapshot();
    state_snapshot result{
      .window_ms = state.window().count(),
      .generation = generation,
      .max_timestamp = snapshot.max_timestamp,
      .inserts_since_evict = static_cast<uint64_t>(
        snapshot.inserts_since_evict)};
    result.entries.reserve(snapshot.entries.size());
    for (auto& entry : snapshot.entries) {
        result.entries.push_back(
          {.key = std::move(entry.key), .timestamp = entry.timestamp});
    }
    return result;
}

void dedup_stm::restore_snapshot(
  dedup_window_filter& state,
  int64_t& generation,
  const state_snapshot& snapshot) {
    state.set_window(std::chrono::milliseconds{snapshot.window_ms});
    dedup_index_snapshot restored{
      .max_timestamp = snapshot.max_timestamp,
      .inserts_since_evict = static_cast<size_t>(snapshot.inserts_since_evict)};
    restored.entries.reserve(snapshot.entries.size());
    for (const auto& entry : snapshot.entries) {
        restored.entries.push_back({entry.key, entry.timestamp});
    }
    state.restore(restored);
    generation = snapshot.generation;
}

void dedup_stm::adopt_config(
  std::chrono::milliseconds window, int64_t generation) {
    if (_generation != generation) {
        _state.clear();
        _generation = generation;
    }
    _state.set_window(window);
}

ss::future<> dedup_stm::do_apply(const model::record_batch& batch) {
    if (batch.header().type != model::record_batch_type::raft_data) {
        co_return;
    }
    if (batch.header().attrs.is_control()) {
        co_return;
    }
    const auto& cfg = _raft->log()->config();
    auto window = cfg.dedup_window_ms();
    if (!window) {
        co_return;
    }
    adopt_config(*window, cfg.dedup_generation());

    std::optional<model::record_batch> decompressed;
    if (batch.compressed()) {
        decompressed = model::decompress_batch_sync(batch);
    }
    const model::record_batch& readable = decompressed ? *decompressed : batch;
    const auto base_ts = readable.header().first_timestamp;
    readable.for_each_record([this, base_ts](model::record r) {
        if (r.has_key()) {
            _state.populate(
              r.key(), model::timestamp{base_ts.value() + r.timestamp_delta()});
        }
    });
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

    if (!co_await sync(_sync_timeout())) {
        co_return errc::not_leader;
    }
    if (!opts.expected_term) {
        opts.expected_term = _insync_term;
    }
    adopt_config(window, generation);

    const auto original_count = batch.record_count();
    auto filtered = _state.filter_request(std::move(batch));
    const auto admitted_count = filtered.batch ? filtered.batch->record_count()
                                               : 0;

    if (admitted_count != original_count && _append_tail) {
        // This request observed at least one duplicate. The map entries that
        // caused the drops may belong to requests whose batches are not yet
        // appended to the log, so classification order alone does not imply
        // log order. Waiting for the latest admitted append (a conservative,
        // global fence) orders everything this request does after the appends
        // that introduced its keys.
        auto tail_wait = co_await ss::coroutine::as_future(
          ss::with_timeout(
            model::timeout_clock::now() + _sync_timeout(),
            _append_tail->get_shared_future()));
        if (tail_wait.failed()) {
            auto ex = tail_wait.get_exception();
            vlog(
              clusterlog.debug,
              "{} dedup append fence wait failed: {}",
              _raft->ntp(),
              ex);
            _state.revert_request(filtered.undo);
            co_return errc::timeout;
        }
    }

    if (!filtered.batch) {
        // Every record was a duplicate: nothing to replicate. A quorum-ack
        // request must not acknowledge more strongly than the entries that
        // introduced its keys, so wait for everything appended so far — which
        // now includes those entries — to commit first.
        enqueued->set_value();
        if (opts.consistency == raft::consistency_level::quorum_ack) {
            const auto fence = _raft->dirty_offset();
            auto waited = co_await ss::coroutine::as_future(
              _raft->visible_offset_monitor().wait(
                fence, model::timeout_clock::now() + _sync_timeout(), opts.as));
            if (waited.failed()) {
                auto ex = waited.get_exception();
                vlog(
                  clusterlog.debug,
                  "{} dedup fence wait for {} failed: {}",
                  _raft->ntp(),
                  fence,
                  ex);
                co_return errc::timeout;
            }
        }
        co_return kafka_result{
          .last_offset = from_log_offset(_raft->committed_offset()),
          .last_term = _raft->term(),
          .replicated_record_count = 0};
    }

    auto stages = _raft->replicate_in_stages(std::move(*filtered.batch), opts);
    auto append_done = ss::make_lw_shared<ss::shared_promise<>>();
    _append_tail = append_done;
    auto enqueued_result = co_await ss::coroutine::as_future(
      std::move(stages.request_enqueued));
    // Resolve the fence on failure too so waiters never hang. A waiter that
    // classified against entries reverted below proceeds as best-effort; the
    // window-bounded residue is documented in the log-derived dedup RFC.
    append_done->set_value();
    if (enqueued_result.failed()) {
        auto ex = enqueued_result.get_exception();
        vlog(
          clusterlog.debug,
          "{} dedup replicate enqueue failed: {}",
          _raft->ntp(),
          ex);
        _state.revert_request(filtered.undo);
        co_return errc::replication_error;
    }
    enqueued->set_value();

    auto replicated = co_await ss::coroutine::as_future(
      std::move(stages.replicate_finished));
    if (replicated.failed()) {
        auto ex = replicated.get_exception();
        vlog(
          clusterlog.debug, "{} dedup replicate failed: {}", _raft->ntp(), ex);
        _state.revert_request(filtered.undo);
        co_return errc::replication_error;
    }
    auto result = replicated.get();
    if (!result) {
        _state.revert_request(filtered.undo);
        co_return result.error();
    }

    kafka_result response{
      .last_offset = from_log_offset(result.value().last_offset),
      .last_term = result.value().last_term};
    if (admitted_count != original_count) {
        response.replicated_record_count = admitted_count;
    }
    co_return response;
}

ss::future<iobuf> dedup_stm::take_raft_snapshot(model::offset) {
    // Convergent snapshot: serialize the current state regardless of the
    // target offset. populate() is idempotent (max-timestamp-wins), so a
    // replica installing this snapshot and replaying the remaining log
    // converges to exactly this state; see the log-derived dedup RFC.
    _state.evict_expired();
    co_return serde::to_iobuf(to_snapshot(_state, _generation));
}

ss::future<> dedup_stm::apply_raft_snapshot(const iobuf& buffer) {
    _state.clear();
    _generation = 0;
    if (!buffer.empty()) {
        auto snapshot = serde::from_iobuf<state_snapshot>(buffer.copy());
        restore_snapshot(_state, _generation, snapshot);
    }
    co_return;
}

ss::future<raft::local_snapshot_applied>
dedup_stm::apply_local_snapshot(raft::stm_snapshot_header, iobuf&& buffer) {
    auto snapshot = serde::from_iobuf<state_snapshot>(std::move(buffer));
    restore_snapshot(_state, _generation, snapshot);
    co_return raft::local_snapshot_applied::yes;
}

ss::future<raft::stm_snapshot>
dedup_stm::take_local_snapshot(ssx::semaphore_units apply_units) {
    _state.evict_expired();
    auto snapshot = to_snapshot(_state, _generation);
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
