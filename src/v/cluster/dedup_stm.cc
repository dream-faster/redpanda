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
#include "storage/types.h"

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
    // Only reached when get_initial_recovery_start_offset() returned
    // std::nullopt, i.e. dedup isn't configured: nothing to recover.
    return raft::stm_initial_recovery_policy::skip_to_end;
}

ss::future<std::optional<model::offset>>
dedup_stm::get_initial_recovery_start_offset() {
    auto window = _raft->log()->config().dedup_window_ms();
    if (!window) {
        co_return std::nullopt;
    }
    const auto log_offsets = _raft->log()->offsets();
    if (log_offsets.start_offset > log_offsets.committed_offset) {
        // Empty log: nothing to bound.
        co_return std::nullopt;
    }
    const auto cutoff = model::timestamp(
      model::timestamp::now().value() - window->count());
    auto result = co_await _raft->timequery(
      storage::timequery_config{
        log_offsets.start_offset,
        cutoff,
        log_offsets.committed_offset,
        model::record_batch_type::raft_data});
    if (!result) {
        // Nothing retained is within the window (e.g. the whole log
        // predates it): nothing to index, skip straight to the tail.
        co_return model::next_offset(log_offsets.committed_offset);
    }
    co_return result->offset;
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
    for (const auto& entry : snapshot.entries) {
        result.entries.push_back(
          {.identity_hi = entry.identity.hi,
           .identity_lo = entry.identity.lo,
           .timestamp = entry.timestamp});
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
        restored.entries.push_back(
          {.identity = {.hi = entry.identity_hi, .lo = entry.identity_lo},
           .timestamp = entry.timestamp});
    }
    state.restore(restored);
    generation = snapshot.generation;
}

void dedup_stm::adopt_config(
  std::chrono::milliseconds window,
  int64_t generation,
  const std::optional<ss::sstring>& key_header) {
    if (_generation != generation) {
        _state.clear();
        _generation = generation;
    }
    _state.set_window(window);
    _state.set_key_header(key_header);
}

ss::future<> dedup_stm::do_apply(const model::record_batch& batch) {
    // Checked first, ahead of every other early return below: the STM
    // manager dispatches *every* committed batch on the partition to
    // do_apply (not just plain raft_data ones), so this is the one point
    // guaranteed to run regardless of what kind of traffic the partition
    // carries. adopt_config() (the only other caller of _state.clear()) is
    // never reached while dedup stays disabled, so a partition receiving
    // only control, transactional, or idempotent traffic -- or no traffic
    // at all -- after dedup is disabled would otherwise never release the
    // index. map_size() guards against clearing (and resetting _max_ts) on
    // every batch once already empty.
    const auto& cfg = _raft->log()->config();
    auto window = cfg.dedup_window_ms();
    if (!window) {
        if (_state.map_size() > 0) {
            _state.clear();
        }
        co_return;
    }
    if (batch.header().type != model::record_batch_type::raft_data) {
        co_return;
    }
    if (batch.header().attrs.is_control()) {
        co_return;
    }
    if (batch.header().producer_id >= 0) {
        // Idempotent and transactional data batches carry a real producer
        // id; plain produce batches don't (see model::batch_builder's
        // default of -1). G6 documents that idempotent/transactional
        // produce bypasses dedup entirely, matching partition.cc's routing
        // (only plain batches reach dedup_stm::replicate_in_stages on the
        // leader). Indexing them here too -- which the leader-side check
        // alone doesn't prevent, since do_apply sees every replica's
        // committed batches regardless of which STM replicated them --
        // would let a record from a transaction that later aborts poison
        // the index against an ordinary record with the same identity,
        // even though the aborted record is never visible to
        // read_committed consumers.
        co_return;
    }
    adopt_config(*window, cfg.dedup_generation(), cfg.dedup_key_header());

    std::optional<model::record_batch> decompressed;
    if (batch.compressed()) {
        decompressed = model::decompress_batch_sync(batch);
    }
    const model::record_batch& readable = decompressed ? *decompressed : batch;
    const auto base_ts = readable.header().first_timestamp;
    const auto& key_header = _state.key_header();
    readable.for_each_record([this, base_ts, &key_header](model::record r) {
        const iobuf* identity = nullptr;
        // The header identity source can change after this batch was
        // already validated and committed under an older config, so a
        // record here may lack the *current* header; skip indexing that
        // record rather than rejecting an already committed batch.
        if (
          dedup_identity_for_record(r, key_header, &identity)
          == dedup_identity_lookup::identity) {
            _state.populate(
              *identity,
              model::timestamp{base_ts.value() + r.timestamp_delta()});
        }
    });
}

kafka::offset dedup_stm::from_log_offset(model::offset offset) const {
    return kafka::offset(_raft->log()->from_log_offset(offset));
}

kafka_stages dedup_stm::replicate_in_stages(
  model::record_batch batch, raft::replicate_options opts) {
    auto enqueued = ss::make_lw_shared<available_promise<>>();
    auto enqueued_f = enqueued->get_future();
    auto finished
      = do_replicate(std::move(batch), opts, enqueued).finally([enqueued] {
            if (!enqueued->available()) {
                enqueued->set_value();
            }
        });
    return {std::move(enqueued_f), std::move(finished)};
}

ss::future<result<kafka_result>> dedup_stm::do_replicate(
  model::record_batch batch,
  raft::replicate_options opts,
  ss::lw_shared_ptr<available_promise<>> enqueued) {
    auto gate_holder = _gate.hold();

    if (!co_await sync(_sync_timeout())) {
        co_return errc::not_leader;
    }
    if (!opts.expected_term) {
        opts.expected_term = _insync_term;
    }

    const auto original_count = batch.record_count();
    dedup_filter_result filtered;
    const auto& cfg = _raft->log()->config();
    if (auto window = cfg.dedup_window_ms(); window) {
        adopt_config(*window, cfg.dedup_generation(), cfg.dedup_key_header());
        filtered = _state.filter_request(std::move(batch));
    } else {
        // Dedup was disabled between the partition's routing check and this
        // point: replicate unfiltered.
        filtered.batch = std::move(batch);
    }
    if (filtered.missing_required_header) {
        // Header mode is active and some record lacked the configured
        // header. Nothing was mutated or replicated; the record is never
        // silently admitted un-deduplicated.
        enqueued->set_value();
        co_return errc::invalid_request;
    }
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
        if (!tail_wait.get()) {
            // The introducing request's enqueue failed: no copy of the key
            // that made this request observe a duplicate ever reached the
            // log. Proceeding here would risk acknowledging a duplicate of
            // a write that doesn't exist. Revert this request's own
            // mutations and fail it so the producer retries the whole
            // batch -- by then the introducer's reverted entry is gone, so
            // the retry is classified fresh.
            vlog(
              clusterlog.debug,
              "{} dedup append fence observed a failed introducing enqueue, "
              "failing this request for retry",
              _raft->ntp());
            _state.revert_request(filtered.undo);
            co_return errc::replication_error;
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
            // Commit-based, not visibility-based: last_visible_index() can
            // advance past the true flush-durable commit point under
            // relaxed-consistency traffic on the same partition (see
            // maybe_update_last_visible_index()), which would let this wait
            // resolve -- and the duplicate get acked -- before the
            // introducing write is actually durable. events().wait() waits
            // on the real commit index. Fall back to a throwaway,
            // never-triggered abort_source when the caller didn't supply
            // one, matching what passing std::nullopt to the previous
            // optional-abort_source wait already meant: no external
            // cancellation signal, timeout still enforced below.
            ss::abort_source local_as;
            ss::abort_source& as = opts.as ? opts.as->get() : local_as;
            auto waited = co_await ss::coroutine::as_future(
              _raft->events().wait(
                fence, model::timeout_clock::now() + _sync_timeout(), as));
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
    auto append_done = ss::make_lw_shared<ss::shared_promise<bool>>();
    _append_tail = append_done;
    auto enqueued_result = co_await ss::coroutine::as_future(
      std::move(stages.request_enqueued));
    // Resolve the fence unconditionally so waiters never hang, but carry
    // whether the enqueue actually succeeded: a waiter must not treat this
    // introducer's failure as proof its own duplicate has a durable copy
    // (see the check after the wait above).
    append_done->set_value(!enqueued_result.failed());
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

bool dedup_stm::try_restore_snapshot(iobuf buffer) {
    // A snapshot written before the index switched to identity digests
    // (state_snapshot version 0) carries identities this build cannot
    // re-derive digests from, so serde rejects it on compat version. The
    // index is advisory and log-derived, so starting empty and rebuilding
    // from the retained log is a correct -- if slower -- outcome, and a far
    // better one than refusing to start the partition.
    try {
        auto snapshot = serde::from_iobuf<state_snapshot>(std::move(buffer));
        restore_snapshot(_state, _generation, snapshot);
        return true;
    } catch (...) {
        auto ex = std::current_exception();
        vlog(
          clusterlog.warn,
          "{} discarding unreadable dedup snapshot, rebuilding the index from "
          "the log: {}",
          _raft->ntp(),
          ex);
        _state.clear();
        _generation = 0;
        return false;
    }
}

ss::future<> dedup_stm::apply_raft_snapshot(const iobuf& buffer) {
    _state.clear();
    _generation = 0;
    if (!buffer.empty()) {
        try_restore_snapshot(buffer.copy());
    }
    co_return;
}

ss::future<raft::local_snapshot_applied>
dedup_stm::apply_local_snapshot(raft::stm_snapshot_header, iobuf&& buffer) {
    try_restore_snapshot(std::move(buffer));
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
