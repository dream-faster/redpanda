// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/dedup_stm.h"

#include "base/units.h"
#include "bytes/iobuf_parser.h"
#include "cluster/logger.h"
#include "cluster/snapshot.h"
#include "config/configuration.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"
#include "model/batch_compression.h"
#include "model/namespace.h"
#include "model/record_batch_types.h"
#include "raft/consensus.h"
#include "serde/serde_exception.h"
#include "ssx/future-util.h"
#include "ssx/sformat.h"
#include "storage/kvstore.h"
#include "storage/types.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/coroutine/as_future.hh>

#include <array>
#include <cstring>
#include <new>

namespace cluster {

dedup_stm::dedup_stm(
  raft::consensus* raft,
  ss::logger& logger,
  storage::kvstore& kvstore,
  config::binding<std::chrono::milliseconds> sync_timeout,
  size_t max_entries,
  ss::lw_shared_ptr<dedup_shard_budget> shard_budget)
  : raft::persisted_stm<>(dedup_stm_snapshot, logger, raft)
  , _kvstore(kvstore)
  , _sync_timeout(std::move(sync_timeout))
  , _state(std::chrono::milliseconds{0}, max_entries, std::move(shard_budget)) {
}

void dedup_stm::setup_metrics() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }
    namespace sm = ss::metrics;

    const auto& ntp = _raft->ntp();
    const std::vector<sm::label_instance> labels = {
      metrics::namespace_label(ntp.ns()),
      metrics::topic_label(ntp.tp.topic()),
      metrics::partition_label(ntp.tp.partition()),
    };

    _metrics.add_group(
      prometheus_sanitize::metrics_name("dedup:partition"),
      {
        sm::make_counter(
          "records_dropped",
          [this] { return _state.dropped_records(); },
          sm::description("Number of records dropped as duplicates."),
          labels),
        sm::make_counter(
          "records_skew_clamped",
          [this] { return _state.skew_clamped_records(); },
          sm::description(
            "Number of records whose timestamp was ahead of the broker clock "
            "and was clamped before reaching the dedup index. A non-zero "
            "value indicates producer clock skew."),
          labels),
        sm::make_counter(
          "records_unindexed",
          [this] { return _state.unindexed_records(); },
          sm::description(
            "Number of records admitted without being indexed because the "
            "dedup index was at dedup_max_entries_per_partition. Duplicates "
            "of these records are not detected."),
          labels),
        sm::make_counter(
          "capacity_evicted_entries",
          [this] { return _state.capacity_evicted_entries(); },
          sm::description(
            "Number of still-live identities dropped to make room at "
            "dedup_max_entries_per_partition. Non-zero means the effective "
            "dedup window on this partition is narrower than the configured "
            "one: roughly the entry limit divided by the new-identity "
            "arrival rate."),
          labels),
        sm::make_counter(
          "capacity_emergency_sweeps",
          [this] { return _state.capacity_sweeps(); },
          sm::description(
            "Number of times a new identity arrived with the dedup index "
            "exactly at dedup_max_entries_per_partition, so the cursor had "
            "to run a bounded extra sweep to find room. Non-zero alongside "
            "records_unindexed means that sweep sometimes found nothing "
            "freeable."),
          labels),
        sm::make_counter(
          "snapshot_entries_truncated",
          [this] { return _state.truncated_entries(); },
          sm::description(
            "Number of identities dropped while installing a snapshot larger "
            "than dedup_max_entries_per_partition on this broker. Non-zero "
            "means a peer retains a wider dedup window than this broker."),
          labels),
        sm::make_counter(
          "requests_rebuilt",
          [this] { return _state.rebuilt_requests(); },
          sm::description(
            "Number of produce requests whose batch had to be rebuilt and "
            "recompressed because some, but not all, of its records were "
            "dropped as duplicates. This is the rate that drives the dedup "
            "feature's cost on the produce path."),
          labels),
        sm::make_gauge(
          "index_entries",
          [this] { return _state.map_size(); },
          sm::description("Number of identities in the dedup index."),
          labels),
      },
      {},
      {sm::shard_label, metrics::partition_label});
}

ss::future<> dedup_stm::start() {
    // Earlier builds kept the snapshot in the kvstore, which holds it in
    // memory until it is removed. It is not migrated: the index rebuilds from
    // the log, bounded to one window by get_initial_recovery_start_offset().
    // kvstore recovery already discards these keys (see
    // kvstore_config::discarded_stm_snapshots), so this only matters for a
    // kvstore recovered without that, and must not cost every partition start
    // a kvstore write and flush when the key is absent.
    auto key = bytes::from_string(
      raft::kvstore_backed_stm_snapshot::snapshot_key(
        dedup_stm_snapshot, _raft->ntp()));
    if (_kvstore.contains(storage::kvstore::key_space::stms, key)) {
        co_await _kvstore.remove(
          storage::kvstore::key_space::stms, std::move(key));
    }
    {
        // persisted_stm::start() reads the whole snapshot file before the
        // restore trims it to what fits; one partition at a time per shard
        // keeps that transient to one snapshot however many partitions start
        // together.
        std::optional<ssx::semaphore_units> one_at_a_time;
        if (auto budget = _state.shard_budget(); budget) {
            one_at_a_time = co_await budget->restore_one_at_a_time.get_units(1);
        }
        co_await raft::persisted_stm<>::start();
    }
    setup_metrics();
}

ss::future<> dedup_stm::stop() {
    // Every counter is a lambda reading _state through this, so the
    // registrations must not outlive the STM. Pairing them with start()/stop()
    // rather than the constructor and destructor keeps that true for an STM
    // that is constructed and discarded without ever running, which is what
    // the fixture does when it recreates nodes.
    _metrics.clear();
    co_await raft::persisted_stm<>::stop();
}

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
    // timequery() can resolve to an offset in the middle of a multi-record
    // batch (see storage::batch_timequery, which walks into a batch to find
    // the first record at or after the target timestamp), but the STM
    // manager's apply loop requires next() to land exactly on a batch's
    // base offset: batch_applicator::apply_to_stm() treats
    // stm->next() > batch.base_offset() as "already applied" and skips the
    // batch without ever advancing next() past it, permanently stalling
    // this STM if next() ends up mid-batch. Round down to the nearest
    // indexed batch base offset at or before the resolved offset -- never
    // past it, since replaying a little more than exactly one window is
    // harmless but skipping into the window wouldn't be -- falling back to
    // the log's start offset (itself always a valid batch boundary) if
    // nothing is indexed yet.
    auto aligned = _raft->log()->index_batch_base_offset_lower_bound(
      result->offset);
    co_return aligned.value_or(log_offsets.start_offset);
}

iobuf dedup_stm::serialize_snapshot(
  const dedup_window_filter& state, int64_t generation) {
    // One pass over the live index, which cannot yield: the produce path
    // mutates the index without the apply lock. Entries are staged in a fixed
    // local buffer and appended a chunk at a time, so the pass costs a memcpy
    // per entry rather than an iobuf append per entry.
    constexpr size_t chunk_entries = 16_KiB / packed_entry_size;
    std::array<char, chunk_entries * packed_entry_size> chunk{};
    size_t staged = 0;
    iobuf entries;
    state.for_each_live_entry(
      [&](dedup_identity_digest identity, model::timestamp timestamp) {
          char* out = chunk.data() + staged * packed_entry_size;
          const auto hi = ss::cpu_to_le(identity.hi);
          const auto lo = ss::cpu_to_le(identity.lo);
          const auto ts = ss::cpu_to_le(timestamp.value());
          std::memcpy(out, &hi, sizeof(hi));
          std::memcpy(out + sizeof(hi), &lo, sizeof(lo));
          std::memcpy(out + sizeof(hi) + sizeof(lo), &ts, sizeof(ts));
          if (++staged == chunk_entries) {
              entries.append(chunk.data(), staged * packed_entry_size);
              staged = 0;
          }
      });
    if (staged > 0) {
        entries.append(chunk.data(), staged * packed_entry_size);
    }
    // serde moves the entry fragments into the output rather than copying
    // them, so this is still the only serialized copy.
    return serde::to_iobuf(
      state_snapshot{
        .window_ms = state.window().count(),
        .generation = generation,
        .max_timestamp = state.max_timestamp(),
        .entries = std::move(entries)});
}

void dedup_stm::restore_snapshot(
  dedup_window_filter& state, int64_t& generation, state_snapshot snapshot) {
    const auto packed_bytes = snapshot.entries.size_bytes();
    if (packed_bytes % packed_entry_size != 0) {
        throw serde::serde_exception(
          ssx::sformat(
            "dedup snapshot entries are {} bytes, not a multiple of {}",
            packed_bytes,
            packed_entry_size));
    }
    state.set_window(std::chrono::milliseconds{snapshot.window_ms});
    // Read straight from the packed entries -- twice when they must be
    // trimmed -- rather than decoded into an entry vector first: restores run
    // at startup for every partition on the shard, and one more full copy of
    // each index is memory a starting broker may not have. restore_from()
    // owns the selection of which entries to keep.
    const auto& packed = snapshot.entries;
    state.restore_from(
      packed_bytes / packed_entry_size,
      snapshot.max_timestamp,
      [&packed](auto&& emit) {
          iobuf_const_parser parser(packed);
          while (parser.bytes_left() > 0) {
              const auto hi = ss::le_to_cpu(parser.consume_type<uint64_t>());
              const auto lo = ss::le_to_cpu(parser.consume_type<uint64_t>());
              const auto ts = ss::le_to_cpu(parser.consume_type<int64_t>());
              emit(
                dedup_identity_digest{.hi = hi, .lo = lo},
                model::timestamp{ts});
          }
      });
    generation = snapshot.generation;
}

void dedup_stm::adopt_config(
  std::chrono::milliseconds window,
  int64_t generation,
  const std::optional<ss::sstring>& key_header) {
    if (_generation != generation) {
        _state.clear();
        // Whatever this leader filtered under the old generation is gone
        // from the index; its batches must be indexed when they apply.
        _self_filtered.clear();
        _self_filtered_order.clear();
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
            _self_filtered.clear();
            _self_filtered_order.clear();
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
    if (take_self_filtered(self_filtered_key(batch.header()))) {
        // This leader indexed the batch's identities when it filtered the
        // request, with these timestamps (max-wins makes the two the same
        // state), so applying it again would only cost the parse. Followers,
        // which never filtered it, index it here.
        co_return;
    }

    std::optional<model::record_batch> decompressed;
    if (batch.compressed()) {
        decompressed = model::decompress_batch_sync(batch);
    }
    const model::record_batch& readable = decompressed ? *decompressed : batch;
    // The header identity source can change after this batch was already
    // validated and committed under an older config, so a record here may
    // lack the *current* header; it is left unindexed rather than rejecting
    // an already committed batch.
    for_each_dedup_identity(
      readable,
      _state.key_header(),
      [this](
        dedup_identity_lookup lookup,
        dedup_identity_digest digest,
        model::timestamp ts) {
          if (lookup == dedup_identity_lookup::identity) {
              _state.populate(digest, ts);
          }
          return true;
      });
}

kafka::offset dedup_stm::from_log_offset(model::offset offset) const {
    return kafka::offset(_raft->log()->from_log_offset(offset));
}

uint64_t dedup_stm::self_filtered_key(const model::record_batch_header& h) {
    // The batch crc covers the records and the attributes, timestamps and
    // count, but not the base offset raft assigns: it is the same when the
    // batch comes back through do_apply as when filter_request() saw it. A
    // crc collision between two batches in flight on one partition would
    // leave the other's identities unindexed on this replica alone -- a
    // 2^-32 chance per pair of a bounded coverage loss the index already
    // tolerates elsewhere.
    return (static_cast<uint64_t>(h.crc) << 32)
           | static_cast<uint32_t>(h.record_count);
}

void dedup_stm::remember_self_filtered(uint64_t key) {
    if (_self_filtered_order.size() >= self_filtered_max) {
        forget_self_filtered(_self_filtered_order.front());
        _self_filtered_order.pop_front();
    }
    _self_filtered_order.push_back(key);
    ++_self_filtered.try_emplace(key, 0U).first->second;
}

void dedup_stm::forget_self_filtered(uint64_t key) {
    auto it = _self_filtered.find(key);
    if (it == _self_filtered.end()) {
        return;
    }
    if (--it->second == 0) {
        _self_filtered.erase(it);
    }
}

bool dedup_stm::take_self_filtered(uint64_t key) {
    auto it = _self_filtered.find(key);
    if (it == _self_filtered.end()) {
        return false;
    }
    if (--it->second == 0) {
        _self_filtered.erase(it);
    }
    return true;
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
        const auto unindexed_before = _state.unindexed_records();
        filtered = _state.filter_request(std::move(batch));
        if (_state.unindexed_records() != unindexed_before) {
            // The index is at its ceiling, or the shard is at its own, and is
            // admitting new identities without remembering them, so their
            // duplicates go undetected. Raising either limit needs a broker
            // restart, so operators need to know this is happening.
            thread_local static ss::logger::rate_limit rate(
              std::chrono::minutes(5));
            if (_state.map_size() >= _state.max_entries()) {
                clusterlog.log(
                  ss::log_level::warn,
                  rate,
                  "Dedup index for {} is full at {} entries; new identities "
                  "are admitted without being deduplicated. Consider raising "
                  "dedup_max_entries_per_partition.",
                  _raft->ntp(),
                  _state.max_entries());
            } else {
                clusterlog.log(
                  ss::log_level::warn,
                  rate,
                  "Dedup indices on this shard are full ({} entries, {} of "
                  "them for {}); new identities are admitted without being "
                  "deduplicated. Consider raising dedup_max_entries_per_shard "
                  "or giving the broker more memory per core.",
                  _state.shard_budget() ? _state.shard_budget()->entries : 0,
                  _state.map_size(),
                  _raft->ntp());
            }
        }
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
            // The wait is on an offset, not on an offset in a term:
            // event_manager::wait() resolves as soon as the commit index
            // reaches `fence`, whoever wrote the entry sitting there. A new
            // leader that truncated this term's tail and replicated its own
            // records advances the commit index just the same, so without
            // this check the wait can resolve on records that *replaced* the
            // introducing write -- and this request would report a write the
            // log does not hold. Nothing else guards it: a request that
            // replicates is covered by opts.expected_term, but a fully
            // deduplicated one never replicates.
            //
            // Revalidating the term is what makes the wait mean what this
            // branch needs, because within one unbroken term the entries at
            // or below the `fence` read above are this leader's own and
            // cannot have been truncated -- truncation requires a higher
            // term. raft::replication_monitor would report truncation
            // directly, but it is documented as raft-internal and points
            // other subsystems back at event_manager.
            if (!_raft->is_leader() || _raft->term() != _insync_term) {
                vlog(
                  clusterlog.debug,
                  "{} dedup fence for {} resolved after losing term {}, "
                  "failing the request for retry",
                  _raft->ntp(),
                  fence,
                  _insync_term);
                co_return errc::not_leader;
            }
        }
        co_return kafka_result{
          .last_offset = from_log_offset(_raft->committed_offset()),
          .last_term = _raft->term(),
          .replicated_record_count = 0};
    }

    const auto self_key = self_filtered_key(filtered.batch->header());
    remember_self_filtered(self_key);
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
        forget_self_filtered(self_key);
        co_return errc::replication_error;
    }
    enqueued->set_value();

    auto replicated = co_await ss::coroutine::as_future(
      std::move(stages.replicate_finished));
    if (replicated.failed()) {
        auto ex = replicated.get_exception();
        vlog(
          clusterlog.debug, "{} dedup replicate failed: {}", _raft->ntp(), ex);
        // The entries are reverted, so if the batch commits after all its
        // apply must index it: stop telling do_apply to skip it.
        _state.revert_request(filtered.undo);
        forget_self_filtered(self_key);
        co_return errc::replication_error;
    }
    auto result = replicated.get();
    if (!result) {
        _state.revert_request(filtered.undo);
        forget_self_filtered(self_key);
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
    co_return serialize_snapshot(_state, _generation);
}

bool dedup_stm::try_restore_snapshot(iobuf buffer) {
    // A snapshot written before the index switched to identity digests
    // (state_snapshot version 0) carries identities this build cannot
    // re-derive digests from, so serde rejects it on compat version. The
    // index is advisory and log-derived, so discarding it and rebuilding
    // from the retained log is a correct -- if slower -- outcome, and a far
    // better one than refusing to start the partition.
    try {
        restore_snapshot(
          _state,
          _generation,
          serde::from_iobuf<state_snapshot>(std::move(buffer)));
        return true;
    } catch (const std::bad_alloc&) {
        // Not a bad snapshot either, and the worst thing to answer with: a
        // discard reports "rebuilding from the log", which allocates the
        // same index again. Let it propagate instead of looping.
        throw;
    } catch (...) {
        auto ex = std::current_exception();
        if (ssx::is_shutdown_exception(ex)) {
            // Not a bad snapshot: the node is going away. Let it propagate
            // rather than reporting a rebuild that will never happen.
            std::rethrow_exception(ex);
        }
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
    // Reporting `no` for a snapshot we discarded is what actually makes the
    // rebuild happen: persisted_stm only calls set_next(next_offset) -- i.e.
    // skips the log the snapshot covered -- when the snapshot was applied.
    // Answering `yes` here would leave the index empty *and* skip past the
    // records it should have been rebuilt from.
    co_return try_restore_snapshot(std::move(buffer))
      ? raft::local_snapshot_applied::yes
      : raft::local_snapshot_applied::no;
}

ss::future<raft::stm_snapshot>
dedup_stm::take_local_snapshot(ssx::semaphore_units apply_units) {
    auto buffer = serialize_snapshot(_state, _generation);
    auto offset = last_applied_offset();
    apply_units.return_all();
    // The 0 here is stm_snapshot_header::version, a channel this STM does
    // not use: unlike rm_stm/tm_stm, which dispatch between snapshot layouts
    // on it, dedup_stm versions its payload through the serde envelope
    // (state_snapshot's version/compat_version). Deliberately left at 0 so
    // there is only one place the format is versioned.
    co_return raft::stm_snapshot::create(0, offset, std::move(buffer));
}

namespace {

size_t resolve_shard_limit(size_t configured) {
    if (configured > 0) {
        return configured;
    }
    const auto shard_memory = ss::memory::stats().total_memory();
    return std::max<size_t>(
      1,
      shard_memory / 100 * DEDUP_SHARD_MEMORY_PERCENT
        / DEDUP_SHARD_BYTES_PER_ENTRY);
}

} // namespace

dedup_stm_factory::dedup_stm_factory(
  storage::kvstore& kvstore,
  config::binding<std::chrono::milliseconds> sync_timeout,
  size_t max_entries,
  size_t max_entries_per_shard)
  : _kvstore(kvstore)
  , _sync_timeout(std::move(sync_timeout))
  , _max_entries(max_entries)
  , _shard_budget(
      ss::make_lw_shared<dedup_shard_budget>(
        resolve_shard_limit(max_entries_per_shard))) {
    vlog(
      clusterlog.info,
      "Dedup indices on this shard are limited to {} entries in total",
      _shard_budget->max_entries);
    setup_metrics();
}

void dedup_stm_factory::setup_metrics() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }
    namespace sm = ss::metrics;
    _metrics.add_group(
      prometheus_sanitize::metrics_name("dedup:shard"),
      {
        sm::make_gauge(
          "index_entries",
          [budget = _shard_budget] { return budget->entries; },
          sm::description(
            "Number of identities in all dedup indices on this shard.")),
        sm::make_gauge(
          "max_index_entries",
          [budget = _shard_budget] { return budget->max_entries; },
          sm::description(
            "Limit on the identities all dedup indices on this shard hold "
            "together (dedup_max_entries_per_shard).")),
      });
}

bool dedup_stm_factory::is_applicable_for(
  const storage::ntp_config& cfg) const {
    return model::is_user_topic(cfg.ntp());
}

void dedup_stm_factory::create(
  raft::state_machine_manager_builder& builder,
  raft::consensus* raft,
  const cluster::stm_instance_config&) {
    auto stm = builder.create_stm<dedup_stm>(
      raft, clusterlog, _kvstore, _sync_timeout, _max_entries, _shard_budget);
    raft->log()->stm_hookset()->add_stm(stm);
}

} // namespace cluster
