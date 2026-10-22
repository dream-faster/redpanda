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
#include "raft/errc.h"
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
#include <seastar/coroutine/maybe_yield.hh>

#include <algorithm>
#include <array>
#include <cstring>
#include <new>
#include <vector>

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
          "expired_entries_reclaimed",
          [this] { return _state.reclaimed_entries(); },
          sm::description(
            "Number of expired identities dropped from this partition's "
            "index by other partitions' inserts while the shard was near "
            "dedup_max_entries_per_shard, before any live identity on the "
            "shard is trimmed."),
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
        sm::make_counter(
          "batches_skipped",
          [this] { return _batches_skipped; },
          sm::description(
            "Number of committed batches whose records could not be read "
            "while indexing them. Their identities are not in the index, so "
            "their duplicates are not detected."),
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
            one_at_a_time = co_await ss::get_units(
              budget->restore_one_at_a_time, 1);
        }
        co_await raft::persisted_stm<>::start();
    }
    if (!_local_snapshot_applied) {
        // The STM manager bounds recovery to the dedup window only on the
        // partition's first start; afterwards it resumes from the offset it
        // recorded then. Without a local snapshot to resume from (the file
        // was lost, or discarded on a version break) that would replay the
        // whole retained log before the partition answers a produce request.
        // Everything before the bounded offset is older than the window and
        // could not cause a drop, so skip to it; the manager only ever moves
        // this forward.
        if (
          auto bounded = co_await get_initial_recovery_start_offset();
          bounded && *bounded > next()) {
            vlog(
              clusterlog.info,
              "{} dedup index has no snapshot to resume from, rebuilding "
              "from offset {} (one window) rather than {}",
              _raft->ntp(),
              *bounded,
              next());
            set_next(*bounded);
        }
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

ss::future<iobuf> dedup_stm::serialize_snapshot(
  dedup_window_filter& state,
  const int64_t& generation,
  const pending_identities* pending) {
    // Entries are staged in a fixed local buffer and appended a chunk at a
    // time, so the pass costs a memcpy per entry rather than an iobuf append
    // per entry.
    constexpr size_t chunk_entries = 16_KiB / packed_entry_size;
    std::array<char, chunk_entries * packed_entry_size> chunk{};
    iobuf entries;
    for (;;) {
        size_t staged = 0;
        const bool complete = co_await state.for_each_live_entry_async(
          [&](dedup_identity_digest identity, model::timestamp timestamp) {
              if (pending && pending->contains(identity)) {
                  return;
              }
              char* out = chunk.data() + staged * packed_entry_size;
              const auto lo = ss::cpu_to_le(identity.lo);
              const auto hi = ss::cpu_to_le(
                static_cast<uint32_t>(identity.hi & dedup_digest_hi_mask));
              const auto ts = ss::cpu_to_le(timestamp.value());
              std::memcpy(out, &lo, sizeof(lo));
              std::memcpy(out + sizeof(lo), &hi, sizeof(hi));
              std::memcpy(out + sizeof(lo) + sizeof(hi), &ts, sizeof(ts));
              if (++staged == chunk_entries) {
                  entries.append(chunk.data(), staged * packed_entry_size);
                  staged = 0;
              }
          });
        if (complete) {
            if (staged > 0) {
                entries.append(chunk.data(), staged * packed_entry_size);
            }
            break;
        }
        // The index was cleared or replaced under the walk (a generation
        // change, a raft snapshot): what was written describes no single
        // index. Rare, so simply start over on the new one.
        entries = iobuf{};
    }
    // serde moves the entry fragments into the output rather than copying
    // them, so this is still the only serialized copy.
    co_return serde::to_iobuf(
      state_snapshot{
        .window_ms = state.window().count(),
        .generation = generation,
        .max_timestamp = state.max_timestamp(),
        .entries = std::move(entries),
        .entry_size = packed_entry_size});
}

namespace {

/// Decode the packed entries of a state_snapshot, yielding every few
/// thousand so a large restore shares the reactor. \p entry_size names the
/// layout: dedup_stm::packed_entry_size, or the version 2 one.
template<typename Emit>
ss::future<>
for_each_packed_entry(const iobuf& packed, size_t entry_size, Emit emit) {
    constexpr size_t entries_per_yield = 4096;
    iobuf_const_parser parser(packed);
    size_t n = 0;
    while (parser.bytes_left() > 0) {
        dedup_identity_digest identity;
        if (entry_size == dedup_stm::legacy_packed_entry_size) {
            identity.hi = ss::le_to_cpu(parser.consume_type<uint64_t>());
            identity.lo = ss::le_to_cpu(parser.consume_type<uint64_t>());
        } else {
            identity.lo = ss::le_to_cpu(parser.consume_type<uint64_t>());
            identity.hi = ss::le_to_cpu(parser.consume_type<uint32_t>());
        }
        const auto ts = ss::le_to_cpu(parser.consume_type<int64_t>());
        emit(dedup_canonical_digest(identity), model::timestamp{ts});
        if ((++n % entries_per_yield) == 0) {
            co_await ss::coroutine::maybe_yield();
        }
    }
}

} // namespace

dedup_stm::state_snapshot dedup_stm::decode_snapshot(iobuf buffer) {
    // An envelope starts with its version.
    if (buffer.size_bytes() > 0) {
        iobuf_const_parser peek(buffer);
        if (
          peek.consume_type<serde::version_t>()
          == state_snapshot_v2::redpanda_serde_version) {
            auto v2 = serde::from_iobuf<state_snapshot_v2>(std::move(buffer));
            return state_snapshot{
              .window_ms = v2.window_ms,
              .generation = v2.generation,
              .max_timestamp = v2.max_timestamp,
              .entries = std::move(v2.entries),
              .entry_size = legacy_packed_entry_size};
        }
    }
    return serde::from_iobuf<state_snapshot>(std::move(buffer));
}

ss::future<> dedup_stm::restore_snapshot(
  dedup_window_filter& state, int64_t& generation, state_snapshot snapshot) {
    const size_t entry_size = snapshot.entry_size;
    if (
      entry_size != packed_entry_size
      && entry_size != legacy_packed_entry_size) {
        throw serde::serde_exception(
          ssx::sformat(
            "dedup snapshot entries of unknown size {}", entry_size));
    }
    const auto packed_bytes = snapshot.entries.size_bytes();
    if (packed_bytes % entry_size != 0) {
        throw serde::serde_exception(
          ssx::sformat(
            "dedup snapshot entries are {} bytes, not a multiple of {}",
            packed_bytes,
            entry_size));
    }
    state.set_window(std::chrono::milliseconds{snapshot.window_ms});
    // Read straight from the packed entries -- in passes, when they must be
    // trimmed -- rather than decoded into an entry vector first: restores run
    // at startup for every partition on the shard, and one more full copy of
    // each index is memory a starting broker may not have. restore_from()
    // owns the selection of which entries to keep.
    const auto& packed = snapshot.entries;
    co_await state.restore_from(
      packed_bytes / entry_size,
      snapshot.max_timestamp,
      [&packed, entry_size](auto&& emit) {
          return for_each_packed_entry(
            packed, entry_size, std::forward<decltype(emit)>(emit));
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
    const auto key = self_filtered_key(batch.header());
    if (auto it = _resolving.find(key); it != _resolving.end()) {
        // A batch raft timed out on has applied after all; its request's
        // identities are already in the index. resolve_after_timeout() is
        // waiting to hear so.
        it->second = true;
        co_return;
    }
    if (take_self_filtered(key)) {
        // This leader indexed the batch's identities when it filtered the
        // request, with these timestamps (max-wins makes the two the same
        // state), so applying it again would only cost the parse. Followers,
        // which never filtered it, index it here.
        co_return;
    }

    // A batch this replica cannot read is skipped, not retried: the STM
    // manager treats an exception from apply as transient and retries the
    // batch forever, which also holds up every raft snapshot and so the
    // log's prefix truncation. The index is advisory, and an unreadable
    // batch is one the leader could not have filtered either.
    try {
        std::optional<model::record_batch> decompressed;
        if (batch.compressed()) {
            decompressed = model::decompress_batch_sync(batch);
        }
        const model::record_batch& readable = decompressed ? *decompressed
                                                           : batch;
        // The header identity source can change after this batch was
        // already validated and committed under an older config, so a
        // record here may lack the *current* header; it is left unindexed
        // rather than rejecting an already committed batch.
        auto batch_scope = _state.admit_batch();
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
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        ++_batches_skipped;
        thread_local static ss::logger::rate_limit rate(
          std::chrono::minutes(1));
        auto ex = std::current_exception();
        clusterlog.log(
          ss::log_level::warn,
          rate,
          "{} dedup could not read the records of the batch at {}, its "
          "identities are not indexed: {}",
          _raft->ntp(),
          batch.base_offset(),
          ex);
    }
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
        // Most entries in the order are stale: their key was taken or
        // forgotten already. Only a live one counts as evicted.
        if (forget_self_filtered(_self_filtered_order.front())) {
            ++_self_filtered_evictions;
        }
        _self_filtered_order.pop_front();
    }
    _self_filtered_order.push_back(key);
    ++_self_filtered.try_emplace(key, 0U).first->second;
}

bool dedup_stm::forget_self_filtered(uint64_t key) {
    auto it = _self_filtered.find(key);
    if (it == _self_filtered.end()) {
        return false;
    }
    if (--it->second == 0) {
        _self_filtered.erase(it);
    }
    return true;
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
    // Set when the index was consulted, so do_apply() is told to skip the
    // batch only when filter_request() has indexed it (see self_filtered_key).
    bool indexed_here = false;
    const auto& cfg = _raft->log()->config();
    if (auto window = cfg.dedup_window_ms(); window) {
        adopt_config(*window, cfg.dedup_generation(), cfg.dedup_key_header());
        const auto unindexed_before = _state.unindexed_records();
        filtered = _state.filter_request(std::move(batch));
        indexed_here = true;
        if (_state.unindexed_records() != unindexed_before) {
            // The index is at its ceiling, or the shard is at its own, and is
            // admitting new identities without remembering them, so their
            // duplicates go undetected. Operators need to know this is
            // happening, to raise either limit (no restart needed).
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

    // The requests this one depends on: those still in flight whose
    // identities made it drop records. Until each is known appended, the
    // dropped records have no durable copy, and acknowledging this request
    // would tell the producer otherwise. Looked up before this request's own
    // identities go in, which happens before its first suspension, so a
    // request classified behind it finds them pending in turn.
    //
    // Only the acknowledgment waits for them, not this request's own
    // records: those are handed to raft straight away. The connection does
    // not read its next request until this one is enqueued, so waiting for an
    // introducer's replication first would stall every partition the
    // producer writes through this broker for a round trip, and add that
    // round trip to this request's own. If an introducer turns out never to
    // have been appended, the request fails for retry once its own records
    // are settled; the retry drops those against themselves and admits the
    // reverted ones afresh.
    std::vector<inflight_ptr> depends_on;
    for (const auto& identity : filtered.dropped) {
        if (auto it = _pending.find(identity); it != _pending.end()) {
            if (std::ranges::find(depends_on, it->second) == depends_on.end()) {
                depends_on.push_back(it->second);
            }
        }
    }
    inflight_ptr mine;
    if (filtered.batch) {
        mine = ss::make_lw_shared<inflight_request>();
        for (const auto& entry : filtered.undo.entries) {
            _pending.insert_or_assign(entry.identity, mine);
        }
    }

    if (!filtered.batch) {
        // Every record was a duplicate: nothing to replicate, so nothing for
        // the requests behind this one to be ordered after. Once its
        // introducers are appended, a quorum-ack request must not
        // acknowledge more strongly than they were.
        //
        // An identity in the index that is no longer pending came from a
        // committed batch -- applied, restored from a snapshot, or admitted
        // by a request raft reported done only once it committed -- unless
        // raft reported its batch done earlier: on the leader's own append
        // for weaker acks, on a majority's unflushed append under write
        // caching. So only those need the commit index to catch up, and
        // only while it is behind the last of them. Waiting for the dirty
        // offset instead held every all-duplicate request for a
        // replication round trip whatever it dropped against.
        enqueued->set_value();
        if (!depends_on.empty()) {
            auto failed = co_await wait_for_introducers(depends_on, opts.as);
            if (failed) {
                co_return *failed;
            }
        }
        if (opts.consistency != raft::consistency_level::quorum_ack) {
            co_return kafka_result{
              .last_offset = from_log_offset(_raft->committed_offset()),
              .last_term = _raft->term(),
              .replicated_record_count = 0};
        }
        if (
          _relaxed_admitted_term == _insync_term
          && _relaxed_admitted_offset > _raft->committed_offset()) {
            const auto fence = _relaxed_admitted_offset;
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
        // The wait above is on an offset, not on an offset in a term:
        // event_manager::wait() resolves as soon as the commit index reaches
        // the fence, whoever wrote the entry sitting there. A new leader that
        // truncated this term's tail and replicated its own records advances
        // the commit index just the same, so without this check the wait can
        // resolve on records that *replaced* the introducing write -- and
        // this request would report a write the log does not hold. Nothing
        // else guards it: a request that replicates is covered by
        // opts.expected_term, but a fully deduplicated one never replicates.
        // Without a fence to wait for, the check still keeps a request that
        // lost leadership waiting on its introducers from reporting success.
        //
        // Revalidating the term is what makes the wait mean what this branch
        // needs, because within one unbroken term the entries at or below the
        // fence are this leader's own and cannot have been truncated --
        // truncation requires a higher term. raft::replication_monitor would
        // report truncation directly, but it is documented as raft-internal
        // and points other subsystems back at event_manager.
        if (!_raft->is_leader() || _raft->term() != _insync_term) {
            vlog(
              clusterlog.debug,
              "{} dedup all-duplicate request lost term {} while waiting, "
              "failing it for retry",
              _raft->ntp(),
              _insync_term);
            co_return errc::not_leader;
        }
        co_return kafka_result{
          .last_offset = from_log_offset(_raft->committed_offset()),
          .last_term = _raft->term(),
          .replicated_record_count = 0};
    }

    std::optional<uint64_t> self_key;
    if (indexed_here) {
        self_key = self_filtered_key(filtered.batch->header());
        remember_self_filtered(*self_key);
    }
    const auto evictions_before = _self_filtered_evictions;
    const auto term = _insync_term;
    auto stages = _raft->replicate_in_stages(std::move(*filtered.batch), opts);
    auto enqueued_result = co_await ss::coroutine::as_future(
      std::move(stages.request_enqueued));
    if (enqueued_result.failed()) {
        auto ex = enqueued_result.get_exception();
        vlog(
          clusterlog.debug,
          "{} dedup replicate enqueue failed: {}",
          _raft->ntp(),
          ex);
        settle(mine, filtered.undo, false, self_key);
        co_return errc::replication_error;
    }
    enqueued->set_value();

    auto replicated = co_await ss::coroutine::as_future(
      std::move(stages.replicate_finished));
    if (replicated.failed()) {
        auto ex = replicated.get_exception();
        vlog(
          clusterlog.debug, "{} dedup replicate failed: {}", _raft->ntp(), ex);
        settle(mine, filtered.undo, false, self_key);
        co_return errc::replication_error;
    }
    auto result = replicated.get();
    if (!result) {
        if (result.error() == raft::errc::timeout) {
            // The batch may or may not have been appended; see
            // resolve_after_timeout(). Its identities stay pending until
            // that is known, so a retry of one of them neither gets
            // acknowledged against a write that may not exist nor admitted
            // next to one that does.
            ssx::spawn_with_gate(
              _gate,
              [this,
               mine,
               undo = std::move(filtered.undo),
               self_key,
               term,
               evictions_before]() mutable {
                  return resolve_after_timeout(
                    std::move(mine),
                    std::move(undo),
                    self_key,
                    term,
                    evictions_before);
              });
            co_return result.error();
        }
        settle(mine, filtered.undo, false, self_key);
        co_return result.error();
    }
    if (
      const auto& appended = result.value();
      appended.last_offset > _raft->committed_offset()) {
        // Reported done before it committed; see the all-duplicate branch
        // above. Recorded before settle() releases the requests waiting on
        // this one.
        if (appended.last_term > _relaxed_admitted_term) {
            _relaxed_admitted_term = appended.last_term;
            _relaxed_admitted_offset = appended.last_offset;
        } else if (appended.last_term == _relaxed_admitted_term) {
            _relaxed_admitted_offset = std::max(
              _relaxed_admitted_offset, appended.last_offset);
        }
    }
    settle(mine, filtered.undo, true, std::nullopt);
    if (!depends_on.empty()) {
        auto failed = co_await wait_for_introducers(depends_on, opts.as);
        if (failed) {
            co_return *failed;
        }
    }

    kafka_result response{
      .last_offset = from_log_offset(result.value().last_offset),
      .last_term = result.value().last_term};
    if (admitted_count != original_count) {
        response.replicated_record_count = admitted_count;
    }
    co_return response;
}

ss::future<std::optional<errc>> dedup_stm::wait_for_introducers(
  const std::vector<inflight_ptr>& introducers,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    // One deadline for all of them: the wait is for this request's
    // acknowledgment, which sync_timeout bounds however many it waits on.
    const auto deadline = model::timeout_clock::now() + _sync_timeout();
    for (const auto& introducer : introducers) {
        auto appended = introducer->appended.get_shared_future();
        auto waited = co_await ss::coroutine::as_future(
          as ? ssx::with_timeout_abortable(std::move(appended), deadline, *as)
             : ss::with_timeout(deadline, std::move(appended)));
        const bool unknown = waited.failed();
        if (unknown) {
            waited.ignore_ready_future();
        }
        if (unknown || !waited.get()) {
            // Either the introducer never appended -- its entry is reverted,
            // so nothing in the log holds an identity this request dropped
            // against -- or its fate is still unknown. Fail this request so
            // the producer retries the whole batch; by then it is classified
            // fresh.
            vlog(
              clusterlog.debug,
              "{} dedup request waited on an introducing request that {}, "
              "failing it for retry",
              _raft->ntp(),
              unknown ? "did not resolve in time" : "never appended");
            co_return unknown ? errc::timeout : errc::replication_error;
        }
    }
    co_return std::nullopt;
}

void dedup_stm::settle(
  const inflight_ptr& mine,
  const dedup_request_undo& undo,
  bool appended,
  std::optional<uint64_t> self_key) {
    if (!mine) {
        // Nothing was admitted, so nothing is pending.
        return;
    }
    for (const auto& entry : undo.entries) {
        // Only this request's own registrations: a later request may have
        // written a newer timestamp for the same identity and registered
        // itself in its place.
        if (
          auto it = _pending.find(entry.identity);
          it != _pending.end() && it->second == mine) {
            _pending.erase(it);
        }
    }
    if (!appended) {
        _state.revert_request(undo);
        if (self_key) {
            forget_self_filtered(*self_key);
        }
    }
    mine->appended.set_value(appended);
}

ss::future<> dedup_stm::resolve_after_timeout(
  inflight_ptr mine,
  dedup_request_undo undo,
  std::optional<uint64_t> self_key,
  model::term_id term,
  uint64_t evictions_before) {
    // Whether the batch was appended shows in whether do_apply() sees it.
    // Three cases at this point:
    //  - its key is still remembered: not applied yet. Move the key where
    //    load cannot age it out and wait.
    //  - its key is gone and nothing aged out meanwhile: do_apply() took it,
    //    the batch is in the log.
    //  - its key is gone and keys did age out: unknowable. Treat it as never
    //    appended and release its identities. If it was appended after all,
    //    the producer's retry is admitted beside it -- a duplicate, which is
    //    what a timed-out produce means to a Kafka client anyway -- rather
    //    than acknowledged against a write that might not exist.
    bool appended = false;
    bool watch = false;
    if (self_key && take_self_filtered(*self_key)) {
        watch = _resolving.try_emplace(*self_key, false).second;
    } else if (self_key && _self_filtered_evictions == evictions_before) {
        appended = true;
    }
    if (watch) {
        // If the batch was appended it sits at or below the dirty offset read
        // here: the batcher only appends an item it has not expired, and an
        // expired one is dropped. Once this replica has applied everything
        // up to that offset the batch has applied too, if it ever was.
        const auto fence = _raft->dirty_offset();
        while (!_gate.is_closed()) {
            if (
              co_await wait_no_throw(
                fence, model::timeout_clock::now() + _sync_timeout())) {
                break;
            }
            if (!_raft->is_leader() || _raft->term() != term) {
                // The question is open, and closed another way: a follower
                // does not filter, and indexes whatever commits through
                // do_apply() -- after this key is forgotten below.
                break;
            }
        }
        auto it = _resolving.find(*self_key);
        appended = it->second;
        _resolving.erase(it);
    }
    vlog(
      clusterlog.debug,
      "{} dedup request that timed out resolved as {}",
      _raft->ntp(),
      appended ? "appended" : "never appended");
    settle(mine, undo, appended, self_key);
}

ss::future<iobuf> dedup_stm::take_raft_snapshot(model::offset) {
    // Convergent snapshot: serialize the current state regardless of the
    // target offset. populate() is idempotent (max-timestamp-wins), so a
    // replica installing this snapshot and replaying the remaining log
    // converges to exactly this state; see the log-derived dedup RFC.
    co_return co_await serialize_snapshot(_state, _generation, &_pending);
}

ss::future<bool> dedup_stm::try_restore_snapshot(iobuf buffer) {
    // A snapshot written before the index switched to identity digests
    // (state_snapshot version 0) carries identities this build cannot
    // re-derive digests from, so serde rejects it on compat version. The
    // index is advisory and log-derived, so discarding it and rebuilding
    // from the retained log is a correct -- if slower -- outcome, and a far
    // better one than refusing to start the partition.
    try {
        co_await restore_snapshot(
          _state, _generation, decode_snapshot(std::move(buffer)));
        co_return true;
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
        co_return false;
    }
}

ss::future<> dedup_stm::apply_raft_snapshot(const iobuf& buffer) {
    // One at a time per shard, as at startup: a node rejoining after a long
    // absence installs raft snapshots for every partition it fell behind on,
    // and each restore holds its snapshot's entries twice over while it runs.
    std::optional<ssx::semaphore_units> one_at_a_time;
    if (auto budget = _state.shard_budget(); budget) {
        one_at_a_time = co_await ss::get_units(
          budget->restore_one_at_a_time, 1);
    }
    _state.clear();
    _generation = 0;
    // Whatever this replica filtered is behind the snapshot now; the batches
    // after it must be indexed when they apply. Keys under resolution stay:
    // a snapshot only installs on a replica that fell behind, which is no
    // longer the leader those requests ran on, so they resolve as never
    // appended either way.
    _self_filtered.clear();
    _self_filtered_order.clear();
    if (!buffer.empty()) {
        co_await try_restore_snapshot(buffer.copy());
    }
}

ss::future<raft::local_snapshot_applied>
dedup_stm::apply_local_snapshot(raft::stm_snapshot_header, iobuf&& buffer) {
    // Reporting `no` for a snapshot we discarded is what actually makes the
    // rebuild happen: persisted_stm only calls set_next(next_offset) -- i.e.
    // skips the log the snapshot covered -- when the snapshot was applied.
    // Answering `yes` here would leave the index empty *and* skip past the
    // records it should have been rebuilt from.
    const bool applied = co_await try_restore_snapshot(std::move(buffer));
    _local_snapshot_applied = applied;
    co_return applied ? raft::local_snapshot_applied::yes
                      : raft::local_snapshot_applied::no;
}

ss::future<raft::stm_snapshot>
dedup_stm::take_local_snapshot(ssx::semaphore_units apply_units) {
    // The walk yields, but the apply lock is held throughout, so the offset
    // read after it is the one the entries cover.
    auto buffer = co_await serialize_snapshot(_state, _generation, &_pending);
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
  config::binding<size_t> max_entries,
  config::binding<size_t> max_entries_per_shard)
  : _kvstore(kvstore)
  , _sync_timeout(std::move(sync_timeout))
  , _max_entries(std::move(max_entries))
  , _max_entries_per_shard(std::move(max_entries_per_shard))
  , _shard_budget(
      ss::make_lw_shared<dedup_shard_budget>(
        resolve_shard_limit(_max_entries_per_shard()))) {
    vlog(
      clusterlog.info,
      "Dedup indices on this shard are limited to {} entries in total",
      _shard_budget->max_entries);
    _max_entries.watch([this] { apply_partition_limit(); });
    _max_entries_per_shard.watch([this] { apply_shard_limit(); });
    setup_metrics();
}

void dedup_stm_factory::apply_partition_limit() {
    // One assignment per index on the shard. Nothing is trimmed here: an index
    // above a lowered ceiling shrinks back with its own traffic.
    const auto limit = _max_entries();
    for (auto* filter : _shard_budget->filters) {
        filter->set_max_entries(limit);
    }
    vlog(
      clusterlog.info,
      "Dedup indices on this shard are now limited to {} entries each",
      limit);
}

void dedup_stm_factory::apply_shard_limit() {
    _shard_budget->max_entries = resolve_shard_limit(_max_entries_per_shard());
    vlog(
      clusterlog.info,
      "Dedup indices on this shard are now limited to {} entries in total "
      "({} held)",
      _shard_budget->max_entries,
      _shard_budget->entries);
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
      raft, clusterlog, _kvstore, _sync_timeout, _max_entries(), _shard_budget);
    raft->log()->stm_hookset()->add_stm(stm);
}

} // namespace cluster
