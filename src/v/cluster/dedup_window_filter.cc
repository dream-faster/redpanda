// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/dedup_window_filter.h"

#include "hashing/xx.h"
#include "model/batch_builder.h"
#include "model/batch_compression.h"
#include "model/record.h"

#include <seastar/core/lowres_clock.hh>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <ranges>

namespace cluster {

dedup_identity_lookup dedup_identity_for_record(
  model::record& r,
  const std::optional<ss::sstring>& key_header,
  const iobuf** out) {
    if (key_header) {
        for (auto& h : r.headers()) {
            if (h.key() == std::string_view{*key_header}) {
                *out = &h.value();
                return dedup_identity_lookup::identity;
            }
        }
        return dedup_identity_lookup::missing_header;
    }
    if (!r.has_key()) {
        return dedup_identity_lookup::no_identity;
    }
    *out = &r.key();
    return dedup_identity_lookup::identity;
}

dedup_identity_digest dedup_digest_of(const iobuf& identity) {
    // Arbitrary distinct constants; they only need to differ from each other
    // and stay fixed, since the digest is persisted in snapshots.
    incremental_xxhash64 hi{0x9E3779B97F4A7C15ULL};
    incremental_xxhash64 lo{0xC2B2AE3D27D4EB4FULL};
    for (const auto& frag : identity) {
        hi.update(frag.get(), frag.size());
        lo.update(frag.get(), frag.size());
    }
    return {.hi = hi.digest(), .lo = lo.digest()};
}

dedup_window_filter::dedup_window_filter(
  std::chrono::milliseconds window, size_t max_entries)
  : _window(window)
  , _max_entries(max_entries) {}

namespace {

model::timestamp broker_now() {
    return model::timestamp{
      std::chrono::duration_cast<std::chrono::milliseconds>(
        ss::lowres_system_clock::now().time_since_epoch())
        .count()};
}

} // namespace

model::timestamp
dedup_window_filter::clamp_to_broker_time(model::timestamp ts) {
    // Client CreateTime is untrusted input. Redpanda admits records up to
    // log_message_timestamp_after_max_ms (one hour by default) ahead of the
    // broker, and _max_ts is a running maximum over those values, so a single
    // future-dated record would push the eviction cutoff (_max_ts - _window)
    // past every entry in the index. The next sweep would then empty it, and
    // dedup would stay off for the partition until the generation is bumped.
    //
    // Clamping is the identity for any record older than the broker's clock --
    // which is every record seen during log replay -- so the index remains a
    // deterministic function of the log rather than of replay timing. See the
    // log-derived dedup RFC, boundary B5.
    const auto now = broker_now();
    if (ts > now) {
        ++_skew_clamped_records;
        return now;
    }
    return ts;
}

bool dedup_window_filter::is_duplicate(
  dedup_identity_digest identity,
  model::timestamp ts,
  dedup_request_undo* undo) {
    auto it = _map.find(identity);
    if (it != _map.end()) {
        auto stored_ts = it->second;
        auto diff_ms = ts.value() - stored_ts.value();
        if (diff_ms >= -_window.count() && diff_ms <= _window.count()) {
            ++_dropped_records;
            return true;
        }
        _max_ts = std::max(_max_ts, ts);
        // Idempotent max-wins, matching populate(): never let a speculative,
        // possibly out-of-order classification regress the stored timestamp
        // below what a later deterministic apply() would compute for the
        // same identity.
        if (ts.value() > stored_ts.value()) {
            it->second = ts;
        }
        if (undo) {
            undo->entries.push_back(
              {.identity = identity,
               .applied_timestamp = it->second,
               .previous_timestamp = stored_ts});
        }
        return false;
    }
    _max_ts = std::max(_max_ts, ts);
    // Count the attempted new identity even when the index is full. This keeps
    // eviction sweeps running at an amortized O(1) rate while saturated, so
    // advancing timestamps eventually reclaim expired entries and reopen
    // capacity without scanning the full map for every admitted record.
    maybe_evict();
    if (_map.size() >= _max_entries) {
        // Best-effort, fail-open behavior: the record is admitted but this
        // identity is not remembered. Existing indexed identities retain their
        // dedup coverage and the map never exceeds its hard ceiling.
        ++_unindexed_records;
        return false;
    }
    _map.emplace(identity, ts);
    if (undo) {
        undo->entries.push_back(
          {.identity = identity,
           .applied_timestamp = ts,
           .previous_timestamp = std::nullopt});
    }
    return false;
}

std::optional<model::record_batch>
dedup_window_filter::filter(model::record_batch batch) {
    return filter_request(std::move(batch)).batch;
}

dedup_filter_result
dedup_window_filter::filter_request(model::record_batch batch) {
    // Decompress into a temporary so the original (possibly compressed) batch
    // can be returned untouched when nothing is filtered.
    std::optional<model::record_batch> decompressed;
    if (batch.compressed()) {
        decompressed = model::decompress_batch_sync(batch);
    }
    model::record_batch& readable = decompressed ? *decompressed : batch;

    // Copied, not referenced: the batch header is read again after the
    // iterator below has taken a share of the records.
    const auto header = readable.header();
    const auto base_ts = header.first_timestamp;
    const int32_t total = readable.record_count();

    // Walk the records by sharing them rather than copying.
    // record_batch::for_each_record() yields records through
    // record_batch_copy_iterator, which deep-copies every key, value, and
    // header of every record; record_batch_iterator refcounts them instead.
    // That matters because this runs on every plain produce.
    //
    // Classification is deferred until the whole batch has been inspected:
    // in header mode one record missing the configured header rejects the
    // entire request, and the index must not have been mutated by then.
    // Digesting each identity up front is what makes deferring cheap -- what
    // has to survive the pass is 32 bytes per record, not the records
    // themselves, whose iobufs would otherwise pin a multiple of the batch
    // size for the duration of the call.
    struct classified {
        dedup_identity_digest identity;
        model::timestamp timestamp;
        bool has_identity{false};
        bool dropped{false};
    };
    chunked_vector<classified> records;
    records.reserve(static_cast<size_t>(total));

    {
        auto it = model::record_batch_iterator::create(readable.share());
        while (it.has_next()) {
            auto r = it.next();
            const iobuf* identity = nullptr;
            const auto lookup = dedup_identity_for_record(
              r, _key_header, &identity);
            if (lookup == dedup_identity_lookup::missing_header) {
                // Nothing has been mutated yet, so the whole request can be
                // rejected without unwinding anything.
                return {.missing_required_header = true};
            }
            classified c{
              .timestamp = clamp_to_broker_time(
                model::timestamp{base_ts.value() + r.timestamp_delta()})};
            if (lookup == dedup_identity_lookup::identity) {
                c.identity = dedup_digest_of(*identity);
                c.has_identity = true;
            }
            records.push_back(c);
        }
    }

    // Classify in record order, so two records sharing an identity within
    // one request resolve first-wins the same way they would across
    // requests.
    dedup_request_undo undo;
    undo.entries.reserve(static_cast<size_t>(total));
    int32_t kept = 0;
    for (auto& c : records) {
        c.dropped = c.has_identity
                    && is_duplicate(c.identity, c.timestamp, &undo);
        if (!c.dropped) {
            ++kept;
        }
    }

    if (kept == total) {
        // Nothing filtered: return the original batch unchanged (preserving
        // its compression, attrs, and timestamps).
        return {.batch = std::move(batch), .undo = std::move(undo)};
    }
    if (kept == 0) {
        return {.batch = std::nullopt, .undo = std::move(undo)};
    }

    // Rebuild with surviving records while preserving batch and record
    // metadata. Offset deltas are made contiguous because this is a produce
    // batch, not a compacted batch with intentional offset gaps.
    model::batch_builder builder;
    builder.set_batch_type(header.type);
    builder.set_base_offset(header.base_offset);
    builder.set_batch_timestamp(header.attrs.timestamp_type(), base_ts);
    // From the original batch, not `header`: `readable` may be the
    // decompressed temporary, whose attrs no longer name the compression the
    // rebuilt batch should be written back with.
    builder.set_compression(batch.header().attrs.compression());
    builder.set_producer_id(header.producer_id);
    builder.set_producer_epoch(header.producer_epoch);
    builder.set_base_sequence(header.base_sequence);
    if (header.attrs.is_transactional()) {
        builder.set_transactional();
    }
    if (header.attrs.is_control()) {
        builder.set_control();
    }

    // Second sharing pass, taken only when something was actually dropped.
    // Re-parsing is cheap next to holding every record's iobufs alive across
    // the classification above, and the fast path above never reaches here.
    int32_t output_offset_delta = 0;
    size_t idx = 0;
    auto rebuild = model::record_batch_iterator::create(readable.share());
    while (rebuild.has_next()) {
        auto r = rebuild.next();
        if (records[idx++].dropped) {
            continue;
        }
        builder.add_record(
          model::record(
            r.attributes(),
            r.timestamp_delta(),
            output_offset_delta++,
            r.share_key_opt(),
            r.share_value_opt(),
            std::move(r.headers())));
    }

    return {.batch = std::move(builder).build_sync(), .undo = std::move(undo)};
}

void dedup_window_filter::revert_request(const dedup_request_undo& undo) {
    // Reverse order so multiple mutations of the same key within one request
    // unwind correctly: the last entry restores the state the previous entry
    // wrote, and the first entry restores the pre-request state.
    for (const auto& entry : undo.entries | std::ranges::views::reverse) {
        auto it = _map.find(entry.identity);
        if (it == _map.end() || it->second != entry.applied_timestamp) {
            // A later request overwrote (or evicted) this key; its state wins.
            continue;
        }
        if (entry.previous_timestamp) {
            it->second = *entry.previous_timestamp;
        } else {
            _map.erase(it);
        }
    }
}

void dedup_window_filter::populate(const iobuf& key, model::timestamp ts) {
    ts = clamp_to_broker_time(ts);
    _max_ts = std::max(_max_ts, ts);
    auto identity = dedup_digest_of(key);

    // Followers apply the same fail-open capacity rule as the leader does in
    // is_duplicate(); the two must agree or replicas diverge at the ceiling.
    //
    // Below the ceiling one try_emplace settles both the new and the existing
    // identity, so the common case on this -- the busier -- replay path costs
    // a single probe. The size check comes first so the insert can reach the
    // limit but never exceed it, which a try_emplace-then-check would.
    if (_map.size() < _max_entries) {
        auto [it, inserted] = _map.try_emplace(identity, ts);
        if (inserted) {
            // May erase arbitrary entries, invalidating it; nothing below
            // touches it.
            maybe_evict();
        } else if (ts.value() > it->second.value()) {
            it->second = ts;
        }
        return;
    }

    // At the ceiling a new identity cannot be indexed until a sweep frees
    // room, so look up first and fail open if it does not.
    auto it = _map.find(identity);
    if (it != _map.end()) {
        if (ts.value() > it->second.value()) {
            it->second = ts;
        }
        return;
    }
    maybe_evict();
    if (_map.size() < _max_entries) {
        _map.emplace(identity, ts);
    } else {
        ++_unindexed_records;
    }
}

void dedup_window_filter::evict_expired() {
    // An entry is only meaningful while a future record could still be within
    // _window of it. Once _max_ts has advanced beyond the window, the entry can
    // never cause a drop again (the next lookup would treat it as expired), so
    // it is safe to remove.
    const int64_t cutoff = _max_ts.value() - _window.count();
    std::erase_if(
      _map, [cutoff](const auto& kv) { return kv.second.value() < cutoff; });
}

void dedup_window_filter::maybe_evict() {
    // evict_expired() scans the whole map, so a fixed interval makes the
    // amortized per-attempt cost grow linearly with the index (at a million
    // entries and a 10k interval, every attempt pays for ~100 scanned
    // entries). Scaling the interval with the map keeps that cost flat: a
    // sweep of N entries happens at most once per N/evict_size_divisor
    // attempts, i.e. evict_size_divisor scanned entries per attempt.
    //
    // The cost is slack, and it is bounded in attempts rather than in
    // time: a sweep waits for map_size/evict_size_divisor new identities, so
    // under a sustained arrival rate the map carries about that fraction above
    // its steady-state working set. A partition that goes idle, or whose
    // traffic turns into mostly-duplicates, stops attempting new identities
    // and so stops sweeping -- it holds its expired entries until traffic
    // resumes.
    // That was already true of the flat 10k interval; scaling makes the wait
    // proportionally longer at large indexes. Expired entries can never
    // change a dedup decision, only occupy space, and both snapshot paths
    // sweep unconditionally, so a snapshot never carries the slack.
    const auto interval = std::max(
      min_evict_interval, _map.size() / evict_size_divisor);
    if (++_inserts_since_evict < interval) {
        return;
    }
    _inserts_since_evict = 0;
    evict_expired();
}

dedup_index_snapshot dedup_window_filter::snapshot() const {
    dedup_index_snapshot result{
      .max_timestamp = _max_ts, .inserts_since_evict = _inserts_since_evict};
    result.entries.reserve(_map.size());
    for (const auto& [identity, timestamp] : _map) {
        result.entries.push_back({identity, timestamp});
    }
    return result;
}

void dedup_window_filter::restore(dedup_index_snapshot snapshot) {
    _map.clear();
    const auto restore_count = std::min(snapshot.entries.size(), _max_entries);
    if (restore_count < snapshot.entries.size()) {
        // Keep the most recent identities. The index is a dense hash map whose
        // iteration is insertion order, so snapshot() emits roughly oldest
        // first; taking a prefix would drop exactly the entries most likely to
        // still match an incoming duplicate. nth_element is O(n) and only runs
        // for a snapshot larger than this node's ceiling.
        std::nth_element(
          snapshot.entries.begin(),
          snapshot.entries.begin()
            + static_cast<std::ptrdiff_t>(restore_count),
          snapshot.entries.end(),
          [](const dedup_index_entry& a, const dedup_index_entry& b) {
              return a.timestamp > b.timestamp;
          });
        _truncated_entries += snapshot.entries.size() - restore_count;
    }
    _map.reserve(restore_count);
    size_t restored = 0;
    for (const auto& entry : snapshot.entries) {
        if (restored++ >= restore_count) {
            break;
        }
        _map.emplace(entry.identity, entry.timestamp);
    }
    // A snapshot written by a broker without the clamp can carry a poisoned
    // watermark; clamping here lets such a partition heal on restart rather
    // than needing a dedup_generation bump. Not counted as record skew.
    _max_ts = std::min(snapshot.max_timestamp, broker_now());
    _inserts_since_evict = snapshot.inserts_since_evict;
}

void dedup_window_filter::clear() {
    _map.clear();
    _max_ts = model::timestamp::min();
    _inserts_since_evict = 0;
}

} // namespace cluster
