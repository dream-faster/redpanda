// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/dedup_window_filter.h"

#include "bytes/bytes.h"
#include "model/batch_builder.h"
#include "model/batch_compression.h"
#include "model/record.h"

#include <algorithm>
#include <ranges>

namespace cluster {

dedup_window_filter::dedup_window_filter(std::chrono::milliseconds window)
  : _window(window) {}

bool dedup_window_filter::is_duplicate(
  const iobuf& key, model::timestamp ts, dedup_request_undo* undo) {
    auto key_bytes = iobuf_to_bytes(key);
    auto it = _map.find(key_bytes);
    if (it != _map.end()) {
        auto stored_ts = it->second;
        auto diff_ms = ts.value() - stored_ts.value();
        if (diff_ms <= _window.count()) {
            return true;
        }
        _max_ts = std::max(_max_ts, ts);
        it->second = ts;
        if (undo) {
            undo->entries.push_back(
              {.key = std::move(key_bytes),
               .applied_timestamp = ts,
               .previous_timestamp = stored_ts});
        }
        return false;
    }
    _max_ts = std::max(_max_ts, ts);
    _map.emplace(key_bytes, ts);
    if (undo) {
        undo->entries.push_back(
          {.key = std::move(key_bytes),
           .applied_timestamp = ts,
           .previous_timestamp = std::nullopt});
    }
    maybe_evict();
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
    const model::record_batch& readable = decompressed ? *decompressed : batch;

    const auto base_ts = readable.header().first_timestamp;
    const int32_t total = readable.record_count();

    // First pass: identify which records survive.
    std::vector<bool> keep(static_cast<size_t>(total), true);
    dedup_request_undo undo;
    undo.entries.reserve(static_cast<size_t>(total));
    int32_t kept = 0;
    int32_t idx = 0;
    readable.for_each_record([&](model::record r) {
        if (r.has_key()) {
            model::timestamp ts{base_ts.value() + r.timestamp_delta()};
            if (is_duplicate(r.key(), ts, &undo)) {
                keep[static_cast<size_t>(idx)] = false;
            } else {
                ++kept;
            }
        } else {
            ++kept;
        }
        ++idx;
    });

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
    builder.set_batch_type(readable.header().type);
    builder.set_base_offset(readable.header().base_offset);
    builder.set_batch_timestamp(
      readable.header().attrs.timestamp_type(), base_ts);
    builder.set_compression(batch.header().attrs.compression());
    builder.set_producer_id(readable.header().producer_id);
    builder.set_producer_epoch(readable.header().producer_epoch);
    builder.set_base_sequence(readable.header().base_sequence);
    if (readable.header().attrs.is_transactional()) {
        builder.set_transactional();
    }
    if (readable.header().attrs.is_control()) {
        builder.set_control();
    }

    idx = 0;
    int32_t output_offset_delta = 0;
    readable.for_each_record([&](model::record r) {
        if (keep[static_cast<size_t>(idx)]) {
            builder.add_record(
              model::record(
                r.attributes(),
                r.timestamp_delta(),
                output_offset_delta++,
                r.share_key_opt(),
                r.share_value_opt(),
                std::move(r.headers())));
        }
        ++idx;
    });

    return {.batch = std::move(builder).build_sync(), .undo = std::move(undo)};
}

void dedup_window_filter::revert_request(const dedup_request_undo& undo) {
    // Reverse order so multiple mutations of the same key within one request
    // unwind correctly: the last entry restores the state the previous entry
    // wrote, and the first entry restores the pre-request state.
    for (const auto& entry : undo.entries | std::ranges::views::reverse) {
        auto it = _map.find(entry.key);
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
    _max_ts = std::max(_max_ts, ts);
    auto key_bytes = iobuf_to_bytes(key);
    auto [it, inserted] = _map.emplace(key_bytes, ts);
    if (inserted) {
        maybe_evict();
    } else if (ts.value() > it->second.value()) {
        it->second = ts;
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
    if (++_inserts_since_evict < evict_after_inserts) {
        return;
    }
    _inserts_since_evict = 0;
    evict_expired();
}

dedup_index_snapshot dedup_window_filter::snapshot() const {
    dedup_index_snapshot result{
      .max_timestamp = _max_ts, .inserts_since_evict = _inserts_since_evict};
    result.entries.reserve(_map.size());
    for (const auto& [key, timestamp] : _map) {
        result.entries.push_back({key, timestamp});
    }
    return result;
}

void dedup_window_filter::restore(const dedup_index_snapshot& snapshot) {
    _map.clear();
    _map.reserve(snapshot.entries.size());
    for (const auto& entry : snapshot.entries) {
        _map.emplace(entry.key, entry.timestamp);
    }
    _max_ts = snapshot.max_timestamp;
    _inserts_since_evict = snapshot.inserts_since_evict;
}

void dedup_window_filter::clear() {
    _map.clear();
    _max_ts = model::timestamp::min();
    _inserts_since_evict = 0;
}

} // namespace cluster
