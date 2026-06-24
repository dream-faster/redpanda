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
#include "model/batch_compression.h"
#include "model/record.h"
#include "storage/record_batch_builder.h"

#include <algorithm>

namespace cluster {

dedup_window_filter::dedup_window_filter(std::chrono::milliseconds window)
  : _window(window) {}

bool dedup_window_filter::is_duplicate(const iobuf& key, model::timestamp ts) {
    _max_ts = std::max(_max_ts, ts);
    auto key_bytes = iobuf_to_bytes(key);
    auto it = _map.find(key_bytes);
    if (it != _map.end()) {
        auto stored_ts = it->second;
        auto diff_ms = ts.value() - stored_ts.value();
        if (diff_ms <= _window.count()) {
            return true;
        }
        it->second = ts;
        return false;
    }
    _map.emplace(std::move(key_bytes), ts);
    maybe_evict();
    return false;
}

std::optional<model::record_batch>
dedup_window_filter::filter(model::record_batch batch) {
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
    int32_t kept = 0;
    int32_t idx = 0;
    readable.for_each_record([&](model::record r) {
        if (r.has_key()) {
            model::timestamp ts{base_ts.value() + r.timestamp_delta()};
            if (is_duplicate(r.key(), ts)) {
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
        return batch;
    }
    if (kept == 0) {
        return std::nullopt;
    }

    // Rebuild with surviving records. Preserve the original batch timestamp so
    // retention semantics are unchanged; record_batch_builder defaults to
    // model::timestamp::now() otherwise.
    storage::record_batch_builder builder(
      readable.header().type, readable.header().base_offset);
    builder.set_timestamp(base_ts);
    builder.set_producer_identity(
      readable.header().producer_id, readable.header().producer_epoch);
    if (readable.header().attrs.is_transactional()) {
        builder.set_transactional_type();
    }
    if (readable.header().attrs.is_control()) {
        builder.set_control_type();
    }

    idx = 0;
    readable.for_each_record([&](model::record r) {
        if (keep[static_cast<size_t>(idx)]) {
            builder.add_raw_kw(
              r.share_key_opt(), r.share_value_opt(), std::move(r.headers()));
        }
        ++idx;
    });

    return std::move(builder).build();
}

void dedup_window_filter::populate(const iobuf& key, model::timestamp ts) {
    _max_ts = std::max(_max_ts, ts);
    auto key_bytes = iobuf_to_bytes(key);
    auto [it, inserted] = _map.emplace(key_bytes, ts);
    if (!inserted) {
        if (ts.value() > it->second.value()) {
            it->second = ts;
        }
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

void dedup_window_filter::clear() {
    _map.clear();
    _max_ts = model::timestamp::min();
    _inserts_since_evict = 0;
}

} // namespace cluster
