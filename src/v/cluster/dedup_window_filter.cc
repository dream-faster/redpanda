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

namespace cluster {

dedup_window_filter::dedup_window_filter(std::chrono::milliseconds window)
  : _window(window) {}

bool dedup_window_filter::is_duplicate(
  const iobuf& key, model::timestamp ts) {
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
    return false;
}

std::optional<model::record_batch>
dedup_window_filter::filter(model::record_batch batch) {
    // Decompress so we can read record keys.
    bool was_compressed = batch.compressed();
    if (was_compressed) {
        batch = model::decompress_batch_sync(batch);
    }

    const auto base_ts = batch.header().first_timestamp;
    const int32_t total = batch.record_count();

    // First pass: identify which records survive.
    std::vector<bool> keep(static_cast<size_t>(total), true);
    int32_t kept = 0;
    int32_t idx = 0;
    batch.for_each_record([&](model::record r) {
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
        return batch;
    }
    if (kept == 0) {
        return std::nullopt;
    }

    // Rebuild with surviving records.
    storage::record_batch_builder builder(
      batch.header().type, batch.header().base_offset);
    builder.set_producer_identity(
      batch.header().producer_id, batch.header().producer_epoch);
    if (batch.header().attrs.is_transactional()) {
        builder.set_transactional_type();
    }
    if (batch.header().attrs.is_control()) {
        builder.set_control_type();
    }

    idx = 0;
    batch.for_each_record([&](model::record r) {
        if (keep[static_cast<size_t>(idx)]) {
            builder.add_raw_kw(
              r.share_key_opt(),
              r.share_value_opt(),
              std::move(r.headers()));
        }
        ++idx;
    });

    return std::move(builder).build();
}

void dedup_window_filter::populate(const iobuf& key, model::timestamp ts) {
    auto key_bytes = iobuf_to_bytes(key);
    auto [it, inserted] = _map.emplace(key_bytes, ts);
    if (!inserted) {
        if (ts.value() > it->second.value()) {
            it->second = ts;
        }
    }
}

void dedup_window_filter::clear() { _map.clear(); }

} // namespace cluster
