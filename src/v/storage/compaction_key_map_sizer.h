/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include <algorithm>
#include <cstddef>
#include <optional>

namespace storage {

/// \brief Picks the size of a shard's shared compaction key-offset map before
/// each housekeeping scan.
///
/// The map is sized between `min_bytes` and `max_bytes` (the reservation,
/// `storage_compaction_key_map_memory`) by how full it got during the scan
/// that just ended:
///
/// - not yet allocated: start at `min_bytes`.
/// - filled up: double. Compaction stays correct with a full map, since it
///   falls back to indexing segments in chunks, but takes more passes.
/// - compaction ran and used under a quarter of it: halve.
/// - no compaction ran: keep the size, so an idle shard doesn't shrink a map
///   its next compaction will need.
struct compaction_key_map_sizer {
    size_t min_bytes;
    size_t max_bytes;

    /// \param current_bytes the map's size, 0 when it isn't allocated yet.
    /// \param capacity_keys the number of keys the map holds at that size.
    /// \param peak_keys the most keys a compaction put in the map during the
    ///        last scan, or nullopt when no compaction used it.
    size_t next_size(
      size_t current_bytes,
      size_t capacity_keys,
      std::optional<size_t> peak_keys) const {
        const auto floor = std::min(min_bytes, max_bytes);
        if (current_bytes == 0) {
            return floor;
        }
        if (!peak_keys.has_value()) {
            return std::clamp(current_bytes, floor, max_bytes);
        }
        if (*peak_keys >= capacity_keys) {
            return std::clamp(current_bytes * 2, floor, max_bytes);
        }
        if (*peak_keys * 4 < capacity_keys) {
            return std::clamp(current_bytes / 2, floor, max_bytes);
        }
        return std::clamp(current_bytes, floor, max_bytes);
    }
};

} // namespace storage
