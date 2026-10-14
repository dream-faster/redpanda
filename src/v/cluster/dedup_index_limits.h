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

#include <cstddef>

namespace cluster {

// Default and maximum for `dedup_max_entries_per_partition`. In constants
// here such that both the cluster property (config/configuration.cc) and the
// filter's own fallback default (cluster/dedup_window_filter.h) can refer to
// them. config cannot include dedup_window_filter.h directly: that header is
// part of //src/v/cluster, which already depends on //src/v/config.
inline constexpr size_t DEFAULT_DEDUP_MAX_ENTRIES_PER_PARTITION = 1'000'000;
inline constexpr size_t MAX_DEDUP_MAX_ENTRIES_PER_PARTITION = 10'000'000;

// `dedup_max_entries_per_shard`: 0 derives the limit from the shard's memory.
// Partitions are placed on shards by count, not by index size, so a shard can
// host several saturated indices at once; the per-partition ceiling alone does
// not bound what one shard holds.
inline constexpr size_t DEFAULT_DEDUP_MAX_ENTRIES_PER_SHARD = 0;
// Share of shard memory the derived limit allows the dedup indices to use.
inline constexpr size_t DEDUP_SHARD_MEMORY_PERCENT = 15;
// Memory one entry costs while a snapshot of it is in flight: the index's
// 16-byte entry and its lookup slot (22-26 bytes, depending on how full the
// lookup table's segments are) plus its 20-byte serialized form.
inline constexpr size_t DEDUP_SHARD_BYTES_PER_ENTRY = 48;

} // namespace cluster
