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

#include "base/units.h"
#include "storage/compaction_key_map_sizer.h"

#include <gtest/gtest.h>

using storage::compaction_key_map_sizer;

namespace {

constexpr compaction_key_map_sizer sizer{
  .min_bytes = 16_MiB, .max_bytes = 128_MiB};

// The capacity, in keys, of a map of the given size. The real map holds a
// few keys fewer per byte; only the ratio to peak_keys matters here.
constexpr size_t keys(size_t bytes) { return bytes / 40; }

} // namespace

TEST(CompactionKeyMapSizer, StartsAtMinimum) {
    EXPECT_EQ(sizer.next_size(0, 0, std::nullopt), 16_MiB);
    EXPECT_EQ(sizer.next_size(0, 0, 0), 16_MiB);
}

TEST(CompactionKeyMapSizer, MinimumAboveMaximumUsesMaximum) {
    // Setting the minimum to the reservation or above restores the old
    // behaviour: the map is allocated at full size and stays there.
    constexpr compaction_key_map_sizer fixed{
      .min_bytes = 1_GiB, .max_bytes = 128_MiB};
    EXPECT_EQ(fixed.next_size(0, 0, std::nullopt), 128_MiB);
    EXPECT_EQ(fixed.next_size(128_MiB, keys(128_MiB), 0), 128_MiB);
    EXPECT_EQ(fixed.next_size(128_MiB, keys(128_MiB), keys(128_MiB)), 128_MiB);
}

TEST(CompactionKeyMapSizer, GrowsWhenFullUpToMaximum) {
    EXPECT_EQ(sizer.next_size(16_MiB, keys(16_MiB), keys(16_MiB)), 32_MiB);
    EXPECT_EQ(sizer.next_size(32_MiB, keys(32_MiB), keys(32_MiB)), 64_MiB);
    EXPECT_EQ(sizer.next_size(64_MiB, keys(64_MiB), keys(64_MiB)), 128_MiB);
    EXPECT_EQ(sizer.next_size(128_MiB, keys(128_MiB), keys(128_MiB)), 128_MiB);
}

TEST(CompactionKeyMapSizer, ShrinksWhenMostlyEmptyDownToMinimum) {
    EXPECT_EQ(sizer.next_size(128_MiB, keys(128_MiB), 1000), 64_MiB);
    EXPECT_EQ(sizer.next_size(64_MiB, keys(64_MiB), 0), 32_MiB);
    EXPECT_EQ(sizer.next_size(32_MiB, keys(32_MiB), 0), 16_MiB);
    EXPECT_EQ(sizer.next_size(16_MiB, keys(16_MiB), 0), 16_MiB);
}

TEST(CompactionKeyMapSizer, KeepsSizeInBetween) {
    EXPECT_EQ(sizer.next_size(64_MiB, keys(64_MiB), keys(64_MiB) / 2), 64_MiB);
    EXPECT_EQ(sizer.next_size(64_MiB, 1000, 250), 64_MiB);
    EXPECT_EQ(sizer.next_size(64_MiB, 1000, 249), 32_MiB);
    EXPECT_EQ(sizer.next_size(64_MiB, keys(64_MiB), keys(64_MiB) - 1), 64_MiB);
}

TEST(CompactionKeyMapSizer, KeepsSizeWhenNothingCompacted) {
    EXPECT_EQ(sizer.next_size(128_MiB, keys(128_MiB), std::nullopt), 128_MiB);
    EXPECT_EQ(sizer.next_size(16_MiB, keys(16_MiB), std::nullopt), 16_MiB);
}

TEST(CompactionKeyMapSizer, ClampsAfterBoundsChange) {
    // A map allocated under a larger reservation is brought back inside the
    // bounds even when it would otherwise keep its size.
    EXPECT_EQ(sizer.next_size(512_MiB, keys(512_MiB), std::nullopt), 128_MiB);
    EXPECT_EQ(sizer.next_size(4_MiB, keys(4_MiB), std::nullopt), 16_MiB);
}
