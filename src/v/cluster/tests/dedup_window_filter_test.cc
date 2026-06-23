// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Unit tests for cluster::dedup_window_filter.
//
// These tests drive the filter directly without any Raft or partition
// dependencies. Each test constructs small record batches in memory and
// verifies the first-wins dedup semantics.

#include "cluster/dedup_window_filter.h"

#include "bytes/iobuf.h"
#include "model/batch_compression.h"
#include "model/record.h"
#include "storage/record_batch_builder.h"

#include <gtest/gtest.h>

#include <chrono>
#include <optional>
#include <string_view>

using namespace std::chrono_literals;

namespace {

model::timestamp ts(int64_t ms) { return model::timestamp{ms}; }

// Build a single-record batch with an explicit key, value, and timestamp.
model::record_batch make_batch(
  std::string_view key,
  std::string_view value,
  model::timestamp batch_ts,
  model::record_batch_type type = model::record_batch_type::raft_data) {
    storage::record_batch_builder builder(type, model::offset{0});
    builder.set_timestamp(batch_ts);
    builder.add_raw_kv(
      iobuf::from(key), iobuf::from(value));
    return std::move(builder).build();
}

// Build a multi-record batch. Each (key, value) pair gets the same batch
// timestamp (timestamp_delta=0 for all records in this helper).
model::record_batch make_multi_batch(
  std::initializer_list<std::pair<std::string_view, std::string_view>> records,
  model::timestamp batch_ts) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset{0});
    builder.set_timestamp(batch_ts);
    for (const auto& [k, v] : records) {
        builder.add_raw_kv(iobuf::from(k), iobuf::from(v));
    }
    return std::move(builder).build();
}

// Build a batch with a null (absent) key.
model::record_batch make_null_key_batch(
  std::string_view value, model::timestamp batch_ts) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset{0});
    builder.set_timestamp(batch_ts);
    builder.add_raw_kv(std::nullopt, iobuf::from(value));
    return std::move(builder).build();
}

// Count records in a batch.
int32_t record_count(const model::record_batch& b) {
    return b.record_count();
}

} // namespace

// First record for a key passes through; a duplicate within the window is
// dropped; the same key after the window expires is kept again.
TEST(DedupWindowFilter, FirstWinsWithinWindow) {
    cluster::dedup_window_filter f(1000ms);

    // First record with key "k": kept.
    auto b1 = make_batch("k", "v1", ts(1000));
    auto r1 = f.filter(std::move(b1));
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(record_count(*r1), 1);

    // Duplicate within window (delta = 500ms < 1000ms): dropped.
    auto b2 = make_batch("k", "v2", ts(1500));
    auto r2 = f.filter(std::move(b2));
    EXPECT_FALSE(r2.has_value());

    // Same key after window expires (delta = 1100ms > 1000ms): kept.
    auto b3 = make_batch("k", "v3", ts(2100));
    auto r3 = f.filter(std::move(b3));
    ASSERT_TRUE(r3.has_value());
    EXPECT_EQ(record_count(*r3), 1);
}

// Records with null keys are always kept (cannot dedup without a key).
TEST(DedupWindowFilter, NullKeyAlwaysKept) {
    cluster::dedup_window_filter f(1000ms);

    auto b1 = make_null_key_batch("v1", ts(1000));
    auto r1 = f.filter(std::move(b1));
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(record_count(*r1), 1);

    // Second null-key record: also kept (no key to dedup on).
    auto b2 = make_null_key_batch("v2", ts(1001));
    auto r2 = f.filter(std::move(b2));
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(record_count(*r2), 1);
}

// A multi-record batch with some duplicates produces a rebuilt batch with only
// the surviving records.
TEST(DedupWindowFilter, MultiRecordPartialFilter) {
    cluster::dedup_window_filter f(1000ms);

    // Seed key "a" at t=1000.
    auto b1 = make_batch("a", "v1", ts(1000));
    auto r1 = f.filter(std::move(b1));
    ASSERT_TRUE(r1.has_value());

    // Batch with records for keys "a" (dup), "b" (new), "a" (dup again), "c".
    // Only "b" and "c" should survive.
    auto b2 = make_multi_batch(
      {{"a", "x"}, {"b", "y"}, {"a", "z"}, {"c", "w"}}, ts(1200));
    auto r2 = f.filter(std::move(b2));
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(record_count(*r2), 2);
}

// When every record in a batch is a duplicate, filter returns nullopt.
TEST(DedupWindowFilter, AllDuplicatesReturnsNullopt) {
    cluster::dedup_window_filter f(1000ms);

    auto b1 = make_batch("k", "v1", ts(1000));
    f.filter(std::move(b1));

    auto b2 = make_batch("k", "v2", ts(1100));
    auto r2 = f.filter(std::move(b2));
    EXPECT_FALSE(r2.has_value());
}

// Different keys do not interfere with each other.
TEST(DedupWindowFilter, IndependentKeys) {
    cluster::dedup_window_filter f(1000ms);

    auto ba = make_batch("a", "1", ts(1000));
    auto rb1 = f.filter(std::move(ba));
    ASSERT_TRUE(rb1.has_value());

    auto bb = make_batch("b", "2", ts(1000));
    auto rb2 = f.filter(std::move(bb));
    ASSERT_TRUE(rb2.has_value());

    // Dup of "a" within window.
    auto ba2 = make_batch("a", "3", ts(1500));
    auto rb3 = f.filter(std::move(ba2));
    EXPECT_FALSE(rb3.has_value());

    // "b" within window.
    auto bb2 = make_batch("b", "4", ts(1500));
    auto rb4 = f.filter(std::move(bb2));
    EXPECT_FALSE(rb4.has_value());
}

// clear() resets the map so subsequent records are treated as fresh.
TEST(DedupWindowFilter, ClearResetsState) {
    cluster::dedup_window_filter f(1000ms);

    auto b1 = make_batch("k", "v1", ts(1000));
    f.filter(std::move(b1));

    f.clear();

    // After clear, the same key within what would have been the window passes.
    auto b2 = make_batch("k", "v2", ts(1500));
    auto r2 = f.filter(std::move(b2));
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(record_count(*r2), 1);
}

// populate() seeds the map so the filter correctly deduplicates on first use.
TEST(DedupWindowFilter, PopulateSeedsMap) {
    cluster::dedup_window_filter f(1000ms);

    // Simulate log-tail rebuild: key "k" was last seen at t=1000.
    f.populate(iobuf::from("k"), ts(1000));

    // A new record at t=1500 (within window) is dropped.
    auto b = make_batch("k", "v", ts(1500));
    auto r = f.filter(std::move(b));
    EXPECT_FALSE(r.has_value());
}

// A compressed batch is correctly decompressed before key inspection.
TEST(DedupWindowFilter, CompressedBatch) {
    cluster::dedup_window_filter f(1000ms);

    // Build and compress a batch.
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset{0});
    builder.set_timestamp(ts(1000));
    builder.add_raw_kv(iobuf::from("k"), iobuf::from("v1"));
    auto plain = std::move(builder).build();
    auto compressed = model::compress_batch_sync(model::compression::lz4, std::move(plain));

    // First produce: kept.
    auto r1 = f.filter(std::move(compressed));
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(record_count(*r1), 1);

    // Second produce of same key within window: dropped.
    auto b2 = make_batch("k", "v2", ts(1500));
    auto r2 = f.filter(std::move(b2));
    EXPECT_FALSE(r2.has_value());
}
