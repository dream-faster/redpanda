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

#include "bytes/iobuf.h"
#include "cluster/dedup_window_filter.h"
#include "model/batch_builder.h"
#include "model/batch_compression.h"
#include "model/record.h"
#include "storage/record_batch_builder.h"

#include <gtest/gtest.h>

#include <chrono>
#include <optional>
#include <string>
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
    builder.add_raw_kv(iobuf::from(key), iobuf::from(value));
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
model::record_batch
make_null_key_batch(std::string_view value, model::timestamp batch_ts) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset{0});
    builder.set_timestamp(batch_ts);
    builder.add_raw_kv(std::nullopt, iobuf::from(value));
    return std::move(builder).build();
}

// Count records in a batch.
int32_t record_count(const model::record_batch& b) { return b.record_count(); }

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

// Fully-deduplicated records are true state no-ops. In particular, a
// future-timestamp duplicate must not advance the eviction clock because no
// Raft state update is emitted for an all-dropped request.
TEST(DedupWindowFilter, DuplicateDoesNotAdvanceEvictionClock) {
    cluster::dedup_window_filter f(1000ms);

    auto admitted = f.filter(make_batch("k", "v1", ts(1000)));
    ASSERT_TRUE(admitted.has_value());
    EXPECT_EQ(f.max_timestamp(), ts(1000));

    auto duplicate = f.filter(make_batch("k", "v2", ts(1500)));
    EXPECT_FALSE(duplicate.has_value());
    EXPECT_EQ(f.max_timestamp(), ts(1000));
}

TEST(DedupWindowFilter, ReplicatedUpdateCanBeReverted) {
    cluster::dedup_window_filter leader(1000ms);
    auto filtered = leader.filter_with_updates(
      make_multi_batch({{"a", "1"}, {"b", "2"}}, ts(1000)));
    ASSERT_TRUE(filtered.batch.has_value());
    ASSERT_EQ(filtered.admitted.size(), 2);

    cluster::dedup_window_filter follower(1000ms);
    auto before = follower.snapshot();
    auto undo = follower.apply(filtered.admitted, 1000ms);
    EXPECT_EQ(follower.map_size(), leader.map_size());
    EXPECT_FALSE(
      follower.filter(make_batch("a", "duplicate", ts(1500))).has_value());

    follower.revert(undo);
    EXPECT_EQ(follower.snapshot(), before);
}

TEST(DedupWindowFilter, LargeReplicatedUpdateAndEvictionCanBeReverted) {
    constexpr size_t key_count = 10'000;
    cluster::dedup_window_filter follower(1000ms);

    // populate() does not advance the opportunistic insertion counter. These
    // keys are all stale by the time the replicated update below is applied.
    for (size_t i = 0; i < key_count; ++i) {
        follower.populate(
          iobuf::from("old-" + std::to_string(i)), model::timestamp{0});
    }

    chunked_vector<cluster::dedup_index_entry> admitted;
    admitted.reserve(key_count);
    for (size_t i = 0; i < key_count; ++i) {
        admitted.push_back(
          {.key = bytes::from_string("new-" + std::to_string(i)),
           .timestamp = model::timestamp{5000}});
    }

    auto undo = follower.apply(admitted, 1000ms);
    EXPECT_EQ(follower.map_size(), key_count);
    // The undo contains one entry for every inserted key and every stale key
    // removed by the sweep. Building it must remain linear in this count.
    EXPECT_EQ(undo.entries.size(), key_count * 2);

    follower.revert(undo);
    EXPECT_EQ(follower.map_size(), key_count);
    EXPECT_FALSE(
      follower.filter(make_batch("old-0", "duplicate", ts(500))).has_value());
    EXPECT_TRUE(
      follower.filter(make_batch("new-0", "new", ts(500))).has_value());
}

TEST(DedupWindowFilter, RevertingUpdateRestoresWindow) {
    cluster::dedup_window_filter f(1000ms);
    const auto undo = f.apply({}, 2000ms);
    EXPECT_EQ(f.window(), 2000ms);

    f.revert(undo);
    EXPECT_EQ(f.window(), 1000ms);
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

TEST(DedupWindowFilter, PartialFilterPreservesRecordMetadata) {
    cluster::dedup_window_filter f(1000ms);
    ASSERT_TRUE(f.filter(make_batch("a", "seed", ts(1000))).has_value());

    model::batch_builder builder;
    builder.set_batch_timestamp(model::timestamp_type::create_time, ts(1000));
    builder.set_compression(model::compression::lz4);
    builder.add_record(
      model::record({}, 10, 0, iobuf::from("a"), iobuf::from("duplicate"), {}));
    chunked_vector<model::record_header> headers;
    headers.emplace_back(iobuf::from("header"), iobuf::from("value"));
    builder.add_record(model::record(
      {}, 20, 1, iobuf::from("b"), iobuf::from("kept"), std::move(headers)));

    auto filtered = f.filter(std::move(builder).build_sync());
    ASSERT_TRUE(filtered.has_value());
    EXPECT_TRUE(filtered->compressed());
    EXPECT_EQ(filtered->header().first_timestamp, ts(1000));
    EXPECT_EQ(filtered->header().max_timestamp, ts(1020));
    EXPECT_EQ(filtered->header().last_offset_delta, 0);

    auto readable = model::decompress_batch_sync(*filtered);
    readable.for_each_record([](model::record record) {
        EXPECT_EQ(record.timestamp_delta(), 20);
        EXPECT_EQ(record.offset_delta(), 0);
        EXPECT_EQ(record.headers().size(), 1);
    });
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
    auto compressed = model::compress_batch_sync(
      model::compression::lz4, std::move(plain));

    // First produce: kept.
    auto r1 = f.filter(std::move(compressed));
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(record_count(*r1), 1);

    // Second produce of same key within window: dropped.
    auto b2 = make_batch("k", "v2", ts(1500));
    auto r2 = f.filter(std::move(b2));
    EXPECT_FALSE(r2.has_value());
}

// When nothing is filtered, a compressed batch is returned unchanged (still
// compressed) rather than the decompressed temporary.
TEST(DedupWindowFilter, CompressedBatchPassedThroughUnchanged) {
    cluster::dedup_window_filter f(1000ms);

    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset{0});
    builder.set_timestamp(ts(1000));
    builder.add_raw_kv(iobuf::from("k"), iobuf::from("v1"));
    auto plain = std::move(builder).build();
    auto compressed = model::compress_batch_sync(
      model::compression::lz4, std::move(plain));

    auto r = f.filter(std::move(compressed));
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->compressed());
}

// A rebuilt (partially filtered) batch preserves the original batch timestamp
// instead of defaulting to model::timestamp::now().
TEST(DedupWindowFilter, RebuiltBatchPreservesTimestamp) {
    cluster::dedup_window_filter f(1000ms);

    // Seed key "a" so the next batch is partially filtered.
    f.filter(make_batch("a", "v1", ts(5000)));

    // Batch at t=5000 with keys "a" (dup) and "b" (new): "b" survives, forcing
    // a rebuild.
    auto b = make_multi_batch({{"a", "x"}, {"b", "y"}}, ts(5000));
    auto r = f.filter(std::move(b));
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(record_count(*r), 1);
    EXPECT_EQ(r->header().first_timestamp, ts(5000));
}

// evict_expired() removes entries older than the window relative to the most
// recent timestamp seen, bounding memory.
TEST(DedupWindowFilter, EvictExpiredDropsStaleEntries) {
    cluster::dedup_window_filter f(1000ms);

    // Key "a" seen at t=1000.
    f.filter(make_batch("a", "v", ts(1000)));
    EXPECT_EQ(f.map_size(), 1u);

    // Key "b" seen at t=5000 advances the reference clock far past "a"'s
    // window.
    f.filter(make_batch("b", "v", ts(5000)));
    EXPECT_EQ(f.map_size(), 2u);

    // "a" (t=1000) is now 4000ms behind t=5000 (> 1000ms window): evicted.
    // "b" (t=5000) is current: retained.
    f.evict_expired();
    EXPECT_EQ(f.map_size(), 1u);

    // "a" is now unknown, so it is admitted again even within what would have
    // been its original window relative to t=5000.
    auto r = f.filter(make_batch("a", "v2", ts(5200)));
    EXPECT_TRUE(r.has_value());
}
