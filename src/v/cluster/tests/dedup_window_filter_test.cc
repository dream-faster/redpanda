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
#include "bytes/iobuf_parser.h"
#include "cluster/dedup_window_filter.h"
#include "model/batch_builder.h"
#include "model/batch_compression.h"
#include "model/record.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/lowres_clock.hh>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <chrono>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using namespace std::chrono_literals;

namespace {

model::timestamp ts(int64_t ms) { return model::timestamp{ms}; }

// The same clock the filter clamps against. Tests that exercise clamping have
// to be anchored to it; the fixed ts() values above all sit in 1970, far
// behind broker time, so the clamp is the identity for every other test.
model::timestamp broker_now() {
    return model::timestamp{
      std::chrono::duration_cast<std::chrono::milliseconds>(
        ss::lowres_system_clock::now().time_since_epoch())
        .count()};
}

model::timestamp shift(model::timestamp t, std::chrono::milliseconds by) {
    return model::timestamp{t.value() + by.count()};
}

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

// Build a single-record batch with an optional key, a list of headers, and an
// explicit timestamp, for exercising header-based dedup identity.
model::record_batch make_batch_with_headers(
  std::optional<std::string_view> key,
  std::string_view value,
  model::timestamp batch_ts,
  std::initializer_list<std::pair<std::string_view, std::string_view>>
    headers) {
    model::batch_builder builder;
    builder.set_batch_type(model::record_batch_type::raft_data);
    builder.set_batch_timestamp(model::timestamp_type::create_time, batch_ts);
    chunked_vector<model::record_header> hdrs;
    for (const auto& [hk, hv] : headers) {
        hdrs.emplace_back(iobuf::from(hk), iobuf::from(hv));
    }
    builder.add_record(
      model::record(
        {},
        0,
        0,
        key ? std::make_optional(iobuf::from(*key)) : std::nullopt,
        iobuf::from(value),
        std::move(hdrs)));
    return std::move(builder).build_sync();
}

// A record specified independently of how the batch is assembled, so tests
// can vary keys, per-record timestamp deltas, and headers together.
struct record_spec {
    std::optional<ss::sstring> key;
    ss::sstring value;
    int64_t timestamp_delta{0};
    std::vector<std::pair<ss::sstring, ss::sstring>> headers{};
};

model::record_batch
make_batch_of(const std::vector<record_spec>& specs, model::timestamp base_ts) {
    model::batch_builder builder;
    builder.set_batch_type(model::record_batch_type::raft_data);
    builder.set_batch_timestamp(model::timestamp_type::create_time, base_ts);
    int32_t offset_delta = 0;
    for (const auto& spec : specs) {
        chunked_vector<model::record_header> hdrs;
        for (const auto& [hk, hv] : spec.headers) {
            hdrs.emplace_back(iobuf::from(hk), iobuf::from(hv));
        }
        builder.add_record(
          model::record(
            {},
            spec.timestamp_delta,
            offset_delta++,
            spec.key ? std::make_optional(iobuf::from(*spec.key))
                     : std::nullopt,
            iobuf::from(spec.value),
            std::move(hdrs)));
    }
    return std::move(builder).build_sync();
}

// read_string_unsafe, not read_string: record payloads are arbitrary bytes
// and read_string would reject anything that is not valid UTF-8.
ss::sstring iobuf_string(const iobuf& b) {
    iobuf_const_parser p(b);
    return p.read_string_unsafe(p.bytes_left());
}

model::record_batch decompressed_copy(const model::record_batch& b) {
    return b.compressed() ? model::decompress_batch_sync(b) : b.copy();
}

std::vector<ss::sstring> values_of(const model::record_batch& b) {
    std::vector<ss::sstring> out;
    decompressed_copy(b).for_each_record(
      [&out](model::record r) { out.push_back(iobuf_string(r.value())); });
    return out;
}

std::vector<ss::sstring> keys_of(const model::record_batch& b) {
    std::vector<ss::sstring> out;
    decompressed_copy(b).for_each_record([&out](model::record r) {
        out.push_back(
          r.has_key() ? iobuf_string(r.key()) : ss::sstring("<null>"));
    });
    return out;
}

std::vector<int32_t> offset_deltas_of(const model::record_batch& b) {
    std::vector<int32_t> out;
    decompressed_copy(b).for_each_record(
      [&out](model::record r) { out.push_back(r.offset_delta()); });
    return out;
}

std::vector<int64_t> timestamp_deltas_of(const model::record_batch& b) {
    std::vector<int64_t> out;
    decompressed_copy(b).for_each_record(
      [&out](model::record r) { out.push_back(r.timestamp_delta()); });
    return out;
}

// Header (name, value) pairs of each record, flattened per record.
std::vector<std::vector<std::pair<ss::sstring, ss::sstring>>>
headers_of(const model::record_batch& b) {
    std::vector<std::vector<std::pair<ss::sstring, ss::sstring>>> out;
    decompressed_copy(b).for_each_record([&out](model::record r) {
        std::vector<std::pair<ss::sstring, ss::sstring>> hs;
        for (auto& h : r.headers()) {
            hs.emplace_back(iobuf_string(h.key()), iobuf_string(h.value()));
        }
        out.push_back(std::move(hs));
    });
    return out;
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

// A record older than the stored timestamp for its key, but by more than one
// window, must not be treated as a duplicate: a single future-timestamped
// (clock-skewed) record for a key must not black-hole every subsequent
// correctly-timestamped record for that key.
TEST(DedupWindowFilter, RecordOlderThanStoredByMoreThanWindowIsNotDuplicate) {
    cluster::dedup_window_filter f(1000ms);

    // A clock-skewed record pins "k" far in the future.
    auto skewed = f.filter(make_batch("k", "v1", ts(1'000'000)));
    ASSERT_TRUE(skewed.has_value());

    // A correctly-timestamped record for "k", far outside the window on the
    // *older* side of the stored timestamp, must still be admitted.
    auto later = f.filter(make_batch("k", "v2", ts(2000)));
    ASSERT_TRUE(later.has_value());
    EXPECT_EQ(record_count(*later), 1);

    // The stored timestamp is max-wins: it does not regress below the
    // clock-skewed value, so a true duplicate of the skewed record is still
    // caught.
    auto duplicate = f.filter(make_batch("k", "v3", ts(1'000'500)));
    EXPECT_FALSE(duplicate.has_value());
}

// Fully-deduplicated records are true state no-ops. In particular, a
// future-timestamp duplicate must not advance the eviction clock because
// nothing is written to the log for an all-dropped request.
TEST(DedupWindowFilter, DuplicateDoesNotAdvanceEvictionClock) {
    cluster::dedup_window_filter f(1000ms);

    auto admitted = f.filter(make_batch("k", "v1", ts(1000)));
    ASSERT_TRUE(admitted.has_value());
    EXPECT_EQ(f.max_timestamp(), ts(1000));

    auto duplicate = f.filter(make_batch("k", "v2", ts(1500)));
    EXPECT_FALSE(duplicate.has_value());
    EXPECT_EQ(f.max_timestamp(), ts(1000));
}

TEST(DedupWindowFilter, FilterRequestReturnsUndoForAdmittedKeys) {
    cluster::dedup_window_filter f(1000ms);

    // "a" is new; "b" is new; the second "a" is a duplicate (no undo entry).
    auto result = f.filter_request(
      make_multi_batch({{"a", "1"}, {"b", "2"}, {"a", "3"}}, ts(1000)));
    ASSERT_TRUE(result.batch.has_value());
    EXPECT_EQ(record_count(*result.batch), 2);
    ASSERT_EQ(result.undo.entries.size(), 2);
    EXPECT_EQ(
      result.undo.entries[0].identity,
      cluster::dedup_digest_of(iobuf::from("a")));
    EXPECT_EQ(result.undo.entries[0].applied_timestamp, ts(1000));
    EXPECT_FALSE(result.undo.entries[0].previous_timestamp.has_value());
    EXPECT_EQ(
      result.undo.entries[1].identity,
      cluster::dedup_digest_of(iobuf::from("b")));

    // Re-admission outside the window records the previous timestamp.
    auto readmitted = f.filter_request(make_batch("a", "4", ts(2500)));
    ASSERT_TRUE(readmitted.batch.has_value());
    ASSERT_EQ(readmitted.undo.entries.size(), 1);
    EXPECT_EQ(readmitted.undo.entries[0].applied_timestamp, ts(2500));
    EXPECT_EQ(readmitted.undo.entries[0].previous_timestamp, ts(1000));
}

TEST(DedupWindowFilter, RevertRequestRestoresPreRequestState) {
    cluster::dedup_window_filter f(1000ms);
    ASSERT_TRUE(f.filter(make_batch("a", "seed", ts(1000))).has_value());

    // One request that inserts a new key and re-admits an existing one.
    auto result = f.filter_request(
      make_multi_batch({{"a", "1"}, {"b", "2"}}, ts(2500)));
    ASSERT_TRUE(result.batch.has_value());
    EXPECT_EQ(f.map_size(), 2u);

    f.revert_request(result.undo);
    EXPECT_EQ(f.map_size(), 1u);
    // "a" is back at its pre-request timestamp: a record within the original
    // window is a duplicate again.
    EXPECT_FALSE(f.filter(make_batch("a", "dup", ts(1500))).has_value());
    // "b" was erased: it is admitted as new.
    EXPECT_TRUE(f.filter(make_batch("b", "new", ts(1500))).has_value());
}

TEST(DedupWindowFilter, RevertRequestLeavesNewerOverwritesUntouched) {
    cluster::dedup_window_filter f(1000ms);

    auto first = f.filter_request(make_batch("k", "1", ts(1000)));
    ASSERT_TRUE(first.batch.has_value());

    // A later request re-admits the same key outside the window before the
    // first request's replication outcome is known.
    auto second = f.filter_request(make_batch("k", "2", ts(2500)));
    ASSERT_TRUE(second.batch.has_value());

    // Reverting the first request must not clobber the second request's
    // newer state (compare-and-revert).
    f.revert_request(first.undo);
    EXPECT_EQ(f.map_size(), 1u);
    EXPECT_FALSE(f.filter(make_batch("k", "dup", ts(3000))).has_value());
}

TEST(DedupWindowFilter, RevertRequestUnwindsSameKeyMutations) {
    cluster::dedup_window_filter f(1000ms);

    // One batch mutating the same key twice: the second record's timestamp
    // delta puts it outside the window relative to the first, so both are
    // admitted and the undo carries two entries for the key.
    model::batch_builder builder;
    builder.set_batch_timestamp(model::timestamp_type::create_time, ts(1000));
    builder.add_record(
      model::record({}, 0, 0, iobuf::from("k"), iobuf::from("v1"), {}));
    builder.add_record(
      model::record({}, 1500, 1, iobuf::from("k"), iobuf::from("v2"), {}));

    auto result = f.filter_request(std::move(builder).build_sync());
    ASSERT_TRUE(result.batch.has_value());
    EXPECT_EQ(record_count(*result.batch), 2);
    ASSERT_EQ(result.undo.entries.size(), 2);

    f.revert_request(result.undo);
    EXPECT_EQ(f.map_size(), 0u);
}

TEST(DedupWindowFilter, PopulateTriggersEvictionSweep) {
    cluster::dedup_window_filter f(1000ms);
    f.populate(iobuf::from("stale"), ts(0));

    // Arrange for the next insertion to trigger the opportunistic sweep.
    auto snapshot = f.snapshot();
    snapshot.inserts_since_evict = 9'999;
    f.restore(snapshot);

    // The new key advances the clock far past "stale"'s window and its
    // insertion crosses the sweep threshold.
    f.populate(iobuf::from("fresh"), ts(5000));
    EXPECT_EQ(f.map_size(), 1u);
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
    builder.add_record(
      model::record(
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

// populate() keeps the most recent timestamp per key, making log replay
// idempotent: applying the same prefix repeatedly converges.
TEST(DedupWindowFilter, PopulateIsIdempotentMaxWins) {
    cluster::dedup_window_filter f(1000ms);

    f.populate(iobuf::from("k"), ts(2000));
    f.populate(iobuf::from("k"), ts(1000));
    f.populate(iobuf::from("k"), ts(2000));
    EXPECT_EQ(f.map_size(), 1u);

    // The stored timestamp is 2000: t=2900 is within the window.
    EXPECT_FALSE(f.filter(make_batch("k", "v", ts(2900))).has_value());
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

TEST(DedupWindowFilter, EntryLimitFailsOpenWithoutGrowingTheIndex) {
    cluster::dedup_window_filter f(1h, 2);

    ASSERT_TRUE(f.filter(make_batch("a", "1", ts(1000))).has_value());
    ASSERT_TRUE(f.filter(make_batch("b", "2", ts(1000))).has_value());
    ASSERT_EQ(f.map_size(), 2u);

    // A new identity is admitted when the index is full, but is not retained.
    ASSERT_TRUE(f.filter(make_batch("c", "3", ts(1000))).has_value());
    EXPECT_EQ(f.map_size(), 2u);
    EXPECT_TRUE(f.filter(make_batch("c", "4", ts(1000))).has_value());
    EXPECT_EQ(f.map_size(), 2u);

    // Identities that were already indexed keep their normal dedup coverage.
    EXPECT_FALSE(f.filter(make_batch("a", "duplicate", ts(1000))).has_value());
    EXPECT_EQ(f.map_size(), 2u);
}

// A record timestamped in the future must not evict entries that a later,
// correctly-ordered duplicate should still match against. Redpanda accepts
// records up to log_message_timestamp_after_max_ms (one hour by default) ahead
// of the broker, so this is legal traffic, not a malformed request. Before the
// broker-time clamp this single record pushed the eviction cutoff an hour
// ahead and the next sweep emptied the whole index. See RFC boundary B5.
TEST(DedupWindowFilter, FutureTimestampDoesNotPoisonEvictionCutoff) {
    cluster::dedup_window_filter f(5min);
    const auto now = broker_now();

    ASSERT_TRUE(f.filter(make_batch("a", "1", now)).has_value());
    ASSERT_TRUE(
      f.filter(make_batch("skewed", "2", shift(now, 1h))).has_value());

    f.evict_expired();

    EXPECT_FALSE(f.filter(make_batch("a", "duplicate", now)).has_value());
}

TEST(DedupWindowFilter, FutureTimestampIsClampedAndCounted) {
    cluster::dedup_window_filter f(5min);
    const auto before = broker_now();

    ASSERT_TRUE(
      f.filter(make_batch("skewed", "1", shift(before, 1h))).has_value());

    EXPECT_EQ(f.skew_clamped_records(), 1u);
    EXPECT_GE(f.max_timestamp().value(), before.value());
    EXPECT_LE(f.max_timestamp().value(), broker_now().value());
}

// The clamp applies to the stored entry too, not just the watermark. Clamping
// only the watermark would leave a forward-skewed entry permanently above the
// eviction cutoff, where it could never expire and would pin index capacity.
TEST(DedupWindowFilter, FutureTimestampIsClampedInTheStoredEntry) {
    cluster::dedup_window_filter f(5min);
    const auto before = broker_now();

    f.populate(iobuf::from("skewed"), shift(before, 1h));

    auto snap = f.snapshot();
    ASSERT_EQ(snap.entries.size(), 1u);
    EXPECT_GE(snap.entries[0].timestamp.value(), before.value());
    EXPECT_LE(snap.entries[0].timestamp.value(), broker_now().value());
    EXPECT_EQ(f.skew_clamped_records(), 1u);
}

// A snapshot written by a broker without the clamp can carry a poisoned
// watermark. Clamping on restore lets such a partition heal on restart rather
// than needing a dedup_generation bump.
TEST(DedupWindowFilter, RestoreClampsPoisonedSnapshotWatermark) {
    cluster::dedup_window_filter f(5min);
    const auto before = broker_now();

    cluster::dedup_index_snapshot snap;
    snap.max_timestamp = shift(before, 1h);
    snap.entries.push_back(
      {.identity = cluster::dedup_digest_of(iobuf::from("a")),
       .timestamp = before});
    f.restore(snap);

    EXPECT_LE(f.max_timestamp().value(), broker_now().value());
    // A restore is not a record, so it does not move the skew counter.
    EXPECT_EQ(f.skew_clamped_records(), 0u);

    // The restored entry survives the first sweep instead of being wiped by a
    // cutoff an hour in the future, and still deduplicates.
    f.evict_expired();
    ASSERT_EQ(f.map_size(), 1u);
    EXPECT_FALSE(f.filter(make_batch("a", "duplicate", before)).has_value());
}

TEST(DedupWindowFilter, DropsAndSaturationAreCounted) {
    cluster::dedup_window_filter f(1h, 2);

    ASSERT_TRUE(f.filter(make_batch("a", "1", ts(1000))).has_value());
    ASSERT_TRUE(f.filter(make_batch("b", "2", ts(1000))).has_value());
    EXPECT_EQ(f.unindexed_records(), 0u);
    EXPECT_EQ(f.dropped_records(), 0u);

    // A duplicate of an indexed identity is dropped and counted.
    ASSERT_FALSE(f.filter(make_batch("a", "dup", ts(1000))).has_value());
    EXPECT_EQ(f.dropped_records(), 1u);

    // At the entry limit new identities are admitted unindexed, and each one
    // is counted so the silent loss of coverage is visible.
    ASSERT_TRUE(f.filter(make_batch("c", "3", ts(1000))).has_value());
    EXPECT_EQ(f.unindexed_records(), 1u);
    ASSERT_TRUE(f.filter(make_batch("c", "4", ts(1000))).has_value());
    EXPECT_EQ(f.unindexed_records(), 2u);
    EXPECT_EQ(f.dropped_records(), 1u);

    // The apply path counts saturation the same way.
    f.populate(iobuf::from("d"), ts(1000));
    EXPECT_EQ(f.unindexed_records(), 3u);
    EXPECT_EQ(f.map_size(), 2u);
}

TEST(DedupWindowFilter, EntryLimitReopensAfterAnEvictionSweep) {
    cluster::dedup_window_filter f(1000ms, 2);
    f.populate(iobuf::from("stale-a"), ts(0));
    f.populate(iobuf::from("stale-b"), ts(0));
    ASSERT_EQ(f.map_size(), 2u);

    // Arrange for this attempted insertion to trigger the normal amortized
    // sweep. Both old entries expire before the capacity check, so the new
    // identity can be indexed without exceeding the hard limit.
    auto snapshot = f.snapshot();
    snapshot.inserts_since_evict = 9'999;
    f.restore(snapshot);
    f.populate(iobuf::from("fresh"), ts(5000));

    ASSERT_EQ(f.map_size(), 1u);
    EXPECT_FALSE(
      f.filter(make_batch("fresh", "duplicate", ts(5000))).has_value());
}

// snapshot()/restore() round-trip the full index state.
TEST(DedupWindowFilter, SnapshotRestoreRoundTrip) {
    cluster::dedup_window_filter f(1000ms);
    f.filter(make_batch("a", "1", ts(1000)));
    f.filter(make_batch("b", "2", ts(1200)));
    auto snapshot = f.snapshot();

    cluster::dedup_window_filter restored(1000ms);
    restored.restore(snapshot);
    EXPECT_EQ(restored.snapshot(), snapshot);
    EXPECT_FALSE(restored.filter(make_batch("a", "dup", ts(1500))).has_value());
}

// --- Header-based dedup identity (redpanda.dedup.key.header) ---

// Legacy topics never call set_key_header(): the default is unset, so
// filter() keeps deduplicating on the Kafka key exactly as before this
// feature existed.
TEST(DedupWindowFilterHeader, LegacyKeyBasedTopicsUnaffectedByDefault) {
    cluster::dedup_window_filter f(1000ms);
    EXPECT_FALSE(f.key_header().has_value());

    EXPECT_TRUE(f.filter(make_batch("k", "v1", ts(1000))).has_value());
    EXPECT_FALSE(f.filter(make_batch("k", "v2", ts(1500))).has_value());
}

// With a header configured, the header's value -- not the Kafka key -- is the
// dedup identity: a duplicate header value within the window is dropped even
// with a different Kafka key, and a distinct header value is kept even with
// the same Kafka key.
TEST(DedupWindowFilterHeader, DeduplicatesOnHeaderValue) {
    cluster::dedup_window_filter f(1000ms);
    f.set_key_header("redpanda-dedup-key");

    auto r1 = f.filter(make_batch_with_headers(
      "partition-key", "v1", ts(1000), {{"redpanda-dedup-key", "id-1"}}));
    ASSERT_TRUE(r1.has_value());

    // Same header value, different Kafka key, within the window: dropped.
    auto r2 = f.filter(make_batch_with_headers(
      "other-partition-key", "v2", ts(1500), {{"redpanda-dedup-key", "id-1"}}));
    EXPECT_FALSE(r2.has_value());
}

// Two records can share a Kafka partition key while having distinct dedup
// header values (e.g. co-partitioned entities with independent dedup
// identities): both are admitted.
TEST(DedupWindowFilterHeader, SamePartitionKeyDifferentDedupKeysBothAdmitted) {
    cluster::dedup_window_filter f(1000ms);
    f.set_key_header("redpanda-dedup-key");

    model::batch_builder builder;
    builder.set_batch_type(model::record_batch_type::raft_data);
    builder.set_batch_timestamp(model::timestamp_type::create_time, ts(1000));
    chunked_vector<model::record_header> h1;
    h1.emplace_back(iobuf::from("redpanda-dedup-key"), iobuf::from("id-1"));
    builder.add_record(
      model::record(
        {},
        0,
        0,
        iobuf::from("same-partition-key"),
        iobuf::from("v1"),
        std::move(h1)));
    chunked_vector<model::record_header> h2;
    h2.emplace_back(iobuf::from("redpanda-dedup-key"), iobuf::from("id-2"));
    builder.add_record(
      model::record(
        {},
        0,
        1,
        iobuf::from("same-partition-key"),
        iobuf::from("v2"),
        std::move(h2)));

    auto r = f.filter(std::move(builder).build_sync());
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(record_count(*r), 2);
}

// When a record carries the configured header more than once, the first
// occurrence is the deterministic tie-breaker.
TEST(DedupWindowFilterHeader, DuplicateHeaderKeysUseFirstOccurrence) {
    cluster::dedup_window_filter f(1000ms);
    f.set_key_header("redpanda-dedup-key");

    model::batch_builder builder;
    builder.set_batch_type(model::record_batch_type::raft_data);
    builder.set_batch_timestamp(model::timestamp_type::create_time, ts(1000));
    chunked_vector<model::record_header> hdrs;
    hdrs.emplace_back(iobuf::from("redpanda-dedup-key"), iobuf::from("first"));
    hdrs.emplace_back(iobuf::from("redpanda-dedup-key"), iobuf::from("second"));
    builder.add_record(
      model::record(
        {}, 0, 0, iobuf::from("k"), iobuf::from("v1"), std::move(hdrs)));
    ASSERT_TRUE(f.filter(std::move(builder).build_sync()).has_value());

    // A later record whose single header matches the *first* occurrence's
    // value is a duplicate; matching the second occurrence's value would not
    // be, so this distinguishes which one was used.
    EXPECT_FALSE(
      f.filter(make_batch_with_headers(
                 "k", "v2", ts(1500), {{"redpanda-dedup-key", "first"}}))
        .has_value());
    EXPECT_TRUE(
      f.filter(make_batch_with_headers(
                 "k", "v3", ts(1600), {{"redpanda-dedup-key", "second"}}))
        .has_value());
}

// A record with no occurrence of the configured header rejects the whole
// request: it is never silently admitted un-deduplicated by falling back to
// the Kafka key.
TEST(DedupWindowFilterHeader, AbsentHeaderRejectsRequest) {
    cluster::dedup_window_filter f(1000ms);
    f.set_key_header("redpanda-dedup-key");

    auto result = f.filter_request(
      make_batch_with_headers("k", "v1", ts(1000), {}));
    EXPECT_TRUE(result.missing_required_header);
    EXPECT_FALSE(result.batch.has_value());
    EXPECT_EQ(f.map_size(), 0u);
}

// A batch with a mix of records carrying the header and one missing it is
// rejected wholesale: none of the header-bearing records are admitted either,
// and the index is left untouched.
TEST(DedupWindowFilterHeader, PartiallyMissingHeaderRejectsWholeBatch) {
    cluster::dedup_window_filter f(1000ms);
    f.set_key_header("redpanda-dedup-key");

    model::batch_builder builder;
    builder.set_batch_type(model::record_batch_type::raft_data);
    builder.set_batch_timestamp(model::timestamp_type::create_time, ts(1000));
    chunked_vector<model::record_header> h1;
    h1.emplace_back(iobuf::from("redpanda-dedup-key"), iobuf::from("id-1"));
    builder.add_record(
      model::record(
        {}, 0, 0, iobuf::from("a"), iobuf::from("v1"), std::move(h1)));
    builder.add_record(
      model::record({}, 0, 1, iobuf::from("b"), iobuf::from("v2"), {}));

    auto result = f.filter_request(std::move(builder).build_sync());
    EXPECT_TRUE(result.missing_required_header);
    EXPECT_FALSE(result.batch.has_value());
    EXPECT_EQ(f.map_size(), 0u);
}

// Header-based dedup works on compressed batches: the header is inspected on
// the decompressed temporary, same as key-based dedup.
TEST(DedupWindowFilterHeader, CompressedBatch) {
    cluster::dedup_window_filter f(1000ms);
    f.set_key_header("redpanda-dedup-key");

    auto plain = make_batch_with_headers(
      "k", "v1", ts(1000), {{"redpanda-dedup-key", "id-1"}});
    auto compressed = model::compress_batch_sync(
      model::compression::lz4, std::move(plain));

    auto r1 = f.filter(std::move(compressed));
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(record_count(*r1), 1);

    auto r2 = f.filter(make_batch_with_headers(
      "k", "v2", ts(1500), {{"redpanda-dedup-key", "id-1"}}));
    EXPECT_FALSE(r2.has_value());
}

// A null Kafka key is irrelevant in header mode: the record is deduplicated
// on the header value regardless of whether it has a key.
TEST(DedupWindowFilterHeader, NullKeyStillDeduplicatesOnHeader) {
    cluster::dedup_window_filter f(1000ms);
    f.set_key_header("redpanda-dedup-key");

    auto r1 = f.filter(make_batch_with_headers(
      std::nullopt, "v1", ts(1000), {{"redpanda-dedup-key", "id-1"}}));
    ASSERT_TRUE(r1.has_value());

    auto r2 = f.filter(make_batch_with_headers(
      std::nullopt, "v2", ts(1500), {{"redpanda-dedup-key", "id-1"}}));
    EXPECT_FALSE(r2.has_value());
}

// --- Identity digests ---

// The index stores a digest, not the identity bytes, so the digest must be a
// pure function of the byte sequence -- not of how that sequence happens to
// be split across iobuf fragments. A leader classifying a fragmented iobuf
// straight off the wire and a replica digesting the same identity after a
// round trip through the log must land on the same entry.
TEST(DedupIdentityDigest, IsIndependentOfFragmentation) {
    // iobuf::append() packs into the previous fragment's spare capacity
    // whenever it fits, so small appends produce a single fragment and would
    // make this test vacuous. Chunks this size cannot be packed, so each is
    // linked as its own fragment -- and the fragment count is asserted below
    // so the test fails loudly rather than silently proving nothing if that
    // ever stops being true.
    const std::string payload(300'000, 'x');
    constexpr size_t chunk_size = 100'000;

    iobuf fragmented;
    for (size_t off = 0; off < payload.size(); off += chunk_size) {
        iobuf chunk;
        chunk.append(payload.data() + off, chunk_size);
        fragmented.append(std::move(chunk));
    }
    ASSERT_EQ(fragmented.size_bytes(), payload.size());

    size_t fragments = 0;
    for (auto it = fragmented.begin(); it != fragmented.end(); ++it) {
        ++fragments;
    }
    ASSERT_GT(fragments, 1u) << "test needs a genuinely fragmented iobuf";

    // The expected values are the xxhash64 of the flat 300k-byte sequence
    // under each seed, computed independently (python-xxhash). Matching them
    // is exactly the property that matters: a digest taken over fragments
    // equals the digest of the byte sequence those fragments spell out, so a
    // leader classifying a wire-fragmented identity and a replica digesting
    // the same identity after a round trip through the log agree.
    const auto d = cluster::dedup_digest_of(fragmented);
    EXPECT_EQ(d.hi, 0x6603e3319e7157bbULL);
    EXPECT_EQ(d.lo, 0xc7c5a0e27b0d32daULL);
}

// Distinct identities must not share an entry. Prefixes are the case worth
// pinning: a length-oblivious digest would let "ab" and "abc" collide.
TEST(DedupIdentityDigest, SeparatesDistinctIdentities) {
    const auto ab = cluster::dedup_digest_of(iobuf::from("ab"));
    const auto abc = cluster::dedup_digest_of(iobuf::from("abc"));
    const auto abd = cluster::dedup_digest_of(iobuf::from("abd"));
    const auto empty = cluster::dedup_digest_of(iobuf{});

    EXPECT_NE(ab, abc);
    EXPECT_NE(abc, abd);
    EXPECT_NE(ab, empty);
    EXPECT_EQ(ab, cluster::dedup_digest_of(iobuf::from("ab")));
}

// The digest is persisted in snapshots and compared across replicas, so the
// seeds and the algorithm are part of the on-disk format: changing either
// silently invalidates every existing index without a serde version bump.
// These constants were computed independently (python-xxhash, xxh64 with the
// same two seeds), so this also cross-checks the implementation rather than
// just pinning whatever it happens to produce.
TEST(DedupIdentityDigest, IsPinnedForSnapshotCompatibility) {
    const auto d = cluster::dedup_digest_of(iobuf::from("dedup-identity"));
    EXPECT_EQ(d.hi, 0xa0e1ea34f71724b7ULL);
    EXPECT_EQ(d.lo, 0x68a86e30b386534dULL);
}

// --- Eviction interval ---

// evict_expired() scans the whole map, so a fixed sweep interval makes the
// amortized cost per attempt grow linearly with the index. The interval scales
// with the map instead: at 160k entries it is 20k attempts, so 15k attempts
// must not have triggered a sweep. Under a fixed 10k interval
// one would have fired and reset the counter to 5k.
TEST(DedupWindowFilter, EvictionIntervalScalesWithMapSize) {
    cluster::dedup_window_filter f(1000ms);

    constexpr size_t seeded = 160'000;
    constexpr size_t added = 15'000;
    // A single timestamp for every entry: nothing is ever evictable, so the
    // only thing that can change the counter is a sweep firing.
    for (size_t i = 0; i < seeded; ++i) {
        f.populate(iobuf::from(fmt::format("seed-{}", i)), ts(1000));
    }
    ASSERT_EQ(f.map_size(), seeded);

    auto reset = f.snapshot();
    reset.inserts_since_evict = 0;
    f.restore(reset);

    for (size_t i = 0; i < added; ++i) {
        f.populate(iobuf::from(fmt::format("extra-{}", i)), ts(1000));
    }
    ASSERT_EQ(f.map_size(), seeded + added);
    EXPECT_EQ(f.snapshot().inserts_since_evict, added);

    // The other half of the property: the sweep must still fire once the
    // scaled interval is crossed, or an interval that had effectively become
    // infinite would pass the check above. The counter can only fall below
    // where it already stood by being reset, which only evict_expired() does.
    constexpr size_t past_the_interval = 10'000;
    for (size_t i = 0; i < past_the_interval; ++i) {
        f.populate(iobuf::from(fmt::format("more-{}", i)), ts(1000));
    }
    EXPECT_LT(f.snapshot().inserts_since_evict, added);
}

// --- Record payload integrity across the sharing rewrite ---

// filter_request() iterates records by sharing them out of the batch rather
// than deep-copying each one. The rebuilt batch must still carry the exact
// key and value bytes of the surviving records -- a mis-sized or misaligned
// share would corrupt payloads while leaving record counts and metadata
// looking correct.
TEST(DedupWindowFilter, PartialFilterPreservesRecordPayloads) {
    cluster::dedup_window_filter f(1000ms);
    ASSERT_TRUE(f.filter(make_batch("dup", "seed", ts(1000))).has_value());

    auto filtered = f.filter(make_multi_batch(
      {{"keep-1", "value-one"},
       {"dup", "dropped"},
       {"keep-2", "value-two"},
       {"keep-3", "value-three"}},
      ts(1000)));
    ASSERT_TRUE(filtered.has_value());

    EXPECT_EQ(
      keys_of(*filtered),
      (std::vector<ss::sstring>{"keep-1", "keep-2", "keep-3"}));
    EXPECT_EQ(
      values_of(*filtered),
      (std::vector<ss::sstring>{"value-one", "value-two", "value-three"}));
}

// The nothing-filtered fast path hands back the original batch object. The
// shares taken during classification are released before that happens, so
// the returned batch must be fully intact.
TEST(DedupWindowFilter, FastPathReturnsOriginalBatchIntact) {
    cluster::dedup_window_filter f(1000ms);

    auto original = make_multi_batch(
      {{"a", "value-a"}, {"b", "value-b"}, {"c", "value-c"}}, ts(1000));
    const auto expected_keys = keys_of(original);
    const auto expected_values = values_of(original);

    auto result = f.filter_request(std::move(original));
    ASSERT_TRUE(result.batch.has_value());
    EXPECT_EQ(keys_of(*result.batch), expected_keys);
    EXPECT_EQ(values_of(*result.batch), expected_values);

    // Passing the batch through untouched must not mean skipping the index
    // work: every record was still classified and inserted, so a failed
    // replication can revert exactly this request.
    ASSERT_EQ(result.undo.entries.size(), 3u);
    EXPECT_EQ(f.map_size(), 3u);
    f.revert_request(result.undo);
    EXPECT_EQ(f.map_size(), 0u);
}

// --- Drop position within a batch ---
//
// The rewrite classifies every record before deciding which survive, then
// rebuilds from a second pass indexed by position. An off-by-one between the
// two passes would surface as the wrong record being dropped, so each drop
// position is pinned separately.

TEST(DedupWindowFilter, DuplicateAtBatchStart) {
    cluster::dedup_window_filter f(1000ms);
    ASSERT_TRUE(f.filter(make_batch("dup", "seed", ts(1000))).has_value());

    auto filtered = f.filter(
      make_multi_batch({{"dup", "x"}, {"a", "1"}, {"b", "2"}}, ts(1000)));
    ASSERT_TRUE(filtered.has_value());
    EXPECT_EQ(keys_of(*filtered), (std::vector<ss::sstring>{"a", "b"}));
    EXPECT_EQ(values_of(*filtered), (std::vector<ss::sstring>{"1", "2"}));
}

TEST(DedupWindowFilter, DuplicateAtBatchEnd) {
    cluster::dedup_window_filter f(1000ms);
    ASSERT_TRUE(f.filter(make_batch("dup", "seed", ts(1000))).has_value());

    auto filtered = f.filter(
      make_multi_batch({{"a", "1"}, {"b", "2"}, {"dup", "x"}}, ts(1000)));
    ASSERT_TRUE(filtered.has_value());
    EXPECT_EQ(keys_of(*filtered), (std::vector<ss::sstring>{"a", "b"}));
    EXPECT_EQ(values_of(*filtered), (std::vector<ss::sstring>{"1", "2"}));
}

TEST(DedupWindowFilter, AlternatingDuplicates) {
    cluster::dedup_window_filter f(1000ms);
    ASSERT_TRUE(f.filter(make_batch("d1", "seed", ts(1000))).has_value());
    ASSERT_TRUE(f.filter(make_batch("d2", "seed", ts(1000))).has_value());

    auto filtered = f.filter(make_multi_batch(
      {{"d1", "x"}, {"a", "1"}, {"d2", "y"}, {"b", "2"}, {"d1", "z"}},
      ts(1000)));
    ASSERT_TRUE(filtered.has_value());
    EXPECT_EQ(keys_of(*filtered), (std::vector<ss::sstring>{"a", "b"}));
    EXPECT_EQ(values_of(*filtered), (std::vector<ss::sstring>{"1", "2"}));
}

// Two records sharing an identity inside one request resolve first-wins the
// same way they would across requests: classification runs in record order.
TEST(DedupWindowFilter, SameKeyTwiceInOneBatchKeepsTheFirst) {
    cluster::dedup_window_filter f(1000ms);

    auto filtered = f.filter(
      make_multi_batch({{"x", "first"}, {"x", "second"}}, ts(1000)));
    ASSERT_TRUE(filtered.has_value());
    EXPECT_EQ(values_of(*filtered), (std::vector<ss::sstring>{"first"}));
}

TEST(DedupWindowFilter, SameKeyManyTimesInOneBatchKeepsTheFirst) {
    cluster::dedup_window_filter f(1000ms);

    auto filtered = f.filter(make_multi_batch(
      {{"x", "first"},
       {"x", "second"},
       {"y", "other"},
       {"x", "third"},
       {"x", "fourth"}},
      ts(1000)));
    ASSERT_TRUE(filtered.has_value());
    EXPECT_EQ(keys_of(*filtered), (std::vector<ss::sstring>{"x", "y"}));
    EXPECT_EQ(
      values_of(*filtered), (std::vector<ss::sstring>{"first", "other"}));
}

// --- Metadata preserved through the rebuild ---

// Offset deltas are renumbered contiguously (this is a produce batch, not a
// compacted one), while timestamp deltas must survive untouched -- they are
// what a consumer reads back as each record's CreateTime.
TEST(DedupWindowFilter, RebuildRenumbersOffsetsAndKeepsTimestampDeltas) {
    cluster::dedup_window_filter f(1000ms);
    ASSERT_TRUE(f.filter(make_batch("dup", "seed", ts(1000))).has_value());

    auto filtered = f.filter(make_batch_of(
      {{.key = "a", .value = "1", .timestamp_delta = 5},
       {.key = "dup", .value = "x", .timestamp_delta = 7},
       {.key = "b", .value = "2", .timestamp_delta = 11},
       {.key = "c", .value = "3", .timestamp_delta = 13}},
      ts(1000)));
    ASSERT_TRUE(filtered.has_value());

    EXPECT_EQ(offset_deltas_of(*filtered), (std::vector<int32_t>{0, 1, 2}));
    EXPECT_EQ(
      timestamp_deltas_of(*filtered), (std::vector<int64_t>{5, 11, 13}));
    EXPECT_EQ(filtered->header().first_timestamp, ts(1000));
    EXPECT_EQ(filtered->header().last_offset_delta, 2);
}

TEST(DedupWindowFilter, RebuildPreservesRecordHeaders) {
    cluster::dedup_window_filter f(1000ms);
    ASSERT_TRUE(f.filter(make_batch("dup", "seed", ts(1000))).has_value());

    auto filtered = f.filter(make_batch_of(
      {{.key = "dup", .value = "x"},
       {.key = "a", .value = "1", .headers = {{"h1", "v1"}, {"h2", "v2"}}},
       {.key = "b", .value = "2", .headers = {{"h3", "v3"}}}},
      ts(1000)));
    ASSERT_TRUE(filtered.has_value());

    const auto hs = headers_of(*filtered);
    ASSERT_EQ(hs.size(), 2u);
    EXPECT_EQ(
      hs[0],
      (std::vector<std::pair<ss::sstring, ss::sstring>>{
        {"h1", "v1"}, {"h2", "v2"}}));
    EXPECT_EQ(
      hs[1], (std::vector<std::pair<ss::sstring, ss::sstring>>{{"h3", "v3"}}));
}

// The rebuild copies these off a header snapshot taken before the record
// iterator ran; losing any of them would corrupt an otherwise valid batch.
TEST(DedupWindowFilter, RebuildPreservesBatchHeaderFields) {
    cluster::dedup_window_filter f(1000ms);
    ASSERT_TRUE(f.filter(make_batch("dup", "seed", ts(1000))).has_value());

    model::batch_builder builder;
    builder.set_batch_type(model::record_batch_type::raft_data);
    builder.set_batch_timestamp(model::timestamp_type::create_time, ts(1000));
    builder.set_base_offset(model::offset{4242});
    builder.set_producer_id(77);
    builder.set_producer_epoch(3);
    builder.set_base_sequence(9);
    builder.add_record(
      model::record({}, 0, 0, iobuf::from("dup"), iobuf::from("x"), {}));
    builder.add_record(
      model::record({}, 0, 1, iobuf::from("keep"), iobuf::from("1"), {}));

    auto filtered = f.filter(std::move(builder).build_sync());
    ASSERT_TRUE(filtered.has_value());
    EXPECT_EQ(filtered->header().base_offset, model::offset{4242});
    EXPECT_EQ(filtered->header().type, model::record_batch_type::raft_data);
    EXPECT_EQ(filtered->header().producer_id, 77);
    EXPECT_EQ(filtered->header().producer_epoch, 3);
    EXPECT_EQ(filtered->header().base_sequence, 9);
    EXPECT_EQ(
      filtered->header().attrs.timestamp_type(),
      model::timestamp_type::create_time);
}

// --- Identity edge cases ---

// A zero-length key is an identity like any other; only an absent key opts
// out of dedup. Conflating the two would silently stop deduplicating a
// legitimate (if unusual) key.
TEST(DedupWindowFilter, EmptyKeyIsAnIdentityButNullKeyIsNot) {
    cluster::dedup_window_filter f(1000ms);

    ASSERT_TRUE(f.filter(make_batch("", "first", ts(1000))).has_value());
    EXPECT_EQ(f.map_size(), 1u);
    // A second empty-key record within the window is a duplicate of it.
    EXPECT_FALSE(f.filter(make_batch("", "second", ts(1200))).has_value());

    // Null keys never enter the index and are never dropped.
    ASSERT_TRUE(f.filter(make_null_key_batch("v1", ts(1000))).has_value());
    ASSERT_TRUE(f.filter(make_null_key_batch("v2", ts(1000))).has_value());
    EXPECT_EQ(f.map_size(), 1u);
}

TEST(DedupWindowFilter, NullKeysSurviveAlongsideDroppedDuplicates) {
    cluster::dedup_window_filter f(1000ms);
    ASSERT_TRUE(f.filter(make_batch("dup", "seed", ts(1000))).has_value());

    auto filtered = f.filter(make_batch_of(
      {{.key = std::nullopt, .value = "n1"},
       {.key = "dup", .value = "x"},
       {.key = std::nullopt, .value = "n2"},
       {.key = "a", .value = "1"}},
      ts(1000)));
    ASSERT_TRUE(filtered.has_value());
    EXPECT_EQ(
      values_of(*filtered), (std::vector<ss::sstring>{"n1", "n2", "1"}));
}

// Identities far larger than any inline buffer must round-trip correctly:
// the digest consumes them fragment by fragment rather than linearizing.
TEST(DedupWindowFilter, LargeIdentitiesDeduplicate) {
    cluster::dedup_window_filter f(1000ms);
    const ss::sstring big_a(200'000, 'a');
    const ss::sstring big_b(200'000, 'b');

    ASSERT_TRUE(f.filter(make_batch(big_a, "v1", ts(1000))).has_value());
    EXPECT_FALSE(f.filter(make_batch(big_a, "v2", ts(1200))).has_value());
    // A different large identity is not confused with the first.
    EXPECT_TRUE(f.filter(make_batch(big_b, "v3", ts(1200))).has_value());
    EXPECT_EQ(f.map_size(), 2u);
}

// Large values are what actually exercise multi-fragment record sharing: the
// rebuilt batch must reproduce them byte for byte.
TEST(DedupWindowFilter, LargeValuesSurviveTheRebuildIntact) {
    cluster::dedup_window_filter f(1000ms);
    ASSERT_TRUE(f.filter(make_batch("dup", "seed", ts(1000))).has_value());

    const ss::sstring big1(150'000, 'p');
    const ss::sstring big2(150'000, 'q');
    auto filtered = f.filter(make_batch_of(
      {{.key = "a", .value = big1},
       {.key = "dup", .value = "dropped"},
       {.key = "b", .value = big2}},
      ts(1000)));
    ASSERT_TRUE(filtered.has_value());
    EXPECT_EQ(values_of(*filtered), (std::vector<ss::sstring>{big1, big2}));
}

// --- Larger batches ---

// Exercises the chunked descriptor vector and, more importantly, that the
// classification pass and the rebuild pass stay index-aligned over many
// records.
TEST(DedupWindowFilter, LargeBatchPartialFilterKeepsTheRightRecords) {
    cluster::dedup_window_filter f(1000ms);

    constexpr int total = 3000;
    std::vector<record_spec> specs;
    std::vector<ss::sstring> expected;
    specs.reserve(total);
    for (int i = 0; i < total; ++i) {
        auto key = ss::sstring(fmt::format("k-{}", i));
        auto value = ss::sstring(fmt::format("v-{}", i));
        if (i % 3 == 0) {
            // Seed it so this record is classified as a duplicate.
            ASSERT_TRUE(
              f.filter(make_batch(key, "seed", ts(1000))).has_value());
        } else {
            expected.push_back(value);
        }
        specs.push_back({.key = key, .value = value});
    }

    auto filtered = f.filter(make_batch_of(specs, ts(1000)));
    ASSERT_TRUE(filtered.has_value());
    EXPECT_EQ(record_count(*filtered), static_cast<int32_t>(expected.size()));
    EXPECT_EQ(values_of(*filtered), expected);

    std::vector<int32_t> contiguous;
    contiguous.reserve(expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        contiguous.push_back(static_cast<int32_t>(i));
    }
    EXPECT_EQ(offset_deltas_of(*filtered), contiguous);
}

// --- Compressed batches ---

TEST(DedupWindowFilter, CompressedPartialFilterPreservesPayloads) {
    cluster::dedup_window_filter f(1000ms);
    ASSERT_TRUE(f.filter(make_batch("dup", "seed", ts(1000))).has_value());

    auto plain = make_multi_batch(
      {{"a", "value-a"}, {"dup", "dropped"}, {"b", "value-b"}}, ts(1000));
    auto compressed = model::compress_batch_sync(
      model::compression::lz4, std::move(plain));

    auto filtered = f.filter(std::move(compressed));
    ASSERT_TRUE(filtered.has_value());
    EXPECT_TRUE(filtered->compressed());
    EXPECT_EQ(keys_of(*filtered), (std::vector<ss::sstring>{"a", "b"}));
    EXPECT_EQ(
      values_of(*filtered), (std::vector<ss::sstring>{"value-a", "value-b"}));
}

TEST(DedupWindowFilter, CompressedAllDuplicatesReturnsNullopt) {
    cluster::dedup_window_filter f(1000ms);
    ASSERT_TRUE(f.filter(make_multi_batch({{"a", "1"}, {"b", "2"}}, ts(1000)))
                  .has_value());

    auto plain = make_multi_batch({{"a", "x"}, {"b", "y"}}, ts(1200));
    auto compressed = model::compress_batch_sync(
      model::compression::lz4, std::move(plain));
    EXPECT_FALSE(f.filter(std::move(compressed)).has_value());
}

// --- Header mode ---

// The rejection scan runs during the single classification pass, so a
// missing header anywhere in the batch must reject it -- and, because
// classification is deferred until the pass completes, must leave the index
// exactly as it was, including entries admitted by earlier requests.
TEST(
  DedupWindowFilterHeader, MissingHeaderAtAnyPositionRejectsAndPreservesIndex) {
    for (size_t missing = 0; missing < 3; ++missing) {
        cluster::dedup_window_filter f(1000ms);
        f.set_key_header("dedup-key");

        // An earlier admitted request whose entry must survive the rejection.
        ASSERT_TRUE(f.filter(make_batch_of(
                               {{.key = "k",
                                 .value = "seed",
                                 .headers = {{"dedup-key", "seeded"}}}},
                               ts(1000)))
                      .has_value());
        ASSERT_EQ(f.map_size(), 1u);

        std::vector<record_spec> specs;
        for (size_t i = 0; i < 3; ++i) {
            record_spec spec{
              .key = "k", .value = ss::sstring(fmt::format("v{}", i))};
            if (i != missing) {
                spec.headers = {
                  {"dedup-key", ss::sstring(fmt::format("id-{}", i))}};
            }
            specs.push_back(std::move(spec));
        }

        auto result = f.filter_request(make_batch_of(specs, ts(1000)));
        EXPECT_TRUE(result.missing_required_header)
          << "missing header at index " << missing;
        EXPECT_FALSE(result.batch.has_value());
        EXPECT_TRUE(result.undo.entries.empty());
        // Nothing from the rejected request was indexed, and the seeded
        // entry is untouched.
        EXPECT_EQ(f.map_size(), 1u);
        EXPECT_FALSE(f.filter(make_batch_of(
                                {{.key = "other",
                                  .value = "dup",
                                  .headers = {{"dedup-key", "seeded"}}}},
                                ts(1200)))
                       .has_value());
    }
}

// In header mode the Kafka key is never consulted: identical header values
// deduplicate even across different partition keys.
TEST(DedupWindowFilterHeader, DifferentKeysWithTheSameHeaderDeduplicate) {
    cluster::dedup_window_filter f(1000ms);
    f.set_key_header("dedup-key");

    ASSERT_TRUE(f.filter(make_batch_with_headers(
                           "key-one", "v1", ts(1000), {{"dedup-key", "same"}}))
                  .has_value());
    EXPECT_FALSE(f.filter(make_batch_with_headers(
                            "key-two", "v2", ts(1200), {{"dedup-key", "same"}}))
                   .has_value());
}

// An empty header value is a present header, so it is an identity -- not a
// missing-header rejection and not a silent pass.
TEST(DedupWindowFilterHeader, EmptyHeaderValueIsAnIdentity) {
    cluster::dedup_window_filter f(1000ms);
    f.set_key_header("dedup-key");

    auto first = f.filter_request(
      make_batch_with_headers("k", "v1", ts(1000), {{"dedup-key", ""}}));
    EXPECT_FALSE(first.missing_required_header);
    ASSERT_TRUE(first.batch.has_value());
    EXPECT_EQ(f.map_size(), 1u);

    auto second = f.filter_request(
      make_batch_with_headers("k", "v2", ts(1200), {{"dedup-key", ""}}));
    EXPECT_FALSE(second.missing_required_header);
    EXPECT_FALSE(second.batch.has_value());
}

// --- Undo and revert ---

// Reverting a partially filtered request must restore exactly the entries it
// introduced -- no more (the duplicate it dropped stays indexed) and no less
// (its own admissions become admissible again).
TEST(DedupWindowFilter, RevertAfterPartialFilterRestoresExactly) {
    cluster::dedup_window_filter f(1000ms);
    ASSERT_TRUE(f.filter(make_batch("dup", "seed", ts(1000))).has_value());
    ASSERT_EQ(f.map_size(), 1u);

    auto result = f.filter_request(
      make_multi_batch({{"a", "1"}, {"dup", "x"}, {"b", "2"}}, ts(1000)));
    ASSERT_TRUE(result.batch.has_value());
    ASSERT_EQ(record_count(*result.batch), 2);
    ASSERT_EQ(f.map_size(), 3u);
    ASSERT_EQ(result.undo.entries.size(), 2u);

    f.revert_request(result.undo);
    EXPECT_EQ(f.map_size(), 1u);

    // "a" and "b" are admissible again...
    EXPECT_TRUE(f.filter(make_batch("a", "retry", ts(1000))).has_value());
    EXPECT_TRUE(f.filter(make_batch("b", "retry", ts(1000))).has_value());
    // ...while the pre-existing "dup" entry was never this request's to undo.
    EXPECT_FALSE(f.filter(make_batch("dup", "retry", ts(1000))).has_value());
}

// A request that admitted nothing has nothing to unwind, and reverting it
// must not disturb the entries that caused its records to be dropped.
TEST(DedupWindowFilter, RevertOfAnAllDuplicateRequestIsANoOp) {
    cluster::dedup_window_filter f(1000ms);
    ASSERT_TRUE(f.filter(make_multi_batch({{"a", "1"}, {"b", "2"}}, ts(1000)))
                  .has_value());
    ASSERT_EQ(f.map_size(), 2u);

    auto result = f.filter_request(
      make_multi_batch({{"a", "x"}, {"b", "y"}}, ts(1200)));
    ASSERT_FALSE(result.batch.has_value());
    EXPECT_TRUE(result.undo.entries.empty());

    f.revert_request(result.undo);
    EXPECT_EQ(f.map_size(), 2u);
    EXPECT_FALSE(f.filter(make_batch("a", "z", ts(1200))).has_value());
}

// --- Snapshot and restore ---

// restore() replaces the index rather than merging into it.
TEST(DedupWindowFilter, RestoreReplacesExistingState) {
    cluster::dedup_window_filter source(1000ms);
    ASSERT_TRUE(
      source.filter(make_batch("from-snapshot", "v", ts(1000))).has_value());
    const auto snapshot = source.snapshot();

    cluster::dedup_window_filter target(1000ms);
    ASSERT_TRUE(
      target.filter(make_batch("pre-existing", "v", ts(1000))).has_value());
    ASSERT_EQ(target.map_size(), 1u);

    target.restore(snapshot);
    EXPECT_EQ(target.map_size(), 1u);
    // The snapshot's entry is present...
    EXPECT_FALSE(
      target.filter(make_batch("from-snapshot", "dup", ts(1200))).has_value());
    // ...and the entry it replaced is gone.
    EXPECT_TRUE(
      target.filter(make_batch("pre-existing", "again", ts(1200))).has_value());
}

TEST(DedupWindowFilter, RestoreTruncatesOversizedSnapshotsToEntryLimit) {
    cluster::dedup_window_filter source(1000ms);
    source.populate(iobuf::from("a"), ts(1000));
    source.populate(iobuf::from("b"), ts(1000));
    source.populate(iobuf::from("c"), ts(1000));
    const auto snapshot = source.snapshot();
    ASSERT_EQ(snapshot.entries.size(), 3u);

    cluster::dedup_window_filter restored(1000ms, 2);
    restored.restore(snapshot);

    EXPECT_EQ(restored.map_size(), 2u);
    EXPECT_EQ(restored.snapshot().entries.size(), 2u);
}

TEST(DedupWindowFilter, SnapshotCarriesEvictionState) {
    cluster::dedup_window_filter f(1000ms);
    ASSERT_TRUE(f.filter(make_batch("a", "v", ts(4321))).has_value());

    const auto snapshot = f.snapshot();
    EXPECT_EQ(snapshot.max_timestamp, ts(4321));

    cluster::dedup_window_filter restored(1000ms);
    restored.restore(snapshot);
    EXPECT_EQ(restored.max_timestamp(), ts(4321));
    EXPECT_EQ(
      restored.snapshot().inserts_since_evict, snapshot.inserts_since_evict);
}

TEST(DedupWindowFilter, SnapshotRoundTripWithManyEntries) {
    cluster::dedup_window_filter f(1000ms);
    constexpr size_t entries = 2000;
    for (size_t i = 0; i < entries; ++i) {
        f.populate(iobuf::from(fmt::format("id-{}", i)), ts(1000));
    }
    ASSERT_EQ(f.map_size(), entries);

    cluster::dedup_window_filter restored(1000ms);
    restored.restore(f.snapshot());
    ASSERT_EQ(restored.map_size(), entries);

    // Every restored digest still matches the identity it was derived from.
    for (size_t i = 0; i < entries; ++i) {
        EXPECT_FALSE(
          restored.filter(make_batch(fmt::format("id-{}", i), "dup", ts(1000)))
            .has_value())
          << "entry " << i << " did not survive the round trip";
    }
    EXPECT_TRUE(
      restored.filter(make_batch("id-absent", "new", ts(1000))).has_value());
}

// --- Eviction correctness (as opposed to interval) ---

TEST(DedupWindowFilter, EvictExpiredKeepsInWindowEntries) {
    cluster::dedup_window_filter f(1000ms);
    f.populate(iobuf::from("old"), ts(1000));
    f.populate(iobuf::from("recent"), ts(5000));
    ASSERT_EQ(f.map_size(), 2u);

    // Cutoff is max_ts - window == 4000, so "old" goes and "recent" stays.
    f.evict_expired();
    EXPECT_EQ(f.map_size(), 1u);
    EXPECT_FALSE(f.filter(make_batch("recent", "dup", ts(5500))).has_value());
    EXPECT_TRUE(f.filter(make_batch("old", "again", ts(5500))).has_value());
}
