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

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <chrono>
#include <limits>
#include <memory>
#include <optional>
#include <random>
#include <set>
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

// The identities of the dropped records come back with the result, so the
// caller can find the in-flight requests that introduced them.
TEST(DedupWindowFilter, FilterRequestReportsDroppedIdentities) {
    cluster::dedup_window_filter f(1000ms);

    auto fresh = f.filter_request(
      make_multi_batch({{"a", "1"}, {"b", "2"}}, ts(1000)));
    ASSERT_TRUE(fresh.batch.has_value());
    EXPECT_TRUE(fresh.dropped.empty());

    // "a" and "b" are duplicates and "c" is new: the batch is rebuilt.
    auto partial = f.filter_request(
      make_multi_batch({{"a", "3"}, {"c", "4"}, {"b", "5"}}, ts(1100)));
    ASSERT_TRUE(partial.batch.has_value());
    EXPECT_EQ(record_count(*partial.batch), 1);
    ASSERT_EQ(partial.dropped.size(), 2u);
    EXPECT_EQ(partial.dropped[0], cluster::dedup_digest_of(iobuf::from("a")));
    EXPECT_EQ(partial.dropped[1], cluster::dedup_digest_of(iobuf::from("b")));

    // Everything a duplicate: no batch, every identity reported in order.
    auto all = f.filter_request(
      make_multi_batch({{"c", "6"}, {"a", "7"}}, ts(1200)));
    EXPECT_FALSE(all.batch.has_value());
    ASSERT_EQ(all.dropped.size(), 2u);
    EXPECT_EQ(all.dropped[0], cluster::dedup_digest_of(iobuf::from("c")));
    EXPECT_EQ(all.dropped[1], cluster::dedup_digest_of(iobuf::from("a")));
}

// A record's timestamp delta is client-controlled. One that carries the
// batch's base timestamp past the int64 range saturates instead of wrapping,
// and the record is admitted like any other with a timestamp from the future.
TEST(DedupWindowFilter, TimestampDeltaOverflowSaturates) {
    cluster::dedup_window_filter f(1000ms);

    model::batch_builder builder;
    builder.set_batch_type(model::record_batch_type::raft_data);
    builder.set_batch_timestamp(
      model::timestamp_type::create_time, model::timestamp{1000});
    builder.add_record(
      model::record({}, 100, 0, iobuf::from("k"), iobuf::from("v"), {}));
    auto built = std::move(builder).build_sync();
    // The builder would overflow computing max_timestamp itself, so the
    // header is patched after the fact; the filter reads the records.
    auto header = built.header();
    header.first_timestamp = model::timestamp{
      std::numeric_limits<int64_t>::max() - 10};
    header.max_timestamp = model::timestamp::max();
    model::record_batch batch(
      header, built.data().copy(), model::record_batch::tag_ctor_ng{});

    auto result = f.filter_request(std::move(batch));
    ASSERT_TRUE(result.batch.has_value());
    EXPECT_EQ(record_count(*result.batch), 1);
    EXPECT_EQ(f.map_size(), 1u);
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

// Every new identity advances the sweep cursor, so an insertion that moves the
// watermark past an existing entry's window drops it without any interval
// having to elapse first.
TEST(DedupWindowFilter, PopulateTriggersEvictionSweep) {
    cluster::dedup_window_filter f(1000ms);
    f.populate(iobuf::from("stale"), ts(0));

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

// A snapshot leaves out entries older than the window relative to the most
// recent timestamp seen, so restoring it does not carry forward state that
// can no longer cause a drop.
TEST(DedupWindowFilter, SnapshotLeavesOutExpiredEntries) {
    cluster::dedup_window_filter f(1000ms);

    // The later timestamp goes in first on purpose. Inserting "a" second
    // leaves it in the index for the snapshot to filter: the sweep cursor
    // runs before the insert, so it cannot see an entry that does not exist
    // yet, whereas inserting "a" first and then advancing the watermark past
    // its window would have the cursor drop it and leave nothing to test.
    f.filter(make_batch("b", "v", ts(5000)));
    EXPECT_EQ(f.map_size(), 1u);

    f.filter(make_batch("a", "v", ts(1000)));
    EXPECT_EQ(f.map_size(), 2u);

    // "a" (t=1000) is 4000ms behind t=5000 (> 1000ms window), so only "b"
    // (t=5000) is worth persisting. The index itself is untouched: filtering
    // a snapshot must not move the entry count the capacity path reads.
    const auto snapshot = f.snapshot();
    ASSERT_EQ(snapshot.entries.size(), 1u);
    EXPECT_EQ(snapshot.entries[0].timestamp, ts(5000));
    EXPECT_EQ(f.map_size(), 2u);

    // "a" is stale rather than absent, so it is admitted again even within
    // what would have been its original window relative to t=5000.
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

// At the entry limit the index drops its oldest slice to make room, rather
// than admitting new identities unremembered. That keeps dedup working under
// saturation with an effective window of roughly the entry limit divided by
// the new-identity arrival rate, instead of switching it off for everything
// new until traffic subsides.
TEST(DedupWindowFilter, CapacityEvictionDropsTheOldestNotTheNewest) {
    constexpr size_t cap = 8;
    constexpr size_t identities = 16;
    cluster::dedup_window_filter f(100s, cap);
    // Twice what the index holds, every one of them still inside the window,
    // so room can only come from evicting live entries. Trimming starts a
    // tenth below the ceiling rather than at it, which is why this fills past
    // the cap instead of asserting a count at it.
    for (size_t i = 0; i < identities; ++i) {
        const auto stamp = ts(static_cast<int64_t>(i + 1) * 5000);
        ASSERT_TRUE(f.filter(make_batch(fmt::format("id-{}", i), "v", stamp))
                      .has_value());
        ASSERT_LE(f.map_size(), f.max_entries());
    }

    EXPECT_GT(f.capacity_evicted_entries(), 0u);
    // Indexed, not admitted unremembered: the point of the change.
    EXPECT_EQ(f.unindexed_records(), 0u);

    // The newest entries survive and still deduplicate.
    EXPECT_FALSE(f.filter(make_batch("id-15", "v", ts(80000))).has_value());
    // The oldest was what got dropped, so it reads as a new identity again.
    EXPECT_TRUE(f.filter(make_batch("id-0", "v", ts(5000))).has_value());
}

TEST(DedupWindowFilter, CapacityEvictionKeepsDedupWorkingWhenSaturated) {
    constexpr size_t cap = 8;
    cluster::dedup_window_filter f(100s, cap);

    // Five times more identities than the index can hold, none of them old
    // enough to expire.
    for (int64_t i = 1; i <= 40; ++i) {
        f.populate(iobuf::from(fmt::format("id-{}", i)), ts(i * 2000));
        ASSERT_LE(f.map_size(), f.max_entries());
    }

    // Under the old rule every identity past the eighth went unindexed and its
    // duplicates were admitted. Now the index carries a sliding tail.
    EXPECT_EQ(f.unindexed_records(), 0u);
    EXPECT_GT(f.capacity_evicted_entries(), 0u);
    EXPECT_FALSE(f.filter(make_batch("id-40", "v", ts(80000))).has_value());
}

// A timestamp cutoff cannot drop a bounded slice when more than that slice
// shares one narrow timestamp range. Over-evicting would throw away coverage
// nobody asked to lose, so the record is admitted unindexed instead -- the old
// behaviour, kept as the floor for a case that needs timestamps far coarser
// than the window.
TEST(DedupWindowFilter, CapacityEvictionFailsOpenWhenNoSliceCanBeFreed) {
    constexpr size_t cap = 4;
    cluster::dedup_window_filter f(1h, cap);
    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE(f.filter(make_batch(fmt::format("id-{}", i), "v", ts(1000)))
                      .has_value());
    }
    ASSERT_EQ(f.map_size(), cap);

    ASSERT_TRUE(f.filter(make_batch("extra", "v", ts(1000))).has_value());
    EXPECT_EQ(f.map_size(), cap);
    EXPECT_EQ(f.capacity_evicted_entries(), 0u);
    EXPECT_EQ(f.unindexed_records(), 1u);
}

// No path into the filter may do work proportional to the index, and a
// saturated index is the case that used to break that: with nothing freeable,
// every admitted record re-scanned the whole map and re-ran a whole histogram
// pass. The cursor's per-call budget is the whole defence, so measure it
// directly rather than counting sweeps.
TEST(DedupWindowFilter, SaturatedIndexDoesBoundedWorkPerRecord) {
    constexpr size_t cap = 64;
    // Budget per new-identity attempt plus the one emergency step taken at the
    // hard ceiling. Mirrors sweep_budget + capacity_emergency_budget.
    constexpr size_t visit_bound = 8 + 64;
    cluster::dedup_window_filter f(1h, cap);
    // One timestamp for the whole index, so the oldest histogram bucket holds
    // everything and no bounded slice can be cut: the sweep has nothing to
    // free on any call.
    for (size_t i = 0; i < cap; ++i) {
        ASSERT_TRUE(f.filter(make_batch(fmt::format("id-{}", i), "v", ts(1000)))
                      .has_value());
    }
    ASSERT_EQ(f.map_size(), cap);

    // New identities, each with a distinct and advancing timestamp so that a
    // gate keyed on _max_ts alone would not hold. None can be indexed: the
    // window is an hour, so nothing has expired.
    constexpr size_t admitted = 500;
    for (size_t i = 0; i < admitted; ++i) {
        const auto before = f.sweep_visits();
        const auto stamp = ts(1000 + static_cast<int64_t>(i));
        ASSERT_TRUE(f.filter(make_batch(fmt::format("new-{}", i), "v", stamp))
                      .has_value());
        ASSERT_LE(f.sweep_visits() - before, visit_bound)
          << "record " << i << " swept more than its budget";
    }
    EXPECT_EQ(f.unindexed_records(), admitted);
    EXPECT_EQ(f.map_size(), cap);

    // Coverage is unchanged: the entries that were indexed still match, and
    // the ones that were not still do not.
    EXPECT_FALSE(f.filter(make_batch("id-0", "dup", ts(1500))).has_value());
    EXPECT_TRUE(f.filter(make_batch("new-0", "again", ts(1500))).has_value());

    // Saturation is not permanent: once a record arrives far enough ahead for
    // the expiry cutoff to reach the indexed entries, the cursor frees them on
    // its next few steps.
    ASSERT_TRUE(
      f.filter(make_batch("later", "v", shift(ts(1000), 2h))).has_value());
    EXPECT_LT(f.map_size(), cap);
}

// The same bound, stated against index size rather than saturation: the point
// of the cursor is that per-call cost does not grow with the index. A full
// pass over 200k entries would stall a reactor; at the production ceiling it
// stalled every replica at the same log position and left no one free to
// complete an acks=all quorum.
TEST(DedupWindowFilter, SweepWorkDoesNotGrowWithIndexSize) {
    cluster::dedup_window_filter f(1h, 1'000'000);

    // A single timestamp for every entry inside an hour-long window: nothing
    // is ever expirable, and the index stays far below its soft limit, so the
    // cursor only ever surveys.
    constexpr size_t seeded = 200'000;
    for (size_t i = 0; i < seeded; ++i) {
        f.populate(iobuf::from(fmt::format("seed-{}", i)), ts(1000));
    }
    ASSERT_EQ(f.map_size(), seeded);

    for (size_t i = 0; i < 2000; ++i) {
        const auto before = f.sweep_visits();
        f.populate(iobuf::from(fmt::format("extra-{}", i)), ts(1000));
        ASSERT_LE(f.sweep_visits() - before, 8u)
          << "insertion " << i << " swept more than one budget";
    }

    // An already-indexed identity does no sweeping at all: it never reaches
    // the cursor.
    const auto before = f.sweep_visits();
    ASSERT_FALSE(f.filter(make_batch("seed-0", "dup", ts(1000))).has_value());
    EXPECT_EQ(f.sweep_visits(), before);
}

// Expiry is spread across insertions rather than done in one pass, so what
// matters is that it still completes: a backlog of expired entries has to
// drain under ordinary ingestion, not wait for a sweep that never comes.
TEST(DedupWindowFilter, ExpiredEntriesDrainIncrementally) {
    constexpr size_t stale = 500;
    cluster::dedup_window_filter f(1000ms);
    for (size_t i = 0; i < stale; ++i) {
        f.populate(iobuf::from(fmt::format("stale-{}", i)), ts(0));
    }
    ASSERT_EQ(f.map_size(), stale);

    // Every one of these is past the window of every entry above, so the whole
    // seeded set is expired from the first of them onwards.
    for (size_t i = 0; i < stale; ++i) {
        f.populate(iobuf::from(fmt::format("fresh-{}", i)), ts(5000));
    }
    // Only the fresh identities are left, and no single call did the work.
    EXPECT_EQ(f.map_size(), stale);
    for (size_t i = 0; i < stale; ++i) {
        EXPECT_FALSE(
          f.filter(make_batch(fmt::format("fresh-{}", i), "v", ts(5000)))
            .has_value());
    }
    EXPECT_TRUE(f.filter(make_batch("stale-0", "v", ts(5000))).has_value());
}

// The index is a deterministic function of the log, which means the leader's
// classification path and the follower's apply path must agree entry for
// entry -- including which live entries capacity trimming drops, since those
// are dedup decisions for every record that follows. The cursor made this
// sharper than it was: sweep position is part of the state, so the two paths
// have to sweep at the same point relative to their insert, not merely sweep
// the same amount.
TEST(DedupWindowFilter, LeaderAndFollowerPathsConvergeAcrossTheEntryLimit) {
    constexpr size_t cap = 8;
    constexpr size_t identities = 60;
    cluster::dedup_window_filter leader(100s, cap);
    cluster::dedup_window_filter follower(100s, cap);

    for (size_t i = 0; i < identities; ++i) {
        const auto key = fmt::format("id-{}", i);
        const auto stamp = ts(1000 + static_cast<int64_t>(i) * 1500);
        ASSERT_TRUE(leader.filter(make_batch(key, "v", stamp)).has_value());
        follower.populate(iobuf::from(key), stamp);
    }

    // Enough identities past the cap that trimming ran many times over.
    ASSERT_GT(leader.capacity_evicted_entries(), 0u);
    // snapshot() emits in the map's dense array order, so this compares the
    // layout the cursor walks as well as the contents.
    EXPECT_EQ(leader.snapshot(), follower.snapshot());
    EXPECT_EQ(leader.max_timestamp(), follower.max_timestamp());
    EXPECT_EQ(
      leader.capacity_evicted_entries(), follower.capacity_evicted_entries());
    EXPECT_EQ(leader.unindexed_records(), follower.unindexed_records());
    EXPECT_EQ(leader.sweep_visits(), follower.sweep_visits());
}

// A cursor position and a half-built histogram describe one particular value
// array. Both are reset when the array is replaced, rather than carried into a
// map they no longer refer to.
TEST(DedupWindowFilter, RestoreAndClearResetTheSweepCursor) {
    cluster::dedup_window_filter f(1000ms);
    for (size_t i = 0; i < 300; ++i) {
        f.populate(iobuf::from(fmt::format("s-{}", i)), ts(0));
    }
    ASSERT_EQ(f.map_size(), 300u);

    // Restoring leaves the cursor at the start, so the insertion afterwards
    // sweeps one full budget from entry zero -- every one of those entries is
    // now expired, so exactly the budget is freed, and the new identity takes
    // one slot back. A cursor carried over would instead spend the call
    // wrapping, and a stale histogram would describe entries that are gone.
    f.restore(f.snapshot());
    f.populate(iobuf::from("fresh"), ts(5000));
    EXPECT_EQ(f.map_size(), 300u - 8u + 1u) << "cursor did not restart at zero";

    f.clear();
    EXPECT_EQ(f.map_size(), 0u);
    EXPECT_EQ(f.max_timestamp(), model::timestamp::min());
    // The cursor is back at zero over an empty array, and the next insertion
    // behaves like the filter's first.
    f.populate(iobuf::from("after-clear"), ts(1000));
    EXPECT_EQ(f.map_size(), 1u);
    EXPECT_FALSE(
      f.filter(make_batch("after-clear", "dup", ts(1000))).has_value());
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

    // A new identity gives the sweep cursor its budget, which is what walked
    // the whole index with a poisoned cutoff and emptied it.
    ASSERT_TRUE(f.filter(make_batch("filler", "3", now)).has_value());

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
    // cutoff an hour in the future, and still deduplicates. A new identity is
    // what gives the cursor its budget.
    ASSERT_TRUE(f.filter(make_batch("filler", "x", before)).has_value());
    ASSERT_EQ(f.map_size(), 2u);
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

    // The index is full, but both entries expire the moment this timestamp
    // advances the watermark, and the cursor runs before the capacity check --
    // so the new identity is indexed rather than admitted unremembered.
    f.populate(iobuf::from("fresh"), ts(5000));

    ASSERT_EQ(f.map_size(), 1u);
    EXPECT_EQ(f.unindexed_records(), 0u);
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
    EXPECT_EQ(d.hi, 0x6603e3319e7157bbULL & cluster::dedup_digest_hi_mask);
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
// just pinning whatever it happens to produce. The digest keeps the low half
// of the `hi` hash only (see dedup_canonical_digest()).
TEST(DedupIdentityDigest, IsPinnedForSnapshotCompatibility) {
    const auto d = cluster::dedup_digest_of(iobuf::from("dedup-identity"));
    EXPECT_EQ(d.hi, 0xa0e1ea34f71724b7ULL & cluster::dedup_digest_hi_mask);
    EXPECT_EQ(d.hi, 0xf71724b7ULL);
    EXPECT_EQ(d.lo, 0x68a86e30b386534dULL);
}

// --- Reading identities straight from the batch buffer ---
//
// for_each_dedup_identity() decodes the record layout itself instead of
// parsing each record into a model::record. Its reference is that parse: the
// two must agree on which records have an identity, on the identity's digest
// and on the timestamp, under both identity sources and for every edge of the
// format (null and empty keys, missing, repeated and null-valued headers).

namespace {

struct walked {
    cluster::dedup_identity_lookup lookup;
    cluster::dedup_identity_digest digest;
    model::timestamp timestamp;

    friend bool operator==(const walked&, const walked&) = default;
};

std::vector<walked> walk(
  const model::record_batch& b, const std::optional<ss::sstring>& key_header) {
    std::vector<walked> out;
    cluster::for_each_dedup_identity(
      b,
      key_header,
      [&out](
        cluster::dedup_identity_lookup lookup,
        cluster::dedup_identity_digest digest,
        model::timestamp ts) {
          out.push_back({lookup, digest, ts});
          return true;
      });
    return out;
}

// The model::record based lookup the walk replaced, kept here as the oracle.
std::vector<walked> parse(
  const model::record_batch& b, const std::optional<ss::sstring>& key_header) {
    std::vector<walked> out;
    const auto base_ts = b.header().first_timestamp.value();
    b.copy().for_each_record([&](model::record r) {
        walked w{
          .lookup = cluster::dedup_identity_lookup::no_identity,
          .digest = {},
          .timestamp = model::timestamp{base_ts + r.timestamp_delta()}};
        if (key_header) {
            w.lookup = cluster::dedup_identity_lookup::missing_header;
            for (auto& h : r.headers()) {
                if (iobuf_string(h.key()) == *key_header) {
                    if (h.value_size() >= 0) {
                        w.lookup = cluster::dedup_identity_lookup::identity;
                        w.digest = cluster::dedup_digest_of(h.value());
                    }
                    break;
                }
            }
        } else if (r.has_key()) {
            w.lookup = cluster::dedup_identity_lookup::identity;
            w.digest = cluster::dedup_digest_of(r.key());
        }
        out.push_back(w);
    });
    return out;
}

// A record whose headers may carry a null value, which record_spec cannot
// express.
model::record_batch make_batch_with_nullable_headers(
  std::optional<std::string_view> key,
  std::vector<std::pair<std::string_view, std::optional<std::string_view>>>
    headers,
  model::timestamp base_ts) {
    model::batch_builder builder;
    builder.set_batch_type(model::record_batch_type::raft_data);
    builder.set_batch_timestamp(model::timestamp_type::create_time, base_ts);
    chunked_vector<model::record_header> hdrs;
    for (const auto& [hk, hv] : headers) {
        hdrs.emplace_back(
          iobuf::from(hk),
          hv ? std::make_optional(iobuf::from(*hv)) : std::nullopt);
    }
    builder.add_record(
      model::record(
        {},
        0,
        0,
        key ? std::make_optional(iobuf::from(*key)) : std::nullopt,
        iobuf::from("value"),
        std::move(hdrs)));
    return std::move(builder).build_sync();
}

} // namespace

TEST(DedupIdentityWalk, MatchesRecordParsingInKeyMode) {
    const std::string big(100'000, 'k');
    auto batch = make_batch_of(
      {
        {.key = "plain", .value = "v", .timestamp_delta = 0},
        {.key = std::nullopt, .value = "null key", .timestamp_delta = 7},
        {.key = "", .value = "empty key", .timestamp_delta = 11},
        {.key = ss::sstring(big), .value = "", .timestamp_delta = 13},
        {.key = "with headers",
         .value = "v",
         .timestamp_delta = 17,
         .headers = {{"h1", "x"}, {"", ""}, {"h3", ss::sstring(5000, 'y')}}},
      },
      ts(1000));
    const auto expected = parse(batch, std::nullopt);
    ASSERT_EQ(expected.size(), 5u);
    EXPECT_EQ(walk(batch, std::nullopt), expected);

    // The identities a record parse yields are what the walk yields.
    EXPECT_EQ(expected[0].lookup, cluster::dedup_identity_lookup::identity);
    EXPECT_EQ(
      expected[0].digest, cluster::dedup_digest_of(iobuf::from("plain")));
    EXPECT_EQ(expected[1].lookup, cluster::dedup_identity_lookup::no_identity);
    EXPECT_EQ(expected[2].lookup, cluster::dedup_identity_lookup::identity);
    EXPECT_EQ(expected[2].digest, cluster::dedup_digest_of(iobuf{}));
    EXPECT_EQ(expected[3].digest, cluster::dedup_digest_of(iobuf::from(big)));
    EXPECT_EQ(expected[4].timestamp, ts(1017));
}

TEST(DedupIdentityWalk, MatchesRecordParsingInHeaderMode) {
    const std::optional<ss::sstring> header{"dedup-id"};
    auto batch = make_batch_of(
      {
        {.key = "k", .value = "v", .headers = {{"dedup-id", "first"}}},
        {.key = std::nullopt, .value = "v", .headers = {}},
        {.key = "k",
         .value = "v",
         .timestamp_delta = 3,
         .headers = {{"other", "x"}, {"dedup-id", "second"}}},
        {.key = "k",
         .value = "v",
         .headers = {{"dedup-id", "wins"}, {"dedup-id", "ignored"}}},
        {.key = "k", .value = "v", .headers = {{"dedup-i", "prefix"}}},
        {.key = "k", .value = "v", .headers = {{"dedup-idx", "longer"}}},
        {.key = "k", .value = "v", .headers = {{"dedup-id", ""}}},
      },
      ts(1000));
    const auto expected = parse(batch, header);
    ASSERT_EQ(expected.size(), 7u);
    EXPECT_EQ(walk(batch, header), expected);

    using lookup = cluster::dedup_identity_lookup;
    EXPECT_EQ(expected[0].lookup, lookup::identity);
    EXPECT_EQ(
      expected[0].digest, cluster::dedup_digest_of(iobuf::from("first")));
    EXPECT_EQ(expected[1].lookup, lookup::missing_header);
    EXPECT_EQ(expected[2].lookup, lookup::identity);
    EXPECT_EQ(expected[2].timestamp, ts(1003));
    EXPECT_EQ(
      expected[3].digest, cluster::dedup_digest_of(iobuf::from("wins")));
    EXPECT_EQ(expected[4].lookup, lookup::missing_header);
    EXPECT_EQ(expected[5].lookup, lookup::missing_header);
    EXPECT_EQ(expected[6].lookup, lookup::identity);
    EXPECT_EQ(expected[6].digest, cluster::dedup_digest_of(iobuf{}));
}

// A header carrying a null value is not an identity, and a later header of
// the same name does not get a second chance: the first occurrence decides.
TEST(DedupIdentityWalk, NullHeaderValueIsNotAnIdentity) {
    const std::optional<ss::sstring> header{"dedup-id"};
    auto null_only = make_batch_with_nullable_headers(
      "k", {{"dedup-id", std::nullopt}}, ts(1000));
    auto null_then_value = make_batch_with_nullable_headers(
      "k", {{"dedup-id", std::nullopt}, {"dedup-id", "late"}}, ts(1000));

    for (const auto* b : {&null_only, &null_then_value}) {
        const auto expected = parse(*b, header);
        ASSERT_EQ(expected.size(), 1u);
        EXPECT_EQ(
          expected[0].lookup, cluster::dedup_identity_lookup::missing_header);
        EXPECT_EQ(walk(*b, header), expected);
    }
    // In key mode the same records are keyed as usual; the headers are
    // skipped over, null values included.
    EXPECT_EQ(
      walk(null_then_value, std::nullopt),
      parse(null_then_value, std::nullopt));
}

TEST(DedupIdentityWalk, StopsWhenAskedTo) {
    auto batch = make_multi_batch(
      {{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"}}, ts(1000));
    int seen = 0;
    cluster::for_each_dedup_identity(
      batch,
      std::nullopt,
      [&seen](
        cluster::dedup_identity_lookup,
        cluster::dedup_identity_digest,
        model::timestamp) { return ++seen < 2; });
    EXPECT_EQ(seen, 2);
}

// Batches reach the walk validated, but a buffer shorter than its record
// count claims must still fail loudly rather than be read past its end.
TEST(DedupIdentityWalk, RejectsATruncatedBatch) {
    auto batch = make_multi_batch(
      {{"key-one", "value-one"}, {"key-two", "value-two"}}, ts(1000));
    auto header = batch.header();
    auto data = batch.data().copy();
    // Leaves less than one record, so the first record's size overruns.
    data.trim_back(data.size_bytes() - 10);
    header.size_bytes = static_cast<int32_t>(
      model::packed_record_batch_header_size + data.size_bytes());
    model::record_batch truncated(
      header, std::move(data), model::record_batch::tag_ctor_ng{});
    EXPECT_THROW(walk(truncated, std::nullopt), std::out_of_range);
    EXPECT_THROW(
      walk(truncated, std::optional<ss::sstring>{"h"}), std::out_of_range);
}

// Identities that reach the index as digests (the apply path) land in the
// same entries as identities that reach it as bytes.
TEST(DedupWindowFilter, PopulateByDigestMatchesPopulateByKey) {
    cluster::dedup_window_filter by_key(1000ms);
    cluster::dedup_window_filter by_digest(1000ms);
    by_key.populate(iobuf::from("same"), ts(1000));
    by_digest.populate(cluster::dedup_digest_of(iobuf::from("same")), ts(1000));

    EXPECT_FALSE(
      by_key.filter(make_batch("same", "dup", ts(1500))).has_value());
    EXPECT_FALSE(
      by_digest.filter(make_batch("same", "dup", ts(1500))).has_value());
    EXPECT_TRUE(
      by_digest.filter(make_batch("other", "v", ts(1500))).has_value());
    // The same max-wins idempotence: a later timestamp moves the entry, an
    // older one does not.
    by_digest.populate(cluster::dedup_digest_of(iobuf::from("same")), ts(2500));
    by_digest.populate(cluster::dedup_digest_of(iobuf::from("same")), ts(1200));
    EXPECT_FALSE(
      by_digest.filter(make_batch("same", "dup", ts(3400))).has_value());
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
    EXPECT_EQ(restored.truncated_entries(), 1u);
}

// The size assertion above passes under any truncation policy, which is how a
// prefix-keeps-the-oldest policy went unnoticed. Pin which entries survive:
// the index is a dense map iterated in insertion order, so a prefix would keep
// exactly the entries closest to expiry.
TEST(DedupWindowFilter, RestoreKeepsTheNewestEntriesWhenOversized) {
    cluster::dedup_window_filter source(1h);
    source.populate(iobuf::from("oldest"), ts(1000));
    source.populate(iobuf::from("middle"), ts(2000));
    source.populate(iobuf::from("newest"), ts(3000));
    const auto snapshot = source.snapshot();
    ASSERT_EQ(snapshot.entries.size(), 3u);

    cluster::dedup_window_filter restored(1h, 2);
    restored.restore(snapshot);

    ASSERT_EQ(restored.map_size(), 2u);
    EXPECT_EQ(restored.truncated_entries(), 1u);

    // The two newest identities still deduplicate; the oldest is gone and so
    // is re-admitted as a new identity.
    EXPECT_FALSE(
      restored.filter(make_batch("newest", "v", ts(3000))).has_value());
    EXPECT_FALSE(
      restored.filter(make_batch("middle", "v", ts(2000))).has_value());
    EXPECT_TRUE(
      restored.filter(make_batch("oldest", "v", ts(1000))).has_value());
}

// The entry ceiling is a hard limit: populate() must never overshoot it, even
// transiently, and an already-indexed identity must still max-wins while full.
TEST(DedupWindowFilter, PopulateRespectsTheEntryLimit) {
    cluster::dedup_window_filter f(1h, 2);
    f.populate(iobuf::from("a"), ts(1000));
    f.populate(iobuf::from("b"), ts(1000));
    ASSERT_EQ(f.map_size(), 2u);

    for (int i = 0; i < 8; ++i) {
        f.populate(iobuf::from(fmt::format("new-{}", i)), ts(1000));
        EXPECT_LE(f.map_size(), f.max_entries());
    }
    EXPECT_EQ(f.map_size(), 2u);
    EXPECT_EQ(f.unindexed_records(), 8u);

    // An indexed identity keeps max-wins semantics at the ceiling.
    f.populate(iobuf::from("a"), ts(2500));
    EXPECT_EQ(f.map_size(), 2u);
    EXPECT_EQ(f.unindexed_records(), 8u);
    EXPECT_FALSE(f.filter(make_batch("a", "dup", ts(2500))).has_value());
}

TEST(DedupWindowFilter, SnapshotCarriesTheWatermark) {
    cluster::dedup_window_filter f(1000ms);
    ASSERT_TRUE(f.filter(make_batch("a", "v", ts(4321))).has_value());

    const auto snapshot = f.snapshot();
    EXPECT_EQ(snapshot.max_timestamp, ts(4321));

    cluster::dedup_window_filter restored(1000ms);
    restored.restore(snapshot);
    EXPECT_EQ(restored.max_timestamp(), ts(4321));
    EXPECT_EQ(restored.snapshot(), snapshot);
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

// --- Expiry correctness ---

TEST(DedupWindowFilter, SnapshotKeepsInWindowEntries) {
    cluster::dedup_window_filter f(1000ms);
    // "recent" first so that "old" survives its own insertion and is still
    // there for the snapshot to filter; see SnapshotLeavesOutExpiredEntries.
    f.populate(iobuf::from("recent"), ts(5000));
    f.populate(iobuf::from("old"), ts(1000));
    ASSERT_EQ(f.map_size(), 2u);

    // Cutoff is max_ts - window == 4000, so "old" is left out and "recent"
    // is kept -- and a replica restoring this snapshot agrees.
    cluster::dedup_window_filter restored(1000ms);
    restored.restore(f.snapshot());
    EXPECT_EQ(restored.map_size(), 1u);
    EXPECT_FALSE(
      restored.filter(make_batch("recent", "dup", ts(5500))).has_value());
    EXPECT_TRUE(
      restored.filter(make_batch("old", "again", ts(5500))).has_value());
}

// Only a partial drop rebuilds and recompresses the batch, which is the whole
// cost of dedup on the produce path. The counter has to distinguish that case
// from the two pass-through cases, or there is no way to tell from a running
// broker how often the expensive path is taken.
TEST(DedupWindowFilter, RebuiltRequestsCountsOnlyPartialDrops) {
    cluster::dedup_window_filter f(1000ms);

    // All new: returned untouched.
    auto all_new = f.filter_request(
      make_multi_batch({{"a", "1"}, {"b", "2"}}, ts(1000)));
    ASSERT_TRUE(all_new.batch.has_value());
    EXPECT_EQ(f.rebuilt_requests(), 0u);

    // All duplicates: nothing to rebuild, the request is dropped whole.
    auto all_dup = f.filter_request(
      make_multi_batch({{"a", "3"}, {"b", "4"}}, ts(1500)));
    ASSERT_FALSE(all_dup.batch.has_value());
    EXPECT_EQ(f.rebuilt_requests(), 0u);

    // Partial: "a" is a duplicate, "c" is new, so the batch is rebuilt.
    auto partial = f.filter_request(
      make_multi_batch({{"a", "5"}, {"c", "6"}}, ts(1500)));
    ASSERT_TRUE(partial.batch.has_value());
    EXPECT_EQ(record_count(*partial.batch), 1);
    EXPECT_EQ(f.rebuilt_requests(), 1u);

    // Reverting a request does not un-count the rebuild it paid for.
    f.revert_request(partial.undo);
    EXPECT_EQ(f.rebuilt_requests(), 1u);
}

// A header can be present with a *null* value, which is distinct from an empty
// one. The iobuf behind it is empty either way, so digesting it would hand
// every such record the identity of the empty buffer and quietly deduplicate
// unrelated records against each other. It has to be rejected like an absent
// header instead.
TEST(DedupWindowFilter, NullValuedIdentityHeaderIsRejected) {
    cluster::dedup_window_filter f(1000ms);
    f.set_key_header("redpanda-dedup-key");

    model::batch_builder builder;
    builder.set_batch_type(model::record_batch_type::raft_data);
    builder.set_batch_timestamp(model::timestamp_type::create_time, ts(1000));
    chunked_vector<model::record_header> hdrs;
    // {key, nullopt} is a header with a null value, the way a Kafka client
    // sends one.
    hdrs.emplace_back(
      std::make_optional(iobuf::from("redpanda-dedup-key")), std::nullopt);
    builder.add_record(
      model::record(
        {}, 0, 0, iobuf::from("k"), iobuf::from("v1"), std::move(hdrs)));

    auto result = f.filter_request(std::move(builder).build_sync());
    EXPECT_TRUE(result.missing_required_header);
    EXPECT_FALSE(result.batch.has_value());
    // Nothing was indexed, so a later well-formed record is unaffected.
    EXPECT_EQ(f.map_size(), 0u);
}

// ---------------------------------------------------------------------------
// Shard budget
//
// Every dedup index on a shard reports its size into one dedup_shard_budget,
// and new identities are refused or trimmed against the shard total as well
// as against the partition's own ceiling. These tests pin the accounting --
// the budget must equal the sum of the indices on every path that changes an
// index's size -- and the capacity behaviour built on it.
// ---------------------------------------------------------------------------

namespace {

ss::lw_shared_ptr<cluster::dedup_shard_budget> make_budget(size_t limit) {
    return ss::make_lw_shared<cluster::dedup_shard_budget>(limit);
}

void populate_n(
  cluster::dedup_window_filter& f,
  std::string_view prefix,
  size_t n,
  int64_t first_ts = 1000,
  int64_t step_ms = 1) {
    for (size_t i = 0; i < n; ++i) {
        f.populate(
          iobuf::from(fmt::format("{}-{}", prefix, i)),
          ts(first_ts + static_cast<int64_t>(i) * step_ms));
    }
}

} // namespace

TEST(DedupShardBudget, WithoutABudgetNothingIsShared) {
    cluster::dedup_window_filter f(100s, 50);
    EXPECT_EQ(f.shard_budget(), nullptr);
    populate_n(f, "k", 40);
    EXPECT_EQ(f.map_size(), 40u);
    EXPECT_EQ(f.unindexed_records(), 0u);
}

TEST(DedupShardBudget, CountsEveryFilterOnTheShard) {
    auto budget = make_budget(1'000);
    cluster::dedup_window_filter a(100s, 1'000, budget);
    cluster::dedup_window_filter b(100s, 1'000, budget);
    populate_n(a, "a", 10);
    populate_n(b, "b", 5);
    EXPECT_EQ(budget->entries, 15u);
    EXPECT_EQ(a.shard_budget(), budget.get());

    // A repeated identity updates in place and costs nothing more.
    populate_n(a, "a", 10, 2000);
    EXPECT_EQ(budget->entries, 15u);
}

TEST(DedupShardBudget, ProducePathInsertsAreCounted) {
    auto budget = make_budget(1'000);
    cluster::dedup_window_filter f(100s, 1'000, budget);
    ASSERT_TRUE(
      f.filter(make_multi_batch({{"a", "1"}, {"b", "2"}, {"c", "3"}}, ts(1000)))
        .has_value());
    EXPECT_EQ(budget->entries, 3u);
    // Duplicates are dropped without touching the index.
    EXPECT_FALSE(f.filter(make_batch("a", "dup", ts(1100))).has_value());
    EXPECT_EQ(budget->entries, 3u);
}

TEST(DedupShardBudget, RevertedRequestGivesItsEntriesBack) {
    auto budget = make_budget(1'000);
    cluster::dedup_window_filter f(100s, 1'000, budget);
    populate_n(f, "kept", 4);
    auto result = f.filter_request(
      make_multi_batch({{"x", "1"}, {"y", "2"}, {"kept-0", "3"}}, ts(5000)));
    ASSERT_TRUE(result.batch.has_value());
    EXPECT_EQ(budget->entries, 6u);
    f.revert_request(result.undo);
    EXPECT_EQ(f.map_size(), 4u);
    EXPECT_EQ(budget->entries, 4u);
}

TEST(DedupShardBudget, ExpirySweepsGiveEntriesBack) {
    auto budget = make_budget(1'000);
    cluster::dedup_window_filter f(1000ms, 1'000, budget);
    populate_n(f, "old", 64, 1000, 0);
    ASSERT_EQ(budget->entries, 64u);
    // Each new identity far past the window sweeps eight expired entries.
    populate_n(f, "new", 4, 1'000'000, 0);
    EXPECT_LT(f.map_size(), 64u);
    EXPECT_EQ(budget->entries, f.map_size());
}

TEST(DedupShardBudget, ClearRestoreAndDestructionKeepTheTotalExact) {
    auto budget = make_budget(1'000);
    cluster::dedup_window_filter a(100s, 1'000, budget);
    populate_n(a, "a", 20);
    {
        cluster::dedup_window_filter b(100s, 1'000, budget);
        populate_n(b, "b", 30);
        EXPECT_EQ(budget->entries, 50u);

        auto snap = b.snapshot();
        b.clear();
        EXPECT_EQ(budget->entries, 20u);
        b.restore(snap);
        EXPECT_EQ(budget->entries, 50u);
        // Restoring over a populated index replaces, not adds.
        b.restore(snap);
        EXPECT_EQ(budget->entries, 50u);
    }
    // b's entries go back when it is destroyed.
    EXPECT_EQ(budget->entries, 20u);
    a.clear();
    EXPECT_EQ(budget->entries, 0u);
}

// The bug this guards against: four saturated partitions on one shard, each
// within its own ceiling, together far over what the shard can hold.
TEST(DedupShardBudget, CombinedSizeStaysWithinTheShardLimit) {
    constexpr size_t shard_limit = 200;
    auto budget = make_budget(shard_limit);
    std::vector<std::unique_ptr<cluster::dedup_window_filter>> filters;
    for (int i = 0; i < 4; ++i) {
        filters.push_back(
          std::make_unique<cluster::dedup_window_filter>(100s, 1'000, budget));
    }
    for (size_t i = 0; i < 2'000; ++i) {
        auto& f = *filters[i % filters.size()];
        f.populate(
          iobuf::from(fmt::format("id-{}", i)),
          ts(1000 + static_cast<int64_t>(i)));
        ASSERT_LE(budget->entries, shard_limit) << "after " << i;
    }
    size_t total = 0;
    size_t trimmed = 0;
    for (auto& f : filters) {
        total += f->map_size();
        trimmed += f->capacity_evicted_entries() + f->unindexed_records();
        // Far below its own ceiling: the shard is what bound it.
        EXPECT_LT(f->map_size(), f->max_entries());
    }
    EXPECT_EQ(total, budget->entries);
    EXPECT_GT(trimmed, 0u);
}

// Shard pressure trims a partition that is nowhere near its own soft limit,
// through the same oldest-quarter cursor the per-partition ceiling uses, so
// the partition keeps its most recent identities rather than going blind.
TEST(DedupShardBudget, PressureTrimsTheOldestEntriesOfTheInsertingPartition) {
    constexpr size_t shard_limit = 100;
    auto budget = make_budget(shard_limit);
    // Timestamps spread across many of the cursor's histogram buckets: an
    // index packed into one narrow range is deliberately never trimmed for
    // capacity (see finish_pass()), which is not what this test is about.
    cluster::dedup_window_filter f(100s, 10'000, budget);
    constexpr size_t identities = 2'000;
    constexpr int64_t step_ms = 100;
    populate_n(f, "id", identities, 1000, step_ms);

    EXPECT_LE(f.map_size(), shard_limit);
    EXPECT_GT(f.capacity_evicted_entries(), 0u);
    EXPECT_EQ(budget->entries, f.map_size());
    // The newest identity is still indexed, so its duplicate is caught.
    const auto newest = fmt::format("id-{}", identities - 1);
    EXPECT_FALSE(
      f.filter(make_batch(newest, "dup", ts(1000 + (identities - 1) * step_ms)))
        .has_value());
}

// At the hard shard limit with nothing of its own to free, a partition admits
// new identities unindexed rather than exceeding the limit. Once the space is
// released elsewhere, it indexes again.
TEST(DedupShardBudget, AtTheLimitAPartitionWithNothingToFreeAdmitsUnindexed) {
    constexpr size_t shard_limit = 50;
    auto budget = make_budget(shard_limit);
    cluster::dedup_window_filter idle(100s, 1'000, budget);
    {
        auto full = std::make_unique<cluster::dedup_window_filter>(
          100s, 1'000, budget);
        // One timestamp for every entry, so the full index has no oldest
        // quarter its cursor could give up: nothing anywhere on the shard is
        // freeable, which is the case this test is about. (With spread
        // timestamps the idle partition would get room from it instead; see
        // AnIdlePartitionCannotStarveABusyOne.)
        populate_n(*full, "full", shard_limit, 1000, 0);
        ASSERT_EQ(budget->entries, shard_limit);

        EXPECT_TRUE(idle.filter(make_batch("new", "v", ts(1000))).has_value());
        EXPECT_EQ(idle.map_size(), 0u);
        EXPECT_EQ(idle.unindexed_records(), 1u);
        EXPECT_GT(idle.capacity_sweeps(), 0u);
        EXPECT_EQ(budget->entries, shard_limit);
    }
    // The full partition went away (moved or deleted) and took its entries
    // with it.
    EXPECT_EQ(budget->entries, 0u);
    EXPECT_TRUE(idle.filter(make_batch("new", "v", ts(2000))).has_value());
    EXPECT_EQ(idle.map_size(), 1u);
    EXPECT_FALSE(idle.filter(make_batch("new", "v", ts(2100))).has_value());
}

// The starvation case: one partition filled the shard's budget and then went
// quiet, so it never inserts and never trims itself. A partition that starts
// receiving new identities must still get room -- from the largest index's
// cursor, a bounded step at a time -- or it would stay without coverage for
// as long as the idle one sits there.
TEST(DedupShardBudget, AnIdlePartitionCannotStarveABusyOne) {
    constexpr size_t shard_limit = 200;
    auto budget = make_budget(shard_limit);
    cluster::dedup_window_filter hog(1'000s, 10'000, budget);
    populate_n(hog, "hog", shard_limit, 1000, 1000);
    // Shard pressure trims the hog's own oldest entries as it nears the
    // limit, so it settles just under it rather than exactly at it.
    const auto hog_full = hog.map_size();
    ASSERT_GE(budget->entries, shard_limit * 9 / 10);
    ASSERT_LE(budget->entries, shard_limit);

    cluster::dedup_window_filter busy(1'000s, 10'000, budget);
    populate_n(busy, "busy", 2'000, 300'000, 100);
    EXPECT_LE(budget->entries, shard_limit);
    EXPECT_EQ(budget->entries, hog.map_size() + busy.map_size());
    EXPECT_GT(busy.map_size(), shard_limit / 4);
    // The room came out of the hog.
    EXPECT_LT(hog.map_size(), hog_full);
    // The busy partition's newest identity is remembered.
    EXPECT_FALSE(
      busy.filter(make_batch("busy-1999", "dup", ts(300'000 + 1999 * 100)))
        .has_value());
}

// A partition at its own ceiling makes room only from itself: the shard
// budget never lets one partition grow past dedup_max_entries_per_partition.
TEST(DedupShardBudget, OwnCeilingStillApplies) {
    auto budget = make_budget(1'000);
    cluster::dedup_window_filter big(1'000s, 10'000, budget);
    populate_n(big, "big", 900, 1000, 1000);
    cluster::dedup_window_filter capped(1'000s, 50, budget);
    populate_n(capped, "c", 500, 300'000, 100);
    EXPECT_LE(capped.map_size(), 50u);
    EXPECT_EQ(big.map_size(), 900u);
}

// Filters leave the budget's registry when destroyed, so a later filter never
// takes a step on one that is gone.
TEST(DedupShardBudget, DestroyedFiltersLeaveTheRegistry) {
    auto budget = make_budget(100);
    {
        cluster::dedup_window_filter a(100s, 1'000, budget);
        cluster::dedup_window_filter b(100s, 1'000, budget);
        EXPECT_EQ(budget->filters.size(), 2u);
    }
    EXPECT_TRUE(budget->filters.empty());
    cluster::dedup_window_filter c(100s, 1'000, budget);
    populate_n(c, "c", 150);
    EXPECT_LE(c.map_size(), 100u);
}

// restore() is bounded by what the shard budget has left, not only by the
// partition's own ceiling: restores happen at startup, and a broker whose
// partitions each restored in full could run out of memory before it served
// anything. What is kept is the newest identities.
TEST(DedupShardBudget, RestoreIsBoundedByShardHeadroom) {
    constexpr size_t shard_limit = 50;
    auto budget = make_budget(shard_limit);
    cluster::dedup_window_filter source(1'000s, 1'000);
    populate_n(source, "r", 100, 1000, 1000);
    const auto snap = source.snapshot();

    cluster::dedup_window_filter first(1'000s, 1'000, budget);
    first.restore(snap);
    EXPECT_EQ(first.map_size(), shard_limit);
    EXPECT_EQ(first.truncated_entries(), 50u);
    EXPECT_EQ(budget->entries, shard_limit);
    for (const auto& entry : first.snapshot().entries) {
        // r-50..r-99, stamped 1000 + i*1000.
        EXPECT_GE(entry.timestamp.value(), 1000 + 50 * 1000);
    }

    // The shard is full, so a second partition restores nothing.
    cluster::dedup_window_filter second(1'000s, 1'000, budget);
    second.restore(snap);
    EXPECT_EQ(second.map_size(), 0u);
    EXPECT_EQ(second.truncated_entries(), 100u);
    EXPECT_EQ(budget->entries, shard_limit);

    // Restoring a partition again replaces its own share first, so it gets
    // that share back rather than being counted against itself.
    first.restore(snap);
    EXPECT_EQ(first.map_size(), shard_limit);
    EXPECT_EQ(budget->entries, shard_limit);
}

// A restore claims its room in the shard budget before it inserts anything.
// It yields between chunks, and the shard's other partitions replay their
// logs meanwhile: counted only afterwards, the restored entries landed on top
// of theirs and the shard ended up above its limit, where freeing one entry
// per new identity never gets back under it.
TEST(DedupShardBudget, RestoreClaimsItsRoomBeforeFillingIt) {
    constexpr size_t shard_limit = 100;
    auto budget = make_budget(shard_limit);
    cluster::dedup_window_filter restoring(1'000s, 1'000, budget);
    cluster::dedup_window_filter replaying(1'000s, 1'000, budget);

    // 80 entries to restore; halfway through, the other partition admits 60
    // new identities, as a log replay would. All at one timestamp, so the
    // sweep has nothing older than the cutoff to trim and the counts below
    // are exact.
    bool interleaved = false;
    restoring
      .restore_from(
        80,
        ts(2000),
        [&](auto&& emit) {
            for (uint64_t i = 0; i < 80; ++i) {
                if (i == 40 && !interleaved) {
                    interleaved = true;
                    populate_n(replaying, "replay", 60, 5000, 0);
                }
                emit(
                  cluster::dedup_identity_digest{.hi = i + 1, .lo = i + 1},
                  ts(1000 + static_cast<int64_t>(i)));
            }
            return ss::now();
        })
      .get();
    EXPECT_TRUE(interleaved);
    EXPECT_EQ(restoring.map_size(), 80u);
    EXPECT_EQ(budget->entries, restoring.map_size() + replaying.map_size());
    EXPECT_LE(budget->entries, shard_limit);
    // The replay got the room the restore left and no more: the restoring
    // index's claim was not its to take, and with nothing of its own older
    // than the incoming records to replace, the rest went unindexed.
    EXPECT_EQ(replaying.map_size(), 20u);
    EXPECT_EQ(replaying.unindexed_records(), 40u);

    // The shard is full, and both partitions keep indexing new identities.
    for (auto* f : {&restoring, &replaying}) {
        const auto unindexed = f->unindexed_records();
        f->populate(iobuf::from("fresh"), ts(9000));
        EXPECT_EQ(f->unindexed_records(), unindexed);
        EXPECT_LE(budget->entries, shard_limit);
        EXPECT_EQ(budget->entries, restoring.map_size() + replaying.map_size());
    }
}

// A shard above its limit, however it got there, frees enough on the next new
// identity to hold it rather than exactly one entry.
TEST(DedupShardBudget, AnOvershotShardGetsBackUnderItsLimit) {
    constexpr size_t shard_limit = 100;
    auto budget = make_budget(shard_limit);
    cluster::dedup_window_filter f(1'000s, 1'000, budget);
    // One timestamp, so the soft-limit sweep trims nothing on the way up.
    populate_n(f, "k", shard_limit, 1000, 0);
    ASSERT_EQ(budget->entries, shard_limit);
    // Stand in for entries counted against the shard from elsewhere.
    constexpr size_t overshoot = 5;
    budget->entries += overshoot;

    const auto unindexed = f.unindexed_records();
    f.populate(iobuf::from("fresh"), ts(9000));
    EXPECT_EQ(f.unindexed_records(), unindexed);
    EXPECT_EQ(budget->entries, shard_limit);
    EXPECT_EQ(f.map_size(), shard_limit - overshoot);
    budget->entries -= overshoot;
}

// Entries at the trim cutoff timestamp fill exactly the room that is left, so
// a restore keeps exactly what it may and never more.
TEST(DedupShardBudget, RestoreTrimsTiesAtTheCutoffExactly) {
    cluster::dedup_index_snapshot snap{.max_timestamp = ts(5000)};
    for (uint64_t i = 0; i < 40; ++i) {
        snap.entries.push_back(
          {.identity = {.hi = i + 1, .lo = (i + 1) * 0x9e37'79b9'7f4a'7c15ULL},
           .timestamp = ts(i < 30 ? 4000 : 5000)});
    }
    auto budget = make_budget(25);
    cluster::dedup_window_filter f(1'000s, 1'000, budget);
    f.restore(snap);
    EXPECT_EQ(f.map_size(), 25u);
    EXPECT_EQ(f.truncated_entries(), 15u);
    size_t newest = 0;
    for (const auto& entry : f.snapshot().entries) {
        newest += entry.timestamp == ts(5000) ? 1 : 0;
    }
    EXPECT_EQ(newest, 10u);
}

// With no shard budget, restore keeps everything up to the partition ceiling,
// as it always did.
TEST(DedupShardBudget, RestoreWithoutABudgetKeepsUpToTheCeiling) {
    cluster::dedup_window_filter source(1'000s, 1'000);
    populate_n(source, "r", 100, 1000, 1000);
    cluster::dedup_window_filter f(1'000s, 60);
    f.restore(source.snapshot());
    EXPECT_EQ(f.map_size(), 60u);
    EXPECT_EQ(f.truncated_entries(), 40u);
}

// A restart that fills the shard from snapshots must not leave dedup off until
// the cursor has walked the whole restored index: restore computes the
// capacity cutoff from what it restored, so the very first new identities
// already find something old enough to trim.
TEST(DedupShardBudget, ARestoredFullShardIndexesNewIdentitiesAtOnce) {
    constexpr size_t shard_limit = 5'000;
    auto budget = make_budget(shard_limit);
    cluster::dedup_window_filter source(1'000s, 100'000);
    populate_n(source, "old", shard_limit, 1000, 10);
    const auto snap = source.snapshot();

    cluster::dedup_window_filter f(1'000s, 100'000, budget);
    f.restore(snap);
    ASSERT_EQ(budget->entries, shard_limit);

    populate_n(f, "new", 200, 1000 + 10 * shard_limit);
    EXPECT_EQ(f.unindexed_records(), 0u);
    EXPECT_LE(budget->entries, shard_limit);
    for (size_t i = 0; i < 200; ++i) {
        EXPECT_FALSE(
          f.filter(make_batch(fmt::format("new-{}", i), "dup", ts(60'000)))
            .has_value())
          << i;
    }
}

// Two partitions filling the shard, one idle at half of it and one still
// receiving new identities. Neither is far larger, so the busy one trims
// itself rather than waiting for the idle one to give up room -- and it trims
// down into the shard's soft zone, as it would below its own ceiling, so its
// cursor always has headroom to find the next entries old enough to drop.
TEST(DedupShardBudget, TiedIndicesAtTheShardLimitKeepIndexing) {
    constexpr size_t shard_limit = 2'000;
    auto budget = make_budget(shard_limit);
    cluster::dedup_window_filter a(1'000s, 100'000, budget);
    cluster::dedup_window_filter b(1'000s, 100'000, budget);
    populate_n(a, "a", shard_limit / 2, 1000, 10);
    populate_n(b, "b", shard_limit / 2, 1000, 10);
    ASSERT_GE(budget->entries, shard_limit * 9 / 10);
    ASSERT_LE(budget->entries, shard_limit);
    ASSERT_EQ(b.unindexed_records(), 0u);

    populate_n(b, "b-new", 400, 1000 + 10 * shard_limit);
    EXPECT_EQ(b.unindexed_records(), 0u);
    EXPECT_LE(budget->entries, shard_limit);
    EXPECT_FALSE(
      b.filter(
         make_batch("b-new-399", "dup", ts(1000 + 10 * shard_limit + 399)))
        .has_value());
}

// Four partitions filled past the shard's budget, one after another, so the
// earlier ones were trimmed while full and their oldest survivors are
// scattered through their indices. After a restart restores them into a full
// shard, every new identity on every partition is indexed: the restore puts
// the entries old enough to trim at the front, where the cursor starts.
TEST(DedupShardBudget, PartitionsRestoredIntoAFullShardKeepIndexing) {
    constexpr size_t shard_limit = 40'000;
    constexpr size_t per_partition = shard_limit * 2 / 5;
    constexpr auto window = std::chrono::milliseconds{3'600'000};
    constexpr size_t no_ceiling = 1'000'000'000;
    auto budget = make_budget(shard_limit);
    std::vector<cluster::dedup_index_snapshot> snaps;
    int64_t clock = 1'000'000;
    {
        std::vector<std::unique_ptr<cluster::dedup_window_filter>> filters;
        for (int p = 0; p < 4; ++p) {
            filters.push_back(
              std::make_unique<cluster::dedup_window_filter>(
                window, no_ceiling, budget));
        }
        for (size_t p = 0; p < filters.size(); ++p) {
            for (size_t i = 0; i < per_partition; ++i) {
                filters[p]->populate(
                  iobuf::from(fmt::format("p{}-{}", p, i)),
                  ts(clock + static_cast<int64_t>(i / 50)));
            }
            clock += static_cast<int64_t>(per_partition / 50);
        }
        ASSERT_GE(budget->entries, shard_limit * 9 / 10);
        for (auto& f : filters) {
            snaps.push_back(f->snapshot());
        }
    }
    ASSERT_EQ(budget->entries, 0u);

    std::vector<std::unique_ptr<cluster::dedup_window_filter>> restored;
    for (const auto& snap : snaps) {
        restored.push_back(
          std::make_unique<cluster::dedup_window_filter>(
            window, no_ceiling, budget));
        restored.back()->restore(snap);
    }
    ASSERT_GE(budget->entries, shard_limit * 9 / 10);

    for (size_t p = 0; p < restored.size(); ++p) {
        for (int64_t i = 0; i < 1'000; ++i) {
            restored[p]->populate(
              iobuf::from(fmt::format("probe{}-{}", p, i)),
              ts(clock + 1'000 + i));
        }
        EXPECT_EQ(restored[p]->unindexed_records(), 0u) << "partition " << p;
        EXPECT_LE(budget->entries, shard_limit);
    }
}

// Indices built on a larger shard, restarted into one with half the budget:
// some partitions restore in full, later ones are cut short, and each then
// replays the log past its snapshot. That replay takes its room from the
// largest index, which drives the largest one's cursor through all its entries
// below the capacity cutoff long before the pass ends. New identities that
// arrive afterwards still get indexed -- each replaces the oldest entry the
// bounded step walked past -- instead of being admitted unindexed until the
// cursor wraps.
TEST(DedupShardBudget, AfterReplayIntoASmallerShardNewIdentitiesAreIndexed) {
    constexpr size_t big_limit = 40'000;
    constexpr size_t per_partition = big_limit * 22 / 100;
    constexpr std::array<double, 4> snapshot_at{0.65, 1.0, 0.9, 1.0};
    constexpr auto window = std::chrono::milliseconds{3'600'000};
    constexpr size_t no_ceiling = 1'000'000'000;
    auto key = [](size_t p, size_t i) {
        return iobuf::from(fmt::format("p{}-{}", p, i));
    };
    struct partition {
        cluster::dedup_index_snapshot snap;
        size_t snapshotted;
        int64_t first_ts;
    };
    std::vector<partition> parts;
    int64_t clock = 1'000'000;
    {
        auto budget = make_budget(big_limit);
        for (size_t p = 0; p < snapshot_at.size(); ++p) {
            cluster::dedup_window_filter f(window, no_ceiling, budget);
            const auto cut = static_cast<size_t>(
              static_cast<double>(per_partition) * snapshot_at[p]);
            for (size_t i = 0; i < per_partition; ++i) {
                if (i == cut) {
                    parts.push_back({f.snapshot(), cut, clock});
                }
                f.populate(key(p, i), ts(clock + static_cast<int64_t>(i / 50)));
            }
            if (cut == per_partition) {
                parts.push_back({f.snapshot(), cut, clock});
            }
            clock += static_cast<int64_t>(per_partition / 50);
        }
    }

    auto budget = make_budget(big_limit / 2);
    std::array<std::unique_ptr<cluster::dedup_window_filter>, 4> restored;
    for (const size_t p : {0UL, 2UL, 1UL, 3UL}) {
        restored[p] = std::make_unique<cluster::dedup_window_filter>(
          window, no_ceiling, budget);
        restored[p]->restore(parts[p].snap);
        for (size_t i = parts[p].snapshotted; i < per_partition; ++i) {
            restored[p]->populate(
              key(p, i), ts(parts[p].first_ts + static_cast<int64_t>(i / 50)));
        }
    }
    ASSERT_LE(budget->entries, big_limit / 2);

    for (size_t p = 0; p < restored.size(); ++p) {
        const auto before = restored[p]->unindexed_records();
        for (int64_t i = 0; i < 500; ++i) {
            restored[p]->populate(
              iobuf::from(fmt::format("new{}-{}", p, i)),
              ts(clock + 10'000 + i));
        }
        EXPECT_EQ(restored[p]->unindexed_records(), before)
          << "partition " << p;
        EXPECT_LE(budget->entries, big_limit / 2);
    }
}

// Many small idle indices filling the shard between them, none larger than a
// tenth of the shard: a partition that starts receiving new identities must
// still take room from them and grow, rather than count as one of the largest
// because they are all small and so only ever trim its own few entries.
TEST(DedupShardBudget, ManySmallIdleIndicesCannotStarveABusyOne) {
    constexpr size_t shard_limit = 20'000;
    constexpr size_t idle_count = 20;
    constexpr auto window = std::chrono::milliseconds{3'600'000};
    constexpr size_t no_ceiling = 1'000'000'000;
    auto budget = make_budget(shard_limit);
    std::vector<std::unique_ptr<cluster::dedup_window_filter>> idle;
    for (size_t j = 0; j < idle_count; ++j) {
        idle.push_back(
          std::make_unique<cluster::dedup_window_filter>(
            window, no_ceiling, budget));
        populate_n(
          *idle.back(),
          fmt::format("idle{}", j),
          shard_limit / idle_count,
          1000 + static_cast<int64_t>(j) * 1000);
    }
    ASSERT_GE(budget->entries, shard_limit * 9 / 10);

    cluster::dedup_window_filter busy(window, no_ceiling, budget);
    populate_n(busy, "busy", 2'000, 1'000'000);
    EXPECT_LE(budget->entries, shard_limit);
    EXPECT_GT(busy.map_size(), shard_limit / idle_count / 2);
    EXPECT_FALSE(
      busy.filter(make_batch("busy-1999", "dup", ts(1'000'000 + 1'999)))
        .has_value());
}

// An index that is emptied stops being the shard's largest at once, rather
// than being offered as the one to take room from until the next refresh.
TEST(DedupShardBudget, AnEmptiedIndexIsNoLongerTheLargest) {
    auto budget = make_budget(1'000);
    cluster::dedup_window_filter big(1'000s, 10'000, budget);
    cluster::dedup_window_filter small(1'000s, 10'000, budget);
    populate_n(big, "big", 500, 1000, 10);
    populate_n(small, "small", 50, 1000, 10);
    ASSERT_EQ(budget->largest(), &big);

    big.clear();
    EXPECT_EQ(budget->largest(), &small);

    populate_n(big, "big-again", 500, 10'000, 10);
    budget->largest_cache = &big;
    big.restore(cluster::dedup_index_snapshot{});
    EXPECT_EQ(budget->largest(), &small);
}

// An index written in a burst covers a sliver of a long window. The capacity
// cutoff is measured against the timestamps the index holds rather than the
// whole window, so it still marks the oldest quarter instead of nothing.
TEST(DedupWindowFilter, CapacityCutoffResolvesABurstInALongWindow) {
    constexpr size_t cap = 1'000;
    cluster::dedup_window_filter f(24h, cap);
    // 1000 identities over one second of a 24 hour window: all of them fall
    // in one bucket of a window-wide frame.
    populate_n(f, "burst", cap, 1'000'000, 1);
    populate_n(f, "more", 400, 1'001'000, 1);
    EXPECT_LE(f.unindexed_records(), cap / 64);
    EXPECT_GT(f.capacity_evicted_entries(), 0u);
    EXPECT_LE(f.map_size(), cap);
}

// The same after a restart, where both restore as far as the shard allows.
TEST(DedupShardBudget, RestoredPartitionsSharingAFullShardKeepIndexing) {
    constexpr size_t shard_limit = 4'000;
    auto budget = make_budget(shard_limit);
    std::vector<cluster::dedup_index_snapshot> snaps;
    size_t held = 0;
    {
        cluster::dedup_window_filter a(1'000s, 100'000, budget);
        cluster::dedup_window_filter b(1'000s, 100'000, budget);
        populate_n(a, "a", shard_limit / 2, 1000, 10);
        populate_n(b, "b", shard_limit / 2, 1000, 10);
        snaps.push_back(a.snapshot());
        snaps.push_back(b.snapshot());
        held = budget->entries;
    }
    ASSERT_EQ(budget->entries, 0u);
    ASSERT_GE(held, shard_limit * 4 / 5);
    cluster::dedup_window_filter a(1'000s, 100'000, budget);
    cluster::dedup_window_filter b(1'000s, 100'000, budget);
    a.restore(snaps[0]);
    b.restore(snaps[1]);
    ASSERT_EQ(budget->entries, held);

    for (auto* f : {&a, &b}) {
        populate_n(*f, fmt::format("new-{}", f == &a ? "a" : "b"), 300, 50'000);
        EXPECT_EQ(f->unindexed_records(), 0u);
        EXPECT_LE(budget->entries, shard_limit);
    }
}

// The leader's and the follower's paths must still agree with each other when
// the shard budget is what is doing the trimming, given the same shard state.
TEST(DedupShardBudget, LeaderAndFollowerPathsAgreeUnderShardPressure) {
    constexpr size_t shard_limit = 40;
    auto leader_budget = make_budget(shard_limit);
    auto follower_budget = make_budget(shard_limit);
    cluster::dedup_window_filter leader(100s, 1'000, leader_budget);
    cluster::dedup_window_filter follower(100s, 1'000, follower_budget);

    for (size_t i = 0; i < 500; ++i) {
        const auto key = fmt::format("id-{}", i);
        const auto stamp = ts(1000 + static_cast<int64_t>(i) * 150);
        ASSERT_TRUE(leader.filter(make_batch(key, "v", stamp)).has_value());
        follower.populate(iobuf::from(key), stamp);
    }
    ASSERT_GT(leader.capacity_evicted_entries(), 0u);
    EXPECT_EQ(leader.snapshot(), follower.snapshot());
    EXPECT_EQ(leader_budget->entries, follower_budget->entries);
    EXPECT_EQ(
      leader.capacity_evicted_entries(), follower.capacity_evicted_entries());
}

// The same, when the room comes from replacing the oldest entry of another
// index: an idle index whose entries share one timestamp never forms a
// capacity cutoff, so every slot the busy partition gets is a replacement.
// The leader's produce path and the follower's apply path must still make
// the same choice on both indices.
TEST(DedupShardBudget, LeaderAndFollowerAgreeWhenReplacing) {
    constexpr size_t shard_limit = 50;
    auto leader_budget = make_budget(shard_limit);
    auto follower_budget = make_budget(shard_limit);
    cluster::dedup_window_filter leader_idle(100s, 1'000, leader_budget);
    cluster::dedup_window_filter leader_busy(100s, 1'000, leader_budget);
    cluster::dedup_window_filter follower_idle(100s, 1'000, follower_budget);
    cluster::dedup_window_filter follower_busy(100s, 1'000, follower_budget);
    populate_n(leader_idle, "idle", 40, 1000, 0);
    populate_n(follower_idle, "idle", 40, 1000, 0);

    for (size_t i = 0; i < 200; ++i) {
        const auto key = fmt::format("busy-{}", i);
        const auto stamp = ts(2000 + static_cast<int64_t>(i));
        ASSERT_TRUE(
          leader_busy.filter(make_batch(key, "v", stamp)).has_value());
        follower_busy.populate(iobuf::from(key), stamp);
    }
    ASSERT_GT(leader_idle.capacity_evicted_entries(), 0u);
    EXPECT_EQ(leader_busy.unindexed_records(), 0u);
    EXPECT_EQ(leader_idle.snapshot(), follower_idle.snapshot());
    EXPECT_EQ(leader_busy.snapshot(), follower_busy.snapshot());
    EXPECT_EQ(leader_budget->entries, follower_budget->entries);
    EXPECT_LE(leader_budget->entries, shard_limit);
}

// Replacement keeps the newer identity, never an older one: an identity older
// than everything in the full shard is admitted unindexed, and nothing is
// evicted for it.
TEST(DedupShardBudget, ReplacementNeverDropsANewerEntry) {
    constexpr size_t shard_limit = 50;
    auto budget = make_budget(shard_limit);
    cluster::dedup_window_filter full(100s, 1'000, budget);
    cluster::dedup_window_filter late(100s, 1'000, budget);
    populate_n(full, "full", shard_limit, 5000, 0);
    ASSERT_EQ(budget->entries, shard_limit);

    late.populate(iobuf::from("older"), ts(1000));
    EXPECT_EQ(late.unindexed_records(), 1u);
    EXPECT_EQ(late.map_size(), 0u);
    EXPECT_EQ(full.capacity_evicted_entries(), 0u);
    EXPECT_EQ(full.map_size(), shard_limit);
}

// Shard pressure trims down to the shard's soft limit and no further: the
// condition is re-checked as each entry is dropped, so a sweep step does not
// keep trimming live identities once its own erases have brought the shard
// back under it.
TEST(DedupShardBudget, ShardPressureTrimsNoFurtherThanTheSoftLimit) {
    constexpr size_t shard_limit = 1'000;
    auto budget = make_budget(shard_limit);
    cluster::dedup_window_filter f(1'000s, 100'000, budget);
    bool reached = false;
    size_t lowest = shard_limit;
    for (size_t i = 0; i < 3'000; ++i) {
        f.populate(
          iobuf::from(fmt::format("k{}", i)),
          ts(1000 + static_cast<int64_t>(i) * 10));
        reached = reached || f.map_size() >= shard_limit;
        if (reached) {
            lowest = std::min(lowest, f.map_size());
        }
    }
    ASSERT_TRUE(reached);
    EXPECT_GE(lowest, shard_limit * 9 / 10);
    EXPECT_EQ(f.unindexed_records(), 0u);
}

// Randomized: whatever mix of operations runs against several filters, the
// budget equals the sum of their sizes after every one of them.
TEST(DedupShardBudget, TotalMatchesTheIndicesUnderRandomOperations) {
    auto budget = make_budget(300);
    std::vector<std::unique_ptr<cluster::dedup_window_filter>> filters;
    for (int i = 0; i < 3; ++i) {
        filters.push_back(
          std::make_unique<cluster::dedup_window_filter>(5s, 150, budget));
    }
    std::mt19937 rng(20261013); // NOLINT(cert-msc51-cpp): reproducible
    int64_t clock = 1000;
    std::vector<cluster::dedup_request_undo> pending;
    for (int step = 0; step < 20'000; ++step) {
        auto& f = *filters[rng() % filters.size()];
        clock += static_cast<int64_t>(rng() % 50);
        const auto key = fmt::format("k-{}", rng() % 700);
        switch (rng() % 100) {
        case 0:
            f.clear();
            break;
        case 1:
            f.restore(f.snapshot());
            break;
        case 2:
            if (!pending.empty()) {
                f.revert_request(pending.back());
                pending.pop_back();
            }
            break;
        default:
            if (rng() % 2 == 0) {
                f.populate(iobuf::from(key), ts(clock));
            } else {
                auto result = f.filter_request(make_batch(key, "v", ts(clock)));
                if (rng() % 10 == 0) {
                    pending.push_back(std::move(result.undo));
                }
            }
        }
        size_t total = 0;
        for (auto& g : filters) {
            total += g->map_size();
        }
        ASSERT_EQ(budget->entries, total) << "step " << step;
    }
    filters.clear();
    EXPECT_EQ(budget->entries, 0u);
}

// --- The compact index under the filter ---
//
// The index keeps 96 bits of each digest and a 32-bit timestamp relative to a
// block of entries (dedup_compact_index). What that may change is bounded: an
// identity is its 96 bits, and an entry whose timestamp a block cannot keep
// beside much newer ones reads as expired, so it can stop matching but never
// make a match that should not happen.

TEST(DedupCompactIndexInTheFilter, IdentitiesAre96Bits) {
    cluster::dedup_window_filter f(1000ms);
    const auto d = cluster::dedup_digest_of(iobuf::from("k"));
    EXPECT_EQ(d.hi & ~cluster::dedup_digest_hi_mask, 0u);
    f.populate(d, ts(1000));
    // The same 96 bits with anything above them is the same identity, as a
    // digest read from a snapshot written before the index kept 96 bits is.
    f.populate(
      cluster::dedup_identity_digest{
        .hi = d.hi | (0xdeadULL << 40), .lo = d.lo},
      ts(1500));
    EXPECT_EQ(f.map_size(), 1u);
    const auto snap = f.snapshot();
    ASSERT_EQ(snap.entries.size(), 1u);
    EXPECT_EQ(snap.entries[0].identity, d);
    EXPECT_EQ(snap.entries[0].timestamp, ts(1500));
    EXPECT_FALSE(f.filter(make_batch("k", "dup", ts(1600))).has_value());
}

// A window longer than the span one block keeps exactly (about 37 days): an
// entry far enough behind a newer one in its block is kept as expired. The
// duplicate is admitted -- coverage lost, as at the entry limit -- rather than
// dropped against a timestamp the index no longer has.
TEST(DedupCompactIndexInTheFilter, AnEntryKeptAsExpiredIsNeverAMatch) {
    constexpr int64_t day = 86'400'000;
    cluster::dedup_window_filter f(std::chrono::days{60});
    ASSERT_TRUE(f.filter(make_batch("old", "1", ts(day))).has_value());
    // Fifty days later, in the same block: the old entry cannot stay beside
    // it.
    ASSERT_TRUE(f.filter(make_batch("new", "1", ts(51 * day))).has_value());
    EXPECT_GT(f.index_stats().rebases, 0u);
    EXPECT_EQ(f.map_size(), 2u);
    // Within the configured window of both its old timestamp and the new one,
    // but the index cannot vouch for it any more, and cannot keep it beside
    // the newer entry when it is written again either.
    EXPECT_TRUE(f.filter(make_batch("old", "2", ts(day))).has_value());
    EXPECT_TRUE(f.filter(make_batch("old", "3", ts(day + 1))).has_value());
    EXPECT_EQ(f.dropped_records(), 0u);
    // Newer identities deduplicate as usual, and so does the old one once it
    // is written close enough to them.
    EXPECT_FALSE(f.filter(make_batch("new", "2", ts(51 * day))).has_value());
    EXPECT_TRUE(f.filter(make_batch("old", "4", ts(40 * day))).has_value());
    EXPECT_FALSE(f.filter(make_batch("old", "5", ts(40 * day))).has_value());
}

// Within the span a block keeps, nothing is lost however the index moves its
// entries around: identities spread over 36 days, erased and refilled across
// blocks, still deduplicate against exactly the timestamps they were admitted
// with.
TEST(DedupCompactIndexInTheFilter, WindowsWithinTheBlockSpanAreExact) {
    constexpr int64_t day = 86'400'000;
    cluster::dedup_window_filter f(std::chrono::days{36});
    constexpr int keys = 3 * cluster::dedup_compact_index::block_entries;
    auto stamp = [](int i) { return ts(day + (i % 36) * day + i); };
    for (int i = 0; i < keys; ++i) {
        ASSERT_TRUE(f.filter(make_batch(fmt::format("k{}", i), "1", stamp(i)))
                      .has_value());
    }
    ASSERT_EQ(f.map_size(), static_cast<size_t>(keys));
    // Every identity, exactly at its own timestamp and one window either side
    // of it: a duplicate at the edges, admitted one millisecond beyond.
    for (int i = 0; i < keys; i += 7) {
        const auto key = fmt::format("k{}", i);
        EXPECT_FALSE(f.filter(make_batch(key, "dup", stamp(i))).has_value())
          << i;
    }
    const auto snap = f.snapshot();
    ASSERT_EQ(snap.entries.size(), static_cast<size_t>(keys));
    std::set<int64_t> stamps;
    for (const auto& e : snap.entries) {
        stamps.insert(e.timestamp.value());
    }
    for (int i = 0; i < keys; ++i) {
        EXPECT_TRUE(stamps.contains(stamp(i).value())) << i;
    }
}

// The index's table refuses an identity rather than search without bound
// when digests collide on everything it hashes by. That cannot happen with
// xxhash64 digests of distinct identities short of a deliberate search for
// them; when it does, the record is admitted unindexed and counted, as at
// the entry limit.
TEST(DedupCompactIndexInTheFilter, RefusedIdentitiesAreAdmittedUnindexed) {
    cluster::dedup_window_filter f(1000s);
    size_t refused = 0;
    for (uint64_t i = 0; i < 1000; ++i) {
        const auto before = f.map_size();
        f.populate(
          cluster::dedup_identity_digest{
            .hi = i, .lo = 0x1234'5678'9abc'def0ULL},
          ts(1000));
        refused += f.map_size() == before ? 1 : 0;
    }
    EXPECT_GT(refused, 0u);
    EXPECT_EQ(f.unindexed_records(), refused);
    EXPECT_EQ(f.map_size() + refused, 1000u);
}

// A snapshot written before the broker-time clamp can carry entries from the
// future as well as a future watermark. Restored as they are, one of them
// would hold its block's newest timestamp decades ahead, and the index could
// keep none of the block's real entries beside it: every one would read as
// expired, and stay so. They are clamped to the restored watermark instead.
TEST(DedupCompactIndexInTheFilter, FutureEntriesInASnapshotAreClamped) {
    cluster::dedup_window_filter f(5min);
    const auto now = broker_now();
    cluster::dedup_index_snapshot snap;
    snap.max_timestamp = model::timestamp{4'102'444'800'000}; // 2100-01-01
    snap.entries.push_back(
      {.identity = cluster::dedup_digest_of(iobuf::from("future")),
       .timestamp = snap.max_timestamp});
    for (int i = 0; i < 100; ++i) {
        snap.entries.push_back(
          {.identity = cluster::dedup_digest_of(
             iobuf::from(fmt::format("real-{}", i))),
           .timestamp = now});
    }
    f.restore(snap);
    ASSERT_EQ(f.map_size(), 101u);
    EXPECT_LE(f.max_timestamp().value(), broker_now().value());
    for (const auto& entry : f.snapshot().entries) {
        EXPECT_LE(entry.timestamp.value(), f.max_timestamp().value());
    }
    // The real entries still deduplicate, and so does the clamped one.
    for (int i = 0; i < 100; ++i) {
        EXPECT_FALSE(f.filter(make_batch(fmt::format("real-{}", i), "dup", now))
                       .has_value())
          << i;
    }
    EXPECT_FALSE(f.filter(make_batch("future", "dup", now)).has_value());
}
