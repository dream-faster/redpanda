// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/bytes.h"
#include "cluster/dedup_stm.h"
#include "cluster/logger.h"
#include "cluster/snapshot.h"
#include "config/mock_property.h"
#include "model/batch_builder.h"
#include "model/record_utils.h"
#include "raft/tests/raft_fixture.h"
#include "serde/envelope.h"
#include "serde/rw/envelope.h"
#include "serde/rw/vector.h"
#include "serde/serde_exception.h"
#include "storage/kvstore.h"
#include "storage/record_batch_builder.h"
#include "test_utils/test.h"

#include <seastar/core/byteorder.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <iterator>
#include <map>
#include <optional>
#include <string>
#include <string_view>

using namespace std::chrono_literals;

namespace cluster {

struct dedup_stm_test_accessor {
    static size_t snapshot_size(iobuf snapshot) {
        return serde::from_iobuf<dedup_stm::state_snapshot>(std::move(snapshot))
                 .entries.size_bytes()
               / dedup_stm::packed_entry_size;
    }

    /// The entry layout and the entries' size in bytes of a snapshot.
    static std::pair<uint32_t, size_t> snapshot_layout(iobuf snapshot) {
        auto parsed = serde::from_iobuf<dedup_stm::state_snapshot>(
          std::move(snapshot));
        return {parsed.entry_size, parsed.entries.size_bytes()};
    }

    static iobuf
    state_snapshot_with_entry_size(iobuf entries, uint32_t entry_size) {
        return serde::to_iobuf(
          dedup_stm::state_snapshot{
            .window_ms = 1000,
            .generation = 1,
            .entries = std::move(entries),
            .entry_size = entry_size});
    }

    static ss::future<raft::local_snapshot_applied>
    apply_local_snapshot(dedup_stm& stm, iobuf buffer) {
        co_return co_await stm.apply_local_snapshot(
          raft::stm_snapshot_header{}, std::move(buffer));
    }

    static ss::future<> apply_raft_snapshot(dedup_stm& stm, iobuf buffer) {
        co_await stm.apply_raft_snapshot(buffer);
    }

    using pending_identities = dedup_stm::pending_identities;

    static iobuf serialize(
      dedup_window_filter& state,
      int64_t generation,
      const pending_identities* pending = nullptr) {
        return dedup_stm::serialize_snapshot(state, generation, pending).get();
    }

    static dedup_stm::inflight_ptr make_inflight() {
        return ss::make_lw_shared<dedup_stm::inflight_request>();
    }

    static size_t pending_size(const dedup_stm& stm) {
        return stm._pending.size();
    }

    static size_t resolving_size(const dedup_stm& stm) {
        return stm._resolving.size();
    }

    static uint64_t batches_skipped(const dedup_stm& stm) {
        return stm._batches_skipped;
    }

    static void
    restore(dedup_window_filter& state, int64_t& generation, iobuf buffer) {
        dedup_stm::restore_snapshot(
          state,
          generation,
          serde::from_iobuf<dedup_stm::state_snapshot>(std::move(buffer)))
          .get();
    }

    static iobuf state_snapshot_with_entry_bytes(size_t bytes) {
        auto entries = iobuf::from(std::string(bytes, 'x'));
        return serde::to_iobuf(
          dedup_stm::state_snapshot{
            .window_ms = 1000, .generation = 1, .entries = std::move(entries)});
    }

    static iobuf
    state_snapshot_with_entries(iobuf entries, model::timestamp max_timestamp) {
        return serde::to_iobuf(
          dedup_stm::state_snapshot{
            .window_ms = 3'600'000,
            .generation = 1,
            .max_timestamp = max_timestamp,
            .entries = std::move(entries)});
    }

    static bytes legacy_kvstore_key(const model::ntp& ntp) {
        return bytes::from_string(
          raft::kvstore_backed_stm_snapshot::snapshot_key(
            dedup_stm_snapshot, ntp));
    }
};

namespace {

model::record_batch make_batch(
  std::string_view key, std::string_view value, model::timestamp timestamp) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset{0});
    builder.set_timestamp(timestamp);
    builder.add_raw_kv(iobuf::from(key), iobuf::from(value));
    return std::move(builder).build();
}

// Build a single-record batch with a Kafka key and, optionally, one header
// (name, value), for exercising header-based dedup identity end to end.
model::record_batch make_batch_with_header(
  std::string_view key,
  std::string_view value,
  model::timestamp timestamp,
  std::optional<std::pair<std::string_view, std::string_view>> header) {
    model::batch_builder builder;
    builder.set_batch_type(model::record_batch_type::raft_data);
    builder.set_batch_timestamp(model::timestamp_type::create_time, timestamp);
    chunked_vector<model::record_header> hdrs;
    if (header) {
        hdrs.emplace_back(
          iobuf::from(header->first), iobuf::from(header->second));
    }
    builder.add_record(
      model::record(
        {}, 0, 0, iobuf::from(key), iobuf::from(value), std::move(hdrs)));
    return std::move(builder).build_sync();
}

// The packed entry encoding is hand-written rather than serde's, so a test is
// what keeps the writer and reader in step. Sizes include zero and enough
// entries to span several iobuf fragments.
TEST(dedup_stm_snapshot, packed_entries_round_trip) {
    for (const size_t count : {0UL, 1UL, 5461UL, 5462UL, 100'000UL}) {
        SCOPED_TRACE(count);
        dedup_window_filter source(1h);
        dedup_index_snapshot seed{
          .max_timestamp = model::timestamp{1'000'000'000 + 999}};
        for (size_t i = 0; i < count; ++i) {
            seed.entries.push_back(
              {.identity
               = {.hi = 0xbf58'476d'1ce4'e5b9ULL * (i + 1), .lo = 0x9e37'79b9'7f4a'7c15ULL * (i + 1)},
               .timestamp = model::timestamp{
                 1'000'000'000 + static_cast<int64_t>(i % 1000)}});
        }
        source.restore(seed);
        ASSERT_EQ(source.map_size(), count);

        auto buffer = dedup_stm_test_accessor::serialize(source, 7);
        ASSERT_EQ(dedup_stm_test_accessor::snapshot_size(buffer.copy()), count);

        dedup_window_filter target(1ms);
        int64_t generation = 0;
        dedup_stm_test_accessor::restore(target, generation, std::move(buffer));
        ASSERT_EQ(generation, 7);
        ASSERT_EQ(target.window(), source.window());
        ASSERT_EQ(target.snapshot(), source.snapshot());
    }
}

// A blob that is not a whole number of entries is corrupt, and must fail the
// read -- which try_restore_snapshot() turns into a discard and rebuild --
// rather than restore a partial last entry.
// Identities introduced by requests raft has not yet confirmed appended are
// left out of a snapshot: after a crash, a snapshot holding one would drop
// the producer's retry of a record no replica ever wrote.
TEST(dedup_stm_snapshot, pending_identities_are_left_out) {
    dedup_window_filter state(1000ms);
    state.populate(iobuf::from("a"), model::timestamp{1000});
    state.populate(iobuf::from("b"), model::timestamp{1000});
    state.populate(iobuf::from("c"), model::timestamp{1000});

    dedup_stm_test_accessor::pending_identities pending;
    pending.emplace(
      dedup_digest_of(iobuf::from("b")),
      dedup_stm_test_accessor::make_inflight());
    auto with_pending = dedup_stm_test_accessor::serialize(state, 1, &pending);
    ASSERT_EQ(
      dedup_stm_test_accessor::snapshot_size(std::move(with_pending)), 2);

    int64_t generation = 0;
    dedup_window_filter restored(1000ms);
    dedup_stm_test_accessor::restore(
      restored,
      generation,
      dedup_stm_test_accessor::serialize(state, 1, &pending));
    ASSERT_EQ(restored.map_size(), 2);
    // "b" is not in the restored index, so a retry of it is admitted.
    auto retry = restored.filter_request(
      make_batch("b", "value", model::timestamp{1000}));
    ASSERT_TRUE(retry.batch.has_value());

    // Nothing pending: everything is written.
    auto all = dedup_stm_test_accessor::serialize(state, 1);
    ASSERT_EQ(dedup_stm_test_accessor::snapshot_size(std::move(all)), 3);
    // The pending request's promise is never resolved here; dropping it
    // with no waiter is fine.
    pending.clear();
}

TEST(dedup_stm_snapshot, misaligned_entries_are_rejected) {
    dedup_window_filter target(1h);
    int64_t generation = 0;
    EXPECT_THROW(
      dedup_stm_test_accessor::restore(
        target,
        generation,
        dedup_stm_test_accessor::state_snapshot_with_entry_bytes(
          dedup_stm::packed_entry_size + 1)),
      serde::serde_exception);
    EXPECT_EQ(target.map_size(), 0u);
}

namespace {

/// The version 1 layout, which the STM no longer carries: one serde envelope
/// per entry. Reproduced to prove such a snapshot is refused, not misread.
struct v1_wire_entry
  : serde::
      envelope<v1_wire_entry, serde::version<1>, serde::compat_version<1>> {
    uint64_t identity_hi{0};
    uint64_t identity_lo{0};
    model::timestamp timestamp;

    auto serde_fields() {
        return std::tie(identity_hi, identity_lo, timestamp);
    }
};

struct v1_state_snapshot
  : serde::
      envelope<v1_state_snapshot, serde::version<1>, serde::compat_version<1>> {
    int64_t window_ms{0};
    int64_t generation{0};
    model::timestamp max_timestamp{model::timestamp::min()};
    uint64_t legacy_inserts_since_evict{0};
    chunked_vector<v1_wire_entry> entries;

    auto serde_fields() {
        return std::tie(
          window_ms,
          generation,
          max_timestamp,
          legacy_inserts_since_evict,
          entries);
    }
};

iobuf v1_snapshot_with(size_t entries) {
    v1_state_snapshot snapshot{
      .window_ms = 300'000,
      .generation = 0,
      .max_timestamp = model::timestamp{1'000'000}};
    for (size_t i = 0; i < entries; ++i) {
        snapshot.entries.push_back(
          {.identity_hi = i + 1,
           .identity_lo = ~uint64_t{i},
           .timestamp = model::timestamp{1'000'000}});
    }
    return serde::to_iobuf(std::move(snapshot));
}

} // namespace

namespace {

/// The version 2 layout as a version 2 binary writes and reads it: 24-byte
/// entries carrying all 64 bits of digest hi, and no entry_size field.
struct v2_state_snapshot
  : serde::
      envelope<v2_state_snapshot, serde::version<2>, serde::compat_version<2>> {
    int64_t window_ms{0};
    int64_t generation{0};
    model::timestamp max_timestamp{model::timestamp::min()};
    iobuf entries;

    auto serde_fields() {
        return std::tie(window_ms, generation, max_timestamp, entries);
    }
};

} // namespace

// A version 2 snapshot still restores, and the digests it carries -- 128 bits
// each, as version 2 binaries computed them -- become the 96-bit identities
// this build computes, so they deduplicate as before the upgrade.
TEST(dedup_stm_snapshot, version_2_snapshot_restores_as_96_bit_identities) {
    const auto canonical = dedup_digest_of(iobuf::from("key"));
    const dedup_identity_digest wide{
      .hi = canonical.hi | 0xa0e1'ea34'0000'0000ULL, .lo = canonical.lo};
    iobuf entries;
    for (const auto& [id, ts] :
         {std::pair{wide, int64_t{1'000'000}},
          std::pair{
            dedup_identity_digest{.hi = (7ULL << 40) | 3, .lo = 9},
            int64_t{999'999}}}) {
        const auto hi = ss::cpu_to_le(id.hi);
        const auto lo = ss::cpu_to_le(id.lo);
        const auto le_ts = ss::cpu_to_le(ts);
        entries.append(reinterpret_cast<const char*>(&hi), sizeof(hi));
        entries.append(reinterpret_cast<const char*>(&lo), sizeof(lo));
        entries.append(reinterpret_cast<const char*>(&le_ts), sizeof(le_ts));
    }
    auto buffer = serde::to_iobuf(
      v2_state_snapshot{
        .window_ms = 3'600'000,
        .generation = 4,
        .max_timestamp = model::timestamp{1'000'000},
        .entries = std::move(entries)});

    dedup_window_filter target(1h);
    int64_t generation = 0;
    dedup_stm_test_accessor::restore(target, generation, std::move(buffer));
    EXPECT_EQ(generation, 4);
    ASSERT_EQ(target.map_size(), 2u);
    const auto restored = target.snapshot().entries;
    EXPECT_EQ(restored[0].identity, canonical);
    EXPECT_EQ(restored[1].identity, (dedup_identity_digest{.hi = 3, .lo = 9}));
    // The identity this build digests from the same key finds it.
    target.populate(iobuf::from("key"), model::timestamp{1'000'100});
    EXPECT_EQ(target.map_size(), 2u);
    EXPECT_EQ(target.unindexed_records(), 0u);

    // Written back out, it takes the current layout.
    auto rewritten = dedup_stm_test_accessor::serialize(target, generation);
    const auto [entry_size, entry_bytes]
      = dedup_stm_test_accessor::snapshot_layout(rewritten.copy());
    EXPECT_EQ(entry_size, dedup_stm::packed_entry_size);
    EXPECT_EQ(entry_bytes, 2 * dedup_stm::packed_entry_size);
    dedup_window_filter again(1h);
    dedup_stm_test_accessor::restore(again, generation, std::move(rewritten));
    EXPECT_EQ(again.snapshot(), target.snapshot());
}

// A version 2 binary refuses the current layout on compat version, and so
// rebuilds from the log, rather than restoring 96-bit digests it would never
// match against the 128-bit ones it computes.
TEST(dedup_stm_snapshot, version_2_reader_refuses_the_current_layout) {
    dedup_window_filter source(1h);
    source.populate(iobuf::from("key"), model::timestamp{1'000'000});
    auto buffer = dedup_stm_test_accessor::serialize(source, 1);
    EXPECT_THROW(
      serde::from_iobuf<v2_state_snapshot>(std::move(buffer)),
      serde::serde_exception);
}

// An entry size this build does not know is refused rather than misread.
TEST(dedup_stm_snapshot, unknown_entry_size_is_refused) {
    dedup_window_filter target(1h);
    int64_t generation = 0;
    EXPECT_THROW(
      dedup_stm_test_accessor::restore(
        target,
        generation,
        dedup_stm_test_accessor::state_snapshot_with_entry_size(
          iobuf::from(std::string(32, 'x')), 16)),
      serde::serde_exception);
    EXPECT_EQ(target.map_size(), 0u);
}

TEST(dedup_stm_snapshot, version_1_snapshot_is_refused) {
    dedup_window_filter target(1h);
    int64_t generation = 0;
    EXPECT_THROW(
      dedup_stm_test_accessor::restore(target, generation, v1_snapshot_with(3)),
      serde::serde_exception);
    EXPECT_EQ(target.map_size(), 0u);
}

// Entries outside the window are left out of the snapshot: they could never
// match again, so persisting them would only cost memory and disk.
TEST(dedup_stm_snapshot, expired_entries_are_not_serialized) {
    dedup_window_filter source(1000ms);
    dedup_index_snapshot seed{.max_timestamp = model::timestamp{1'000'000}};
    for (size_t i = 0; i < 10; ++i) {
        seed.entries.push_back(
          {.identity = {.hi = i + 1, .lo = (i + 1) * 0x9e37'79b9'7f4a'7c15ULL},
           .timestamp = model::timestamp{i < 4 ? 1'000 : 999'500}});
    }
    source.restore(seed);
    ASSERT_EQ(source.map_size(), 10u);
    auto buffer = dedup_stm_test_accessor::serialize(source, 0);
    EXPECT_EQ(dedup_stm_test_accessor::snapshot_size(std::move(buffer)), 6u);
}

// A snapshot from a broker with a larger per-partition ceiling is cut to this
// broker's ceiling on restore, keeping the newest identities.
TEST(dedup_stm_snapshot, restore_into_a_smaller_ceiling_keeps_the_newest) {
    dedup_window_filter source(1h);
    dedup_index_snapshot seed{.max_timestamp = model::timestamp{1'000'100}};
    for (size_t i = 0; i < 100; ++i) {
        seed.entries.push_back(
          {.identity = {.hi = i + 1, .lo = (i + 1) * 0x9e37'79b9'7f4a'7c15ULL},
           .timestamp = model::timestamp{1'000'000 + static_cast<int64_t>(i)}});
    }
    source.restore(seed);

    dedup_window_filter target(1h, 10);
    int64_t generation = 0;
    dedup_stm_test_accessor::restore(
      target, generation, dedup_stm_test_accessor::serialize(source, 3));
    EXPECT_EQ(target.map_size(), 10u);
    EXPECT_EQ(target.truncated_entries(), 90u);
    for (const auto& entry : target.snapshot().entries) {
        EXPECT_GE(entry.timestamp.value(), 1'000'090);
    }
}

// The serialized size is the format's whole point: a fixed header plus 20
// bytes an entry, against 30 for the version 1 envelope-per-entry layout.
TEST(dedup_stm_snapshot, packed_format_is_smaller_than_version_1) {
    constexpr size_t count = 10'000;
    dedup_window_filter source(1h);
    dedup_index_snapshot seed{.max_timestamp = model::timestamp{1'000'000}};
    for (size_t i = 0; i < count; ++i) {
        seed.entries.push_back(
          {.identity = {.hi = i + 1, .lo = (i + 1) * 0x9e37'79b9'7f4a'7c15ULL},
           .timestamp = model::timestamp{1'000'000}});
    }
    source.restore(seed);
    const auto packed
      = dedup_stm_test_accessor::serialize(source, 0).size_bytes();
    static_assert(dedup_stm::packed_entry_size == 20);
    EXPECT_LE(packed, count * dedup_stm::packed_entry_size + 64);
    EXPECT_LT(packed, v1_snapshot_with(count).size_bytes());
}

// --- The yielding walk the snapshot is written from ---
//
// for_each_live_entry_async() suspends between chunks of the index while the
// produce path keeps mutating it. These tests mutate from inside emit(), which
// is the same thing: the walk's view of the array changes under it.

namespace {

constexpr int64_t walk_base_ts = 1'000'000'000;

std::string bulk_key(size_t i) { return "bulk-" + std::to_string(i); }

void populate_bulk(dedup_window_filter& f, size_t count) {
    for (size_t i = 0; i < count; ++i) {
        f.populate(iobuf::from(bulk_key(i)), model::timestamp{walk_base_ts});
    }
}

// Admit `count` distinct keys through filter_request(), whose undo is the only
// way to erase chosen entries from the middle of the array.
dedup_filter_result admit_victims(dedup_window_filter& f, size_t count) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset{0});
    builder.set_timestamp(model::timestamp{walk_base_ts});
    for (size_t i = 0; i < count; ++i) {
        builder.add_raw_kv(
          iobuf::from("victim-" + std::to_string(i)), iobuf::from("v"));
    }
    return f.filter_request(std::move(builder).build());
}

std::vector<dedup_identity_digest>
sorted(std::vector<dedup_identity_digest> v) {
    std::ranges::sort(v, [](const auto& a, const auto& b) {
        return std::tie(a.hi, a.lo) < std::tie(b.hi, b.lo);
    });
    return v;
}

std::vector<dedup_identity_digest> bulk_digests(size_t count) {
    std::vector<dedup_identity_digest> out;
    for (size_t i = 0; i < count; ++i) {
        out.push_back(dedup_digest_of(iobuf::from(bulk_key(i))));
    }
    return sorted(std::move(out));
}

// Every digest in `expected` occurs in `seen` at least `at_least` times and at
// most once more; nothing else does.
void expect_covers(
  std::vector<dedup_identity_digest> seen,
  const std::vector<dedup_identity_digest>& expected) {
    seen = sorted(std::move(seen));
    std::vector<dedup_identity_digest> missing;
    std::ranges::set_difference(
      expected, seen, std::back_inserter(missing), [](auto& a, auto& b) {
          return std::tie(a.hi, a.lo) < std::tie(b.hi, b.lo);
      });
    EXPECT_TRUE(missing.empty()) << missing.size() << " entries missing";
}

size_t duplicates_in(std::vector<dedup_identity_digest> seen) {
    seen = sorted(std::move(seen));
    const auto [first, last] = std::ranges::unique(
      seen, [](const auto& a, const auto& b) { return a == b; });
    return static_cast<size_t>(std::distance(first, last));
}

} // namespace

// Erasing entries the walk has passed moves the array's last entries, not yet
// walked, into their slots. Those must still be emitted.
TEST_CORO(dedup_live_walk, erases_behind_the_cursor_lose_nothing) {
    dedup_window_filter f(1h);
    // The victims come first, so they sit at the front of the array.
    auto request = admit_victims(f, 50);
    ASSERT_EQ_CORO(request.undo.entries.size(), 50u);
    populate_bulk(f, 10'000);
    ASSERT_EQ_CORO(f.map_size(), 10'050u);

    std::vector<dedup_identity_digest> seen;
    bool reverted = false;
    const bool complete = co_await f.for_each_live_entry_async(
      [&](dedup_identity_digest identity, model::timestamp) {
          seen.push_back(identity);
          if (!reverted && seen.size() == 100) {
              // The cursor is past the victims; each erase pulls one of the
              // last 50 bulk entries behind it.
              reverted = true;
              f.revert_request(request.undo);
          }
      });
    ASSERT_TRUE_CORO(complete);
    ASSERT_TRUE_CORO(reverted);
    EXPECT_EQ(f.map_size(), 10'000u);
    expect_covers(seen, bulk_digests(10'000));
    // The victims were emitted before they were erased; nothing was emitted
    // twice.
    EXPECT_EQ(seen.size(), 10'050u);
    EXPECT_EQ(duplicates_in(seen), 0u);
}

// Erasing entries the walk has not reached needs no bookkeeping: whatever
// moves into their slots is still ahead of the cursor.
TEST_CORO(dedup_live_walk, erases_ahead_of_the_cursor_are_simply_gone) {
    dedup_window_filter f(1h);
    populate_bulk(f, 10'000);
    auto request = admit_victims(f, 50);
    ASSERT_EQ_CORO(f.map_size(), 10'050u);

    std::vector<dedup_identity_digest> seen;
    bool reverted = false;
    const bool complete = co_await f.for_each_live_entry_async(
      [&](dedup_identity_digest identity, model::timestamp) {
          seen.push_back(identity);
          if (!reverted) {
              reverted = true;
              f.revert_request(request.undo);
          }
      });
    ASSERT_TRUE_CORO(complete);
    EXPECT_EQ(f.map_size(), 10'000u);
    EXPECT_EQ(seen.size(), 10'000u);
    expect_covers(seen, bulk_digests(10'000));
}

// Inserts during the walk append to the array: they are emitted at most once,
// and everything already there is still emitted.
TEST_CORO(dedup_live_walk, inserts_during_the_walk_are_emitted_at_most_once) {
    dedup_window_filter f(1h);
    populate_bulk(f, 1'000);

    std::vector<dedup_identity_digest> seen;
    bool inserted = false;
    const bool complete = co_await f.for_each_live_entry_async(
      [&](dedup_identity_digest identity, model::timestamp) {
          seen.push_back(identity);
          if (!inserted && seen.size() == 10) {
              inserted = true;
              for (size_t i = 0; i < 100; ++i) {
                  f.populate(
                    iobuf::from("late-" + std::to_string(i)),
                    model::timestamp{walk_base_ts + 1});
              }
          }
      });
    ASSERT_TRUE_CORO(complete);
    EXPECT_EQ(f.map_size(), 1'100u);
    expect_covers(seen, bulk_digests(1'000));
    EXPECT_EQ(duplicates_in(seen), 0u);
    EXPECT_EQ(seen.size(), 1'100u);
}

// An index cleared or replaced under the walk invalidates it: the caller is
// told, and a fresh walk describes the new index.
TEST_CORO(dedup_live_walk, a_replaced_index_invalidates_the_walk) {
    dedup_window_filter f(1h);
    populate_bulk(f, 10'000);

    size_t seen = 0;
    bool complete = co_await f.for_each_live_entry_async(
      [&](dedup_identity_digest, model::timestamp) {
          if (++seen == 5) {
              f.clear();
          }
      });
    EXPECT_FALSE(complete);
    EXPECT_EQ(f.map_size(), 0u);

    populate_bulk(f, 10'000);
    dedup_index_snapshot replacement{
      .max_timestamp = model::timestamp{walk_base_ts}};
    for (size_t i = 0; i < 3; ++i) {
        replacement.entries.push_back(
          {.identity = {.hi = i + 1, .lo = (i + 1) * 0x9e37'79b9'7f4a'7c15ULL},
           .timestamp = model::timestamp{walk_base_ts}});
    }
    seen = 0;
    complete = co_await f.for_each_live_entry_async(
      [&](dedup_identity_digest, model::timestamp) {
          if (++seen == 5) {
              f.restore(replacement);
          }
      });
    EXPECT_FALSE(complete);

    seen = 0;
    complete = co_await f.for_each_live_entry_async(
      [&](dedup_identity_digest, model::timestamp) { ++seen; });
    EXPECT_TRUE(complete);
    EXPECT_EQ(seen, 3u);
}

// A snapshot can carry an identity twice when it was erased and admitted
// again during the walk; the restore keeps the newer timestamp, as populate()
// would.
TEST(dedup_stm_snapshot, a_repeated_identity_restores_to_the_newer_timestamp) {
    dedup_window_filter target(1h);
    int64_t generation = 0;
    const dedup_identity_digest id{.hi = 1, .lo = 2};
    auto entries = iobuf{};
    for (const int64_t ts :
         {walk_base_ts + 5, walk_base_ts + 9, walk_base_ts}) {
        const auto hi = ss::cpu_to_le(id.hi);
        const auto lo = ss::cpu_to_le(id.lo);
        const auto le_ts = ss::cpu_to_le(ts);
        entries.append(reinterpret_cast<const char*>(&hi), sizeof(hi));
        entries.append(reinterpret_cast<const char*>(&lo), sizeof(lo));
        entries.append(reinterpret_cast<const char*>(&le_ts), sizeof(le_ts));
    }
    dedup_stm_test_accessor::restore(
      target,
      generation,
      dedup_stm_test_accessor::state_snapshot_with_entries(
        std::move(entries), model::timestamp{walk_base_ts + 9}));
    ASSERT_EQ(target.map_size(), 1u);
    const auto restored = target.snapshot().entries;
    ASSERT_EQ(restored.size(), 1u);
    EXPECT_EQ(restored[0].timestamp, model::timestamp{walk_base_ts + 9});
}

struct dedup_stm_fixture : raft::stm_raft_fixture<dedup_stm> {
    stm_shptrs_t create_stms(
      raft::state_machine_manager_builder& builder,
      raft::raft_node_instance& node) final {
        // Called after node.initialise() has created the consensus instance
        // but before node.start() starts the state machines, which is the only
        // point where a recreated node's ntp_config can be seeded before the
        // dedup STM reads it at startup.
        if (_dedup_overrides) {
            node.raft()->log()->set_overrides(*_dedup_overrides);
        }
        return builder.create_stm<dedup_stm>(
          node.raft().get(),
          clusterlog,
          node.get_kvstore(),
          sync_timeout.bind(),
          dedup_window_filter::default_max_entries,
          _shard_budget);
    }

    /// Shared by every STM the fixture creates. All replicas run on one shard
    /// here, so they count against one budget -- which is what lets a test
    /// observe that a stopped STM gives its entries back.
    ss::lw_shared_ptr<dedup_shard_budget> _shard_budget;

    // The STM apply path reads the dedup window, generation, and key header
    // from the partition's ntp_config; in production these arrive via topic
    // config propagation. Mirror that here by setting the log overrides on
    // every node.
    void set_dedup_config(
      std::chrono::milliseconds window,
      int64_t generation = 0,
      std::optional<ss::sstring> key_header = std::nullopt) {
        storage::ntp_config::default_overrides overrides;
        overrides.dedup_window_ms = tristate<std::chrono::milliseconds>(window);
        overrides.dedup_generation = generation;
        overrides.dedup_key_header = std::move(key_header);
        _dedup_overrides = overrides;
        for (auto& [_, n] : nodes()) {
            // stop_and_recreate_nodes() replaces every node with a fresh
            // instance whose consensus pointer stays null until start_nodes()
            // initialises it. Dereferencing it here is what made
            // bounded_recovery_survives_mid_batch_timequery segfault; those
            // nodes pick the config up in create_stms() instead.
            if (n->raft()) {
                n->raft()->log()->set_overrides(overrides);
            }
        }
    }

    std::optional<storage::ntp_config::default_overrides> _dedup_overrides;

    ss::future<result<kafka_result>>
    produce_batch(model::node_id leader, model::record_batch batch) {
        auto stm = get_stm<0>(node(leader));
        auto stages = stm->replicate_in_stages(
          std::move(batch),
          raft::replicate_options(raft::consistency_level::quorum_ack));
        co_await std::move(stages.request_enqueued);
        co_return co_await std::move(stages.replicate_finished);
    }

    ss::future<kafka_result> produce(
      model::node_id leader,
      std::string_view key,
      std::string_view value,
      model::timestamp timestamp) {
        auto result = co_await produce_batch(
          leader, make_batch(key, value, timestamp));
        EXPECT_TRUE(result.has_value());
        co_return result.value();
    }

    ss::future<kafka_result> produce_with_header(
      model::node_id leader,
      std::string_view key,
      std::string_view value,
      model::timestamp timestamp,
      std::optional<std::pair<std::string_view, std::string_view>> header) {
        auto result = co_await produce_batch(
          leader, make_batch_with_header(key, value, timestamp, header));
        EXPECT_TRUE(result.has_value());
        co_return result.value();
    }

    ss::future<> wait_for_stms(model::offset offset) {
        for (auto& [_, n] : nodes()) {
            co_await get_stm<0>(*n)->wait(
              offset, model::timeout_clock::now() + 10s);
        }
    }

    config::mock_property<std::chrono::milliseconds> sync_timeout{10s};
};

TEST_F_CORO(dedup_stm_fixture, follower_state_derived_from_data_batches) {
    co_await initialize_state_machines();
    set_dedup_config(1000ms);
    auto leader = co_await wait_for_leader(10s);

    auto first = co_await produce(
      leader, "key", "value-1", model::timestamp{1000});
    ASSERT_EQ_CORO(first.replicated_record_count, -1);

    // Every replica rebuilds the index from the replicated data batch alone:
    // nothing dedup-specific is written to the log.
    auto committed = node(leader).raft()->committed_offset();
    co_await wait_for_stms(committed);
    for (auto& [_, n] : nodes()) {
        ASSERT_EQ_CORO(get_stm<0>(*n)->map_size(), 1);
    }
}

// Idempotent/transactional data batches never go through
// dedup_stm::replicate_in_stages on the leader (partition.cc routes them to
// rm_stm instead), but do_apply() sees every committed raft_data batch
// regardless of which STM replicated it. A batch carrying a real producer id
// must not be indexed: otherwise a record that's part of a since-aborted
// transaction (invisible to read_committed consumers) could still poison the
// index against a later, ordinary record with the same identity.
TEST_F_CORO(dedup_stm_fixture, producer_tracked_batch_bypasses_index) {
    co_await initialize_state_machines();
    set_dedup_config(1000ms);
    auto leader = co_await wait_for_leader(10s);

    model::batch_builder builder;
    builder.set_batch_type(model::record_batch_type::raft_data);
    builder.set_batch_timestamp(
      model::timestamp_type::create_time, model::timestamp{1000});
    builder.set_producer_id(42);
    builder.set_producer_epoch(0);
    builder.add_record(
      model::record({}, 0, 0, iobuf::from("key"), iobuf::from("value-1"), {}));
    auto batch = std::move(builder).build_sync();

    auto stages = node(leader).raft()->replicate_in_stages(
      std::move(batch),
      raft::replicate_options(raft::consistency_level::quorum_ack));
    co_await std::move(stages.request_enqueued);
    auto replicated = co_await std::move(stages.replicate_finished);
    ASSERT_TRUE_CORO(replicated.has_value());

    co_await wait_for_stms(node(leader).raft()->committed_offset());
    for (auto& [_, n] : nodes()) {
        ASSERT_EQ_CORO(get_stm<0>(*n)->map_size(), 0);
    }

    // An ordinary produce with the same key afterward must not be treated as
    // a duplicate of the producer-tracked batch above.
    auto ordinary = co_await produce(
      leader, "key", "value-2", model::timestamp{1500});
    ASSERT_EQ_CORO(ordinary.replicated_record_count, -1);
}

TEST_F_CORO(dedup_stm_fixture, state_survives_leadership_change) {
    co_await initialize_state_machines();
    set_dedup_config(1000ms);
    auto leader = co_await wait_for_leader(10s);

    auto first = co_await produce(
      leader, "key", "value-1", model::timestamp{1000});
    ASSERT_EQ_CORO(first.replicated_record_count, -1);
    co_await wait_for_stms(node(leader).raft()->committed_offset());

    // A fully-deduplicated request is a true no-op on the log.
    auto before_duplicate = node(leader).raft()->dirty_offset();
    auto duplicate = co_await produce(
      leader, "key", "value-2", model::timestamp{1500});
    ASSERT_EQ_CORO(duplicate.replicated_record_count, 0);
    ASSERT_EQ_CORO(node(leader).raft()->dirty_offset(), before_duplicate);

    auto old_term = node(leader).raft()->term();
    auto transfer = co_await node(leader).raft()->transfer_leadership(
      {.group = node(leader).raft()->group(), .timeout = 10s});
    ASSERT_TRUE_CORO(transfer.success);
    auto new_leader = co_await wait_for_leader_change(
      model::timeout_clock::now() + 10s, old_term);
    ASSERT_NE_CORO(new_leader, leader);

    // The new leader syncs its applied state and drops the same duplicate.
    duplicate = co_await produce(
      new_leader, "key", "value-3", model::timestamp{1600});
    ASSERT_EQ_CORO(duplicate.replicated_record_count, 0);
    ASSERT_EQ_CORO(get_stm<0>(node(new_leader))->map_size(), 1);
}

TEST_F_CORO(dedup_stm_fixture, state_survives_local_snapshot_restart) {
    co_await initialize_state_machines();
    set_dedup_config(1000ms);
    auto leader = co_await wait_for_leader(10s);

    co_await produce(leader, "key", "value-1", model::timestamp{1000});
    co_await wait_for_stms(node(leader).raft()->committed_offset());
    for (auto& [_, n] : nodes()) {
        co_await get_stm<0>(*n)->write_local_snapshot();
    }

    co_await restart_nodes();
    set_dedup_config(1000ms);
    leader = co_await wait_for_leader(10s);
    auto duplicate = co_await produce(
      leader, "key", "value-2", model::timestamp{1500});
    ASSERT_EQ_CORO(duplicate.replicated_record_count, 0);
    ASSERT_EQ_CORO(get_stm<0>(node(leader))->map_size(), 1);
}

// Local snapshots go to a file in the partition directory, and the copy an
// earlier build left in the kvstore -- which the kvstore would otherwise hold
// in memory indefinitely -- is removed when the STM starts.
TEST_F_CORO(
  dedup_stm_fixture, snapshot_is_a_file_and_legacy_kvstore_copy_is_dropped) {
    co_await initialize_state_machines();
    set_dedup_config(1000ms);
    auto leader = co_await wait_for_leader(10s);

    co_await produce(leader, "key", "value-1", model::timestamp{1000});
    co_await wait_for_stms(node(leader).raft()->committed_offset());
    // Each fixture node runs its own ntp, so each has its own legacy key.
    std::map<model::node_id, bytes> legacy_keys;
    for (auto& [id, n] : nodes()) {
        const auto& legacy_key = legacy_keys[id]
          = dedup_stm_test_accessor::legacy_kvstore_key(n->raft()->ntp());
        auto stm = get_stm<0>(*n);
        co_await stm->write_local_snapshot();
        // The kvstore backend reports 0 here; the file backend reports the
        // file's size.
        ASSERT_GT_CORO(stm->get_local_snapshot_size(), 0);
        co_await n->get_kvstore().put(
          storage::kvstore::key_space::stms, legacy_key, iobuf::from("stale"));
        ASSERT_TRUE_CORO(n->get_kvstore()
                           .get(storage::kvstore::key_space::stms, legacy_key)
                           .has_value());
    }

    co_await restart_nodes();
    set_dedup_config(1000ms);
    leader = co_await wait_for_leader(10s);
    for (auto& [id, n] : nodes()) {
        ASSERT_FALSE_CORO(
          n->get_kvstore()
            .get(storage::kvstore::key_space::stms, legacy_keys.at(id))
            .has_value());
    }
    // And the index came back from the file snapshot.
    auto duplicate = co_await produce(
      leader, "key", "value-2", model::timestamp{1500});
    ASSERT_EQ_CORO(duplicate.replicated_record_count, 0);
}

// get_initial_recovery_start_offset() resolves a bounded recovery offset via
// timequery(), which can land in the middle of a multi-record batch (see
// storage::batch_timequery, which walks into a batch looking for the first
// record at or after the target timestamp). The STM manager's apply loop
// requires next() to land exactly on a batch's base offset:
// batch_applicator::apply_to_stm() treats stm->next() > batch.base_offset()
// as "already applied" and skips the batch without ever advancing next()
// past it -- if get_initial_recovery_start_offset() ever returns an
// unaligned offset, the STM is stuck there permanently and produces to that
// partition never complete.
TEST_F_CORO(dedup_stm_fixture, bounded_recovery_survives_mid_batch_timequery) {
    co_await initialize_state_machines();
    set_dedup_config(1000ms);
    auto leader = co_await wait_for_leader(10s);

    // One batch, two records: the first (t0 = now-5000) is meant to fall
    // outside the eventual window, the second (t1 = now-1000) inside it, so
    // timequery() must resolve to the second record's offset -- the middle
    // of this batch, never its base.
    const auto t0 = model::timestamp(model::timestamp::now().value() - 5000);
    constexpr int64_t t1_delta = 4000;
    model::batch_builder builder;
    builder.set_batch_type(model::record_batch_type::raft_data);
    builder.set_batch_timestamp(model::timestamp_type::create_time, t0);
    builder.add_record(
      model::record({}, 0, 0, iobuf::from("a"), iobuf::from("v1"), {}));
    builder.add_record(
      model::record({}, t1_delta, 1, iobuf::from("b"), iobuf::from("v2"), {}));
    auto batch = std::move(builder).build_sync();

    auto stages = node(leader).raft()->replicate_in_stages(
      std::move(batch),
      raft::replicate_options(raft::consistency_level::quorum_ack));
    co_await std::move(stages.request_enqueued);
    auto replicated = co_await std::move(stages.replicate_finished);
    ASSERT_TRUE_CORO(replicated.has_value());
    co_await wait_for_stms(node(leader).raft()->committed_offset());

    // window=3000ms puts the cutoff (now - 3000, evaluated at restart) a
    // couple of seconds after t0 and a couple before t1, comfortably inside
    // the margin either side even with test overhead.
    co_await stop_and_recreate_nodes();
    set_dedup_config(3000ms);
    co_await start_nodes();

    leader = co_await wait_for_leader(10s);
    // If get_initial_recovery_start_offset() had returned the unaligned,
    // mid-batch offset, the STM would never make progress and this produce
    // would time out (sync() never returns) instead of completing.
    auto after_restart = co_await produce(
      leader, "c", "v3", model::timestamp(model::timestamp::now().value()));
    ASSERT_EQ_CORO(after_restart.replicated_record_count, -1);
}

TEST_F_CORO(dedup_stm_fixture, generation_change_invalidates_old_state) {
    co_await initialize_state_machines();
    set_dedup_config(1000ms, 0);
    auto leader = co_await wait_for_leader(10s);

    auto first = co_await produce(
      leader, "key", "value-1", model::timestamp{1000});
    ASSERT_EQ_CORO(first.replicated_record_count, -1);
    auto duplicate = co_await produce(
      leader, "key", "value-2", model::timestamp{1100});
    ASSERT_EQ_CORO(duplicate.replicated_record_count, 0);

    // A generation bump models disabling and re-enabling dedup: the old
    // index no longer applies and the same key is admitted again.
    set_dedup_config(1000ms, 1);
    auto after_reenable = co_await produce(
      leader, "key", "value-3", model::timestamp{1200});
    ASSERT_EQ_CORO(after_reenable.replicated_record_count, -1);
    co_await wait_for_stms(node(leader).raft()->committed_offset());

    auto old_term = node(leader).raft()->term();
    auto transfer = co_await node(leader).raft()->transfer_leadership(
      {.group = node(leader).raft()->group(), .timeout = 10s});
    ASSERT_TRUE_CORO(transfer.success);
    auto new_leader = co_await wait_for_leader_change(
      model::timeout_clock::now() + 10s, old_term);

    duplicate = co_await produce(
      new_leader, "key", "value-4", model::timestamp{1300});
    ASSERT_EQ_CORO(duplicate.replicated_record_count, 0);
}

TEST_F_CORO(dedup_stm_fixture, raft_snapshot_is_convergent) {
    co_await initialize_state_machines();
    set_dedup_config(1000ms);
    auto leader = co_await wait_for_leader(10s);
    auto stm = get_stm<0>(node(leader));

    co_await produce(leader, "a", "value-a", model::timestamp{1000});
    const auto first_committed = node(leader).raft()->committed_offset();
    co_await stm->wait(first_committed, model::timeout_clock::now() + 10s);

    co_await produce(leader, "b", "value-b", model::timestamp{1100});
    const auto latest_committed = node(leader).raft()->committed_offset();
    co_await stm->wait(latest_committed, model::timeout_clock::now() + 10s);

    // Snapshots are convergent, not byte-exact: any target offset serializes
    // the current state. Installing it at an older offset and replaying the
    // remaining log converges because apply is idempotent (max-wins).
    auto historical = co_await stm->take_raft_snapshot(first_committed);
    ASSERT_EQ_CORO(
      dedup_stm_test_accessor::snapshot_size(std::move(historical)), 2);
    auto latest = co_await stm->take_raft_snapshot(latest_committed);
    ASSERT_EQ_CORO(
      dedup_stm_test_accessor::snapshot_size(std::move(latest)), 2);

    // Snapshot construction must not mutate the live state.
    ASSERT_EQ_CORO(stm->map_size(), 2);
}

TEST_F_CORO(dedup_stm_fixture, topics_without_dedup_config_are_inert) {
    co_await initialize_state_machines();
    auto leader = co_await wait_for_leader(10s);
    auto stm = get_stm<0>(node(leader));

    ASSERT_EQ_CORO(
      stm->get_initial_recovery_policy(),
      raft::stm_initial_recovery_policy::skip_to_end);

    // Data replicated without a configured dedup window is not indexed.
    auto result = co_await node(leader).raft()->replicate(
      make_batch("key", "value", model::timestamp{1000}),
      raft::replicate_options(raft::consistency_level::quorum_ack));
    ASSERT_TRUE_CORO(result.has_value());
    co_await wait_for_stms(node(leader).raft()->committed_offset());
    for (auto& [_, n] : nodes()) {
        ASSERT_EQ_CORO(get_stm<0>(*n)->map_size(), 0);
    }
}

TEST_F_CORO(dedup_stm_fixture, concurrent_same_key_is_admitted_once) {
    co_await initialize_state_machines();
    set_dedup_config(1000ms);
    auto leader = co_await wait_for_leader(10s);
    const auto before = node(leader).raft()->dirty_offset();

    auto [first, second] = co_await ss::when_all_succeed(
      produce(leader, "key", "value-1", model::timestamp{1000}),
      produce(leader, "key", "value-2", model::timestamp{1000}));

    const auto admitted = (first.replicated_record_count == -1 ? 1 : 0)
                          + (second.replicated_record_count == -1 ? 1 : 0);
    const auto dropped = (first.replicated_record_count == 0 ? 1 : 0)
                         + (second.replicated_record_count == 0 ? 1 : 0);
    ASSERT_EQ_CORO(admitted, 1);
    ASSERT_EQ_CORO(dropped, 1);
    // Exactly one data batch reaches the log; the duplicate writes nothing.
    ASSERT_EQ_CORO(
      node(leader).raft()->dirty_offset(), before + model::offset{1});
    // The duplicate waited on the introducer; once both are acknowledged
    // nothing is in flight any more.
    auto stm = get_stm<0>(node(leader));
    ASSERT_EQ_CORO(dedup_stm_test_accessor::pending_size(*stm), 0);
    ASSERT_EQ_CORO(dedup_stm_test_accessor::resolving_size(*stm), 0);
}

// A committed batch whose records cannot be read is skipped, not retried:
// the STM manager would otherwise retry it forever, and with it hold up every
// raft snapshot of the partition and the log's prefix truncation.
TEST_F_CORO(dedup_stm_fixture, unreadable_batch_is_skipped_not_retried) {
    co_await initialize_state_machines();
    set_dedup_config(1000ms);
    auto leader = co_await wait_for_leader(10s);

    // Cut the record short and seal the header over the result, so storage
    // accepts the batch but its one record cannot be parsed.
    auto good = make_batch("key", "value", model::timestamp{1000});
    auto header = good.header();
    auto data = good.data().copy();
    data.trim_back(4);
    header.size_bytes = static_cast<int32_t>(
      model::packed_record_batch_header_size + data.size_bytes());
    header.crc = model::crc_record_batch(header, data);
    header.header_crc = model::internal_header_only_crc(header);
    model::record_batch broken(
      header, std::move(data), model::record_batch::tag_ctor_ng{});

    auto result = co_await node(leader).raft()->replicate(
      std::move(broken),
      raft::replicate_options(raft::consistency_level::quorum_ack));
    ASSERT_TRUE_CORO(result.has_value());
    // Applies past it on every replica, within the wait's timeout.
    co_await wait_for_stms(node(leader).raft()->committed_offset());
    for (auto& [_, n] : nodes()) {
        auto stm = get_stm<0>(*n);
        ASSERT_EQ_CORO(dedup_stm_test_accessor::batches_skipped(*stm), 1);
        ASSERT_EQ_CORO(stm->map_size(), 0);
    }

    // And indexing continues behind it.
    co_await produce(leader, "key", "value", model::timestamp{1000});
    co_await wait_for_stms(node(leader).raft()->committed_offset());
    for (auto& [_, n] : nodes()) {
        ASSERT_EQ_CORO(get_stm<0>(*n)->map_size(), 1);
    }
}

TEST_F_CORO(dedup_stm_fixture, concurrent_independent_keys_are_admitted) {
    co_await initialize_state_machines();
    set_dedup_config(1000ms);
    auto leader = co_await wait_for_leader(10s);
    const auto before = node(leader).raft()->dirty_offset();

    auto [first, second] = co_await ss::when_all_succeed(
      produce(leader, "key-a", "value-a", model::timestamp{1000}),
      produce(leader, "key-b", "value-b", model::timestamp{1000}));

    ASSERT_EQ_CORO(first.replicated_record_count, -1);
    ASSERT_EQ_CORO(second.replicated_record_count, -1);
    // One data batch per request; neither request is dropped as a duplicate
    // of the other and nothing serializes them.
    ASSERT_EQ_CORO(
      node(leader).raft()->dirty_offset(), before + model::offset{2});
}

// --- Header-based dedup identity (redpanda.dedup.key.header) ---

TEST_F_CORO(dedup_stm_fixture, header_mode_deduplicates_on_header_value) {
    co_await initialize_state_machines();
    set_dedup_config(1000ms, 0, ss::sstring{"redpanda-dedup-key"});
    auto leader = co_await wait_for_leader(10s);

    auto first = co_await produce_with_header(
      leader,
      "partition-key-a",
      "value-1",
      model::timestamp{1000},
      std::make_pair(
        std::string_view{"redpanda-dedup-key"}, std::string_view{"id-1"}));
    ASSERT_EQ_CORO(first.replicated_record_count, -1);

    // Same header value, different Kafka key, within the window: dropped.
    auto duplicate = co_await produce_with_header(
      leader,
      "partition-key-b",
      "value-2",
      model::timestamp{1500},
      std::make_pair(
        std::string_view{"redpanda-dedup-key"}, std::string_view{"id-1"}));
    ASSERT_EQ_CORO(duplicate.replicated_record_count, 0);

    // Every replica derives the header-based index from the data batch.
    co_await wait_for_stms(node(leader).raft()->committed_offset());
    for (auto& [_, n] : nodes()) {
        ASSERT_EQ_CORO(get_stm<0>(*n)->map_size(), 1);
    }
}

TEST_F_CORO(dedup_stm_fixture, header_mode_absent_header_rejects_produce) {
    co_await initialize_state_machines();
    set_dedup_config(1000ms, 0, ss::sstring{"redpanda-dedup-key"});
    auto leader = co_await wait_for_leader(10s);

    auto result = co_await produce_batch(
      leader,
      make_batch_with_header(
        "key", "value", model::timestamp{1000}, std::nullopt));
    ASSERT_FALSE_CORO(result.has_value());
    ASSERT_EQ_CORO(result.error(), errc::invalid_request);

    // Nothing was replicated or indexed.
    ASSERT_EQ_CORO(
      get_stm<0>(node(leader))->map_size(), static_cast<size_t>(0));
}

TEST_F_CORO(
  dedup_stm_fixture, header_mode_state_survives_local_snapshot_restart) {
    co_await initialize_state_machines();
    set_dedup_config(1000ms, 0, ss::sstring{"redpanda-dedup-key"});
    auto leader = co_await wait_for_leader(10s);

    co_await produce_with_header(
      leader,
      "key",
      "value-1",
      model::timestamp{1000},
      std::make_pair(
        std::string_view{"redpanda-dedup-key"}, std::string_view{"id-1"}));
    co_await wait_for_stms(node(leader).raft()->committed_offset());
    for (auto& [_, n] : nodes()) {
        co_await get_stm<0>(*n)->write_local_snapshot();
    }

    co_await restart_nodes();
    set_dedup_config(1000ms, 0, ss::sstring{"redpanda-dedup-key"});
    leader = co_await wait_for_leader(10s);

    auto duplicate = co_await produce_with_header(
      leader,
      "key",
      "value-2",
      model::timestamp{1500},
      std::make_pair(
        std::string_view{"redpanda-dedup-key"}, std::string_view{"id-1"}));
    ASSERT_EQ_CORO(duplicate.replicated_record_count, 0);
    ASSERT_EQ_CORO(get_stm<0>(node(leader))->map_size(), 1);
}

TEST_F_CORO(
  dedup_stm_fixture, switching_identity_source_invalidates_old_state) {
    co_await initialize_state_machines();
    set_dedup_config(1000ms, 0);
    auto leader = co_await wait_for_leader(10s);

    auto first = co_await produce(
      leader, "key", "value-1", model::timestamp{1000});
    ASSERT_EQ_CORO(first.replicated_record_count, -1);
    co_await wait_for_stms(node(leader).raft()->committed_offset());

    // Switching from key-based to header-based identity is modeled the same
    // way as disabling dedup: a generation bump (topic_table::apply()) makes
    // stale key-keyed state unreachable so it can never be compared against
    // header-keyed decisions.
    set_dedup_config(1000ms, 1, ss::sstring{"redpanda-dedup-key"});
    auto after_switch = co_await produce_with_header(
      leader,
      "key",
      "value-2",
      model::timestamp{1100},
      std::make_pair(
        std::string_view{"redpanda-dedup-key"}, std::string_view{"id-1"}));
    ASSERT_EQ_CORO(after_switch.replicated_record_count, -1);
    ASSERT_EQ_CORO(get_stm<0>(node(leader))->map_size(), 1);
}

// The index switched from storing identity bytes to storing 128-bit digests,
// which is an on-disk format change: a snapshot written by an older build
// carries identities this build cannot re-derive digests from, and serde
// rejects it on the compat version. Because the index is advisory and
// log-derived, the right response is to drop it and rebuild from the log --
// never to fail to start the partition. Simulated by rewriting the serde
// envelope's version bytes on a snapshot this build just produced, which is
// exactly what such a buffer looks like at the point serde inspects it.
TEST_F_CORO(dedup_stm_fixture, unreadable_snapshot_starts_from_an_empty_index) {
    co_await initialize_state_machines();
    set_dedup_config(1000ms);
    auto leader = co_await wait_for_leader(10s);
    auto stm = get_stm<0>(node(leader));

    co_await produce(leader, "a", "value-a", model::timestamp{1000});
    co_await stm->wait(
      node(leader).raft()->committed_offset(),
      model::timeout_clock::now() + 10s);
    ASSERT_EQ_CORO(stm->map_size(), 1);

    auto snapshot = co_await stm->take_raft_snapshot(
      node(leader).raft()->committed_offset());
    ASSERT_EQ_CORO(dedup_stm_test_accessor::snapshot_size(snapshot.copy()), 1);

    // serde envelope header is {version:u8, compat_version:u8, size:u32};
    // stamping version 0 makes it look like a pre-digest snapshot.
    auto raw = iobuf_to_bytes(snapshot);
    raw[0] = 0;
    raw[1] = 0;
    auto downgraded = bytes_to_iobuf(raw);

    // Must not throw, and must leave the index empty rather than partially
    // populated with entries decoded before the failure. Reporting `no` is
    // what makes persisted_stm replay the log the snapshot covered instead
    // of skipping past it, which is the difference between an index that
    // rebuilds and one that stays empty.
    auto applied = co_await dedup_stm_test_accessor::apply_local_snapshot(
      *stm, std::move(downgraded));
    ASSERT_EQ_CORO(applied, raft::local_snapshot_applied::no);
    ASSERT_EQ_CORO(stm->map_size(), 0);

    // The STM is still usable afterwards: a fresh produce indexes normally.
    co_await produce(leader, "b", "value-b", model::timestamp{2000});
    co_await stm->wait(
      node(leader).raft()->committed_offset(),
      model::timeout_clock::now() + 10s);
    ASSERT_GT_CORO(stm->map_size(), 0);
}

// A snapshot written by this build round-trips through the digest wire
// format: entry count, and the dedup decisions the restored index makes.
TEST_F_CORO(dedup_stm_fixture, digest_snapshot_round_trips) {
    co_await initialize_state_machines();
    set_dedup_config(1000ms);
    auto leader = co_await wait_for_leader(10s);
    auto stm = get_stm<0>(node(leader));

    co_await produce(leader, "round-trip", "v1", model::timestamp{1000});
    co_await stm->wait(
      node(leader).raft()->committed_offset(),
      model::timeout_clock::now() + 10s);

    auto snapshot = co_await stm->take_raft_snapshot(
      node(leader).raft()->committed_offset());
    ASSERT_EQ_CORO(dedup_stm_test_accessor::snapshot_size(snapshot.copy()), 1);

    auto applied = co_await dedup_stm_test_accessor::apply_local_snapshot(
      *stm, std::move(snapshot));
    ASSERT_EQ_CORO(applied, raft::local_snapshot_applied::yes);
    ASSERT_EQ_CORO(stm->map_size(), 1);

    // The restored digest still matches the identity it came from, so a
    // duplicate within the window is still caught.
    auto duplicate = co_await produce_batch(
      leader, make_batch("round-trip", "v2", model::timestamp{1500}));
    ASSERT_TRUE_CORO(duplicate.has_value());
    ASSERT_EQ_CORO(duplicate.value().replicated_record_count, 0);
}

// A raft snapshot this build cannot read gets the same treatment as an
// unreadable local one: discard it and keep going. apply_raft_snapshot()
// returns void, so the observable contract is that it does not throw and
// leaves the index empty rather than partially decoded.
TEST_F_CORO(dedup_stm_fixture, unreadable_raft_snapshot_is_discarded) {
    co_await initialize_state_machines();
    set_dedup_config(1000ms);
    auto leader = co_await wait_for_leader(10s);
    auto stm = get_stm<0>(node(leader));

    co_await produce(leader, "a", "value-a", model::timestamp{1000});
    co_await stm->wait(
      node(leader).raft()->committed_offset(),
      model::timeout_clock::now() + 10s);
    ASSERT_EQ_CORO(stm->map_size(), 1);

    auto snapshot = co_await stm->take_raft_snapshot(
      node(leader).raft()->committed_offset());
    auto raw = iobuf_to_bytes(snapshot);
    raw[0] = 0;
    raw[1] = 0;

    co_await dedup_stm_test_accessor::apply_raft_snapshot(
      *stm, bytes_to_iobuf(raw));
    ASSERT_EQ_CORO(stm->map_size(), 0);

    // Still usable afterwards.
    co_await produce(leader, "b", "value-b", model::timestamp{2000});
    co_await stm->wait(
      node(leader).raft()->committed_offset(),
      model::timeout_clock::now() + 10s);
    ASSERT_GT_CORO(stm->map_size(), 0);
}

// An empty raft snapshot buffer is the "no state yet" case, not a corrupt
// one: it must clear the index without being reported as unreadable.
TEST_F_CORO(dedup_stm_fixture, empty_raft_snapshot_clears_the_index) {
    co_await initialize_state_machines();
    set_dedup_config(1000ms);
    auto leader = co_await wait_for_leader(10s);
    auto stm = get_stm<0>(node(leader));

    co_await produce(leader, "a", "value-a", model::timestamp{1000});
    co_await stm->wait(
      node(leader).raft()->committed_offset(),
      model::timeout_clock::now() + 10s);
    ASSERT_EQ_CORO(stm->map_size(), 1);

    co_await dedup_stm_test_accessor::apply_raft_snapshot(*stm, iobuf{});
    ASSERT_EQ_CORO(stm->map_size(), 0);
}

// Every replica derives the index from the log, so the digest each one
// computes for a given identity has to agree -- otherwise a follower
// promoted to leader would miss duplicates the old leader was catching.
// Checked by producing a duplicate after the index has propagated: every
// replica indexed the same identity, and the leader drops the repeat.
TEST_F_CORO(dedup_stm_fixture, digests_agree_across_replicas) {
    co_await initialize_state_machines();
    set_dedup_config(1000ms);
    auto leader = co_await wait_for_leader(10s);

    co_await produce(leader, "shared-key", "v1", model::timestamp{1000});
    co_await wait_for_stms(node(leader).raft()->committed_offset());
    for (auto& [_, n] : nodes()) {
        ASSERT_EQ_CORO(get_stm<0>(*n)->map_size(), 1);
    }

    auto duplicate = co_await produce_batch(
      leader, make_batch("shared-key", "v2", model::timestamp{1500}));
    ASSERT_TRUE_CORO(duplicate.has_value());
    ASSERT_EQ_CORO(duplicate.value().replicated_record_count, 0);

    // Nothing was appended, so every replica still holds exactly the one
    // entry -- no replica invented a second digest for the same identity.
    co_await wait_for_stms(node(leader).raft()->committed_offset());
    for (auto& [_, n] : nodes()) {
        ASSERT_EQ_CORO(get_stm<0>(*n)->map_size(), 1);
    }
}

// Large identities are the case digesting exists for: the index entry is the
// same size whatever the key is, and it must still identify the key exactly.
TEST_F_CORO(dedup_stm_fixture, large_identities_deduplicate_end_to_end) {
    co_await initialize_state_machines();
    set_dedup_config(1000ms);
    auto leader = co_await wait_for_leader(10s);

    const ss::sstring big_key(100'000, 'k');
    co_await produce(leader, big_key, "v1", model::timestamp{1000});
    co_await wait_for_stms(node(leader).raft()->committed_offset());

    auto duplicate = co_await produce_batch(
      leader, make_batch(big_key, "v2", model::timestamp{1500}));
    ASSERT_TRUE_CORO(duplicate.has_value());
    ASSERT_EQ_CORO(duplicate.value().replicated_record_count, 0);

    // A different large key of the same length is not confused with it.
    const ss::sstring other_key(100'000, 'j');
    auto admitted = co_await produce_batch(
      leader, make_batch(other_key, "v3", model::timestamp{1500}));
    ASSERT_TRUE_CORO(admitted.has_value());
    ASSERT_EQ_CORO(admitted.value().replicated_record_count, -1);
}

// A partially filtered request is where the leader's speculative
// classification and the replicas' apply-path indexing have to agree on
// exactly which records reached the log.
TEST_F_CORO(dedup_stm_fixture, partial_filter_converges_across_replicas) {
    co_await initialize_state_machines();
    set_dedup_config(1000ms);
    auto leader = co_await wait_for_leader(10s);

    co_await produce(leader, "seeded", "v1", model::timestamp{1000});
    co_await wait_for_stms(node(leader).raft()->committed_offset());

    // One duplicate, two new identities.
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset{0});
    builder.set_timestamp(model::timestamp{1200});
    builder.add_raw_kv(iobuf::from("fresh-a"), iobuf::from("a"));
    builder.add_raw_kv(iobuf::from("seeded"), iobuf::from("dropped"));
    builder.add_raw_kv(iobuf::from("fresh-b"), iobuf::from("b"));

    auto result = co_await produce_batch(leader, std::move(builder).build());
    ASSERT_TRUE_CORO(result.has_value());
    ASSERT_EQ_CORO(result.value().replicated_record_count, 2);

    co_await wait_for_stms(node(leader).raft()->committed_offset());
    for (auto& [_, n] : nodes()) {
        ASSERT_EQ_CORO(get_stm<0>(*n)->map_size(), 3);
    }
}

} // namespace
// A version 1 snapshot -- what a not-yet-upgraded peer sends as a raft
// snapshot during a rolling upgrade -- is discarded and the index rebuilt,
// never misread and never a failed partition.
TEST_F_CORO(dedup_stm_fixture, version_1_snapshot_is_discarded) {
    co_await initialize_state_machines();
    set_dedup_config(1000ms);
    auto leader = co_await wait_for_leader(10s);
    auto stm = get_stm<0>(node(leader));

    co_await produce(leader, "a", "value-a", model::timestamp{1000});
    co_await stm->wait(
      node(leader).raft()->committed_offset(),
      model::timeout_clock::now() + 10s);
    ASSERT_EQ_CORO(stm->map_size(), 1);

    auto applied = co_await dedup_stm_test_accessor::apply_local_snapshot(
      *stm, v1_snapshot_with(5));
    ASSERT_EQ_CORO(applied, raft::local_snapshot_applied::no);
    ASSERT_EQ_CORO(stm->map_size(), 0);

    co_await dedup_stm_test_accessor::apply_raft_snapshot(
      *stm, v1_snapshot_with(5));
    ASSERT_EQ_CORO(stm->map_size(), 0);
}

// Every replica's index is counted in the shard budget, and an STM that stops
// gives its entries back: after a restart the budget equals exactly what the
// new STMs restored, not that plus what the old ones held.
TEST_F_CORO(
  dedup_stm_fixture, shard_budget_counts_replicas_and_survives_restart) {
    _shard_budget = ss::make_lw_shared<dedup_shard_budget>(1'000);
    co_await initialize_state_machines();
    set_dedup_config(1000ms);
    auto leader = co_await wait_for_leader(10s);

    co_await produce(leader, "a", "value-a", model::timestamp{1000});
    co_await produce(leader, "b", "value-b", model::timestamp{1100});
    co_await wait_for_stms(node(leader).raft()->committed_offset());

    auto total_entries = [this] {
        size_t total = 0;
        for (auto& [_, n] : nodes()) {
            total += get_stm<0>(*n)->map_size();
        }
        return total;
    };
    ASSERT_EQ_CORO(total_entries(), 2 * nodes().size());
    ASSERT_EQ_CORO(_shard_budget->entries, total_entries());

    for (auto& [_, n] : nodes()) {
        co_await get_stm<0>(*n)->write_local_snapshot();
    }
    co_await restart_nodes();
    set_dedup_config(1000ms);
    leader = co_await wait_for_leader(10s);
    co_await wait_for_stms(node(leader).raft()->committed_offset());

    ASSERT_EQ_CORO(total_entries(), 2 * nodes().size());
    ASSERT_EQ_CORO(_shard_budget->entries, total_entries());

    // The restored index still catches duplicates.
    auto duplicate = co_await produce(
      leader, "a", "value-a2", model::timestamp{1500});
    ASSERT_EQ_CORO(duplicate.replicated_record_count, 0);
}

// With the shard budget exhausted, produce keeps working and never grows the
// indices past the limit. Each new identity replaces the oldest one its replica
// holds -- the newest identities are the ones a retry is most likely to repeat
// -- rather than being admitted unindexed.
TEST_F_CORO(dedup_stm_fixture, exhausted_shard_budget_keeps_the_newest) {
    // Three replicas share the budget here, so a limit of three lets each of
    // them hold one identity.
    _shard_budget = ss::make_lw_shared<dedup_shard_budget>(3);
    co_await initialize_state_machines();
    set_dedup_config(1000s);
    auto leader = co_await wait_for_leader(10s);

    co_await produce(leader, "first", "v", model::timestamp{1000});
    co_await wait_for_stms(node(leader).raft()->committed_offset());
    ASSERT_EQ_CORO(_shard_budget->entries, 3u);

    for (int i = 0; i < 5; ++i) {
        auto admitted = co_await produce(
          leader, fmt::format("later-{}", i), "v", model::timestamp{2000 + i});
        ASSERT_EQ_CORO(admitted.replicated_record_count, -1);
    }
    co_await wait_for_stms(node(leader).raft()->committed_offset());
    ASSERT_LE_CORO(_shard_budget->entries, 3u);

    // The newest identity is still deduplicated.
    auto duplicate = co_await produce(
      leader, "later-4", "v2", model::timestamp{2004});
    ASSERT_EQ_CORO(duplicate.replicated_record_count, 0);
}

} // namespace cluster
