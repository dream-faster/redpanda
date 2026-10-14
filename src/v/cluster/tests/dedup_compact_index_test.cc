// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/dedup_compact_index.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <random>
#include <unordered_map>
#include <utility>
#include <vector>

namespace cluster {
namespace {

using index_t = dedup_compact_index;
constexpr int64_t expired = index_t::expired;
constexpr int64_t day_ms = 86'400'000;

uint64_t splitmix(uint64_t& x) {
    uint64_t z = (x += 0x9e37'79b9'7f4a'7c15ULL);
    z = (z ^ (z >> 30)) * 0xbf58'476d'1ce4'e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d0'49bb'1331'11ebULL;
    return z ^ (z >> 31);
}

dedup_identity_digest random_digest(uint64_t& seed) {
    return {.hi = splitmix(seed), .lo = splitmix(seed)};
}

struct digest_hash {
    size_t operator()(const dedup_identity_digest& d) const {
        return d.lo ^ (d.hi * 0x9e37'79b9'7f4a'7c15ULL);
    }
};

/// The layout the index promises, kept naively: a dense array in insertion
/// order, erase moving the last entry into the hole, plus a map to find
/// positions.
struct reference {
    std::vector<std::pair<dedup_identity_digest, int64_t>> entries;
    std::unordered_map<dedup_identity_digest, size_t, digest_hash> positions;
    int64_t newest_written = expired;

    bool contains(const dedup_identity_digest& d) const {
        return positions.contains(dedup_canonical_digest(d));
    }
    void push(dedup_identity_digest d, int64_t ts) {
        d = dedup_canonical_digest(d);
        positions[d] = entries.size();
        entries.emplace_back(d, ts);
        note(ts);
    }
    void erase(size_t pos) {
        positions.erase(entries[pos].first);
        if (pos != entries.size() - 1) {
            entries[pos] = entries.back();
            positions[entries[pos].first] = pos;
        }
        entries.pop_back();
    }
    void set(size_t pos, int64_t ts) {
        entries[pos].second = ts;
        note(ts);
    }
    void note(int64_t ts) {
        if (ts != expired) {
            newest_written = std::max(newest_written, ts);
        }
    }
};

enum class stamps {
    /// Within 30 days of a clock that may advance: exact unless a write runs
    /// more than retained_span ahead of an entry.
    near,
    /// Up to 200 days back, so blocks often cannot keep everything.
    wide,
    /// int64 extremes and anything in between.
    extreme,
};

int64_t next_stamp(std::mt19937_64& rng, stamps kind, int64_t now) {
    switch (kind) {
    case stamps::near:
        return now - static_cast<int64_t>(rng() % (30 * day_ms));
    case stamps::wide:
        return now - static_cast<int64_t>(rng() % (200 * day_ms))
               + static_cast<int64_t>(rng() % 1000);
    case stamps::extreme:
        switch (rng() % 6) {
        case 0:
            return std::numeric_limits<int64_t>::min();
        case 1:
            return std::numeric_limits<int64_t>::max();
        case 2:
            return std::numeric_limits<int64_t>::min()
                   + static_cast<int64_t>(rng() % 100'000);
        case 3:
            return std::numeric_limits<int64_t>::max()
                   - static_cast<int64_t>(rng() % 100'000);
        case 4:
            return static_cast<int64_t>(rng());
        default:
            return now - static_cast<int64_t>(rng() % 100'000);
        }
    }
    return now;
}

/// Random pushes, erases, timestamp writes and lookups against the reference.
/// Every timestamp the index returns is the one written, or `expired` for an
/// entry more than retained_span behind something written to the index.
void run_against_reference(
  uint64_t seed,
  size_t ops,
  size_t target_size,
  stamps kind,
  bool clock_advances,
  size_t check_every) {
    std::mt19937_64 rng(seed);
    index_t idx;
    reference ref;
    uint64_t keys = seed * 7919 + 1;
    int64_t now = 1'700'000'000'000;
    for (size_t op = 0; op < ops; ++op) {
        if (clock_advances) {
            now += static_cast<int64_t>(rng() % 3'000'000);
        }
        const auto r = rng() % 100;
        const bool grow = ref.entries.size() < target_size;
        if (r < (grow ? 60u : 30u)) {
            const auto d = random_digest(keys);
            if (ref.contains(d)) {
                continue;
            }
            const auto ts = next_stamp(rng, kind, now);
            ASSERT_EQ(idx.find(d), index_t::npos);
            ASSERT_TRUE(idx.push_back(d, ts));
            ref.push(d, ts);
        } else if (r < 75 && !ref.entries.empty()) {
            const size_t pos = rng() % ref.entries.size();
            idx.erase_at(pos);
            ref.erase(pos);
        } else if (r < 90 && !ref.entries.empty()) {
            const size_t pos = rng() % ref.entries.size();
            const auto ts = next_stamp(rng, kind, now);
            idx.set_timestamp_at(pos, ts);
            ref.set(pos, ts);
        } else if (!ref.entries.empty()) {
            const size_t pos = rng() % ref.entries.size();
            const auto& d = ref.entries[pos].first;
            ASSERT_EQ(idx.find(d), pos);
            // Only 96 bits are the identity.
            auto upper = d;
            upper.hi |= (rng() | 1) << 32;
            ASSERT_EQ(idx.find(upper), pos);
            auto other = d;
            other.hi ^= 1;
            if (!ref.contains(other)) {
                ASSERT_EQ(idx.find(other), index_t::npos);
            }
        }
        ASSERT_EQ(idx.size(), ref.entries.size());
        if (op % check_every == 0 || op + 1 == ops) {
            ASSERT_NO_THROW(idx.check_invariants());
            for (size_t pos = 0; pos < ref.entries.size(); ++pos) {
                const auto& [d, want] = ref.entries[pos];
                ASSERT_EQ(idx.identity_at(pos), d) << pos;
                const auto got = idx.timestamp_at(pos);
                if (got == want) {
                    continue;
                }
                ASSERT_EQ(got, expired) << "pos " << pos << " want " << want;
                if (kind != stamps::extreme) {
                    ASSERT_LT(want, ref.newest_written - index_t::retained_span)
                      << "pos " << pos;
                }
            }
        }
    }
}

TEST(DedupCompactIndex, MatchesTheReferenceWithNearTimestamps) {
    for (uint64_t seed = 1; seed <= 4; ++seed) {
        run_against_reference(seed, 100'000, 3'000, stamps::near, false, 997);
    }
    run_against_reference(5, 400'000, 30'000, stamps::near, true, 99'991);
}

TEST(DedupCompactIndex, MatchesTheReferenceWithWideTimestamps) {
    for (uint64_t seed = 11; seed <= 14; ++seed) {
        run_against_reference(seed, 100'000, 3'000, stamps::wide, true, 997);
    }
    run_against_reference(15, 400'000, 30'000, stamps::wide, true, 99'991);
}

TEST(DedupCompactIndex, MatchesTheReferenceWithExtremeTimestamps) {
    for (uint64_t seed = 21; seed <= 24; ++seed) {
        run_against_reference(seed, 60'000, 3'000, stamps::extreme, true, 997);
    }
}

TEST(DedupCompactIndex, NearTimestampsAreNeverLost) {
    // Entries within retained_span of everything written keep their exact
    // timestamps through every kind of mutation, including moves between
    // blocks with different bases.
    index_t idx;
    uint64_t keys = 9;
    std::mt19937_64 rng(9);
    const int64_t now = 1'700'000'000'000;
    std::vector<int64_t> want;
    for (size_t i = 0; i < 5 * index_t::block_entries; ++i) {
        const auto ts = now - static_cast<int64_t>(rng() % (36 * day_ms));
        ASSERT_TRUE(idx.push_back(random_digest(keys), ts));
        want.push_back(ts);
    }
    for (size_t i = 0; i < 50'000; ++i) {
        const size_t pos = rng() % want.size();
        if (i % 3 == 0) {
            idx.erase_at(pos);
            want[pos] = want.back();
            want.pop_back();
            const auto ts = now - static_cast<int64_t>(rng() % (36 * day_ms));
            ASSERT_TRUE(idx.push_back(random_digest(keys), ts));
            want.push_back(ts);
        } else {
            const auto ts = now - static_cast<int64_t>(rng() % (36 * day_ms));
            idx.set_timestamp_at(pos, ts);
            want[pos] = ts;
        }
    }
    for (size_t pos = 0; pos < want.size(); ++pos) {
        ASSERT_EQ(idx.timestamp_at(pos), want[pos]) << pos;
    }
    idx.check_invariants();
}

TEST(DedupCompactIndex, TimestampBoundaries) {
    index_t idx;
    EXPECT_EQ(idx.find({.hi = 1, .lo = 2}), index_t::npos);
    ASSERT_TRUE(idx.push_back({.hi = 1, .lo = 2}, expired));
    EXPECT_EQ(idx.timestamp_at(0), expired);
    for (int64_t ts : {int64_t{5}, int64_t{-1}, int64_t{0}}) {
        idx.set_timestamp_at(0, ts);
        EXPECT_EQ(idx.timestamp_at(0), ts);
    }
    // One block span apart cannot both be kept: the older goes.
    ASSERT_TRUE(idx.push_back({.hi = 3, .lo = 4}, index_t::retained_span));
    EXPECT_EQ(idx.timestamp_at(0), 0);
    EXPECT_EQ(idx.timestamp_at(1), index_t::retained_span);
    ASSERT_TRUE(idx.push_back({.hi = 5, .lo = 6}, index_t::block_span + 1));
    EXPECT_EQ(idx.timestamp_at(0), expired);
    EXPECT_EQ(idx.timestamp_at(1), index_t::retained_span);
    EXPECT_EQ(idx.timestamp_at(2), index_t::block_span + 1);
    idx.check_invariants();

    index_t ends;
    constexpr auto lowest = std::numeric_limits<int64_t>::min() + 1;
    constexpr auto highest = std::numeric_limits<int64_t>::max();
    ASSERT_TRUE(ends.push_back({.hi = 1, .lo = 1}, lowest));
    EXPECT_EQ(ends.timestamp_at(0), lowest);
    ASSERT_TRUE(ends.push_back({.hi = 1, .lo = 2}, highest));
    EXPECT_EQ(ends.timestamp_at(1), highest);
    EXPECT_EQ(ends.timestamp_at(0), expired);
    ends.check_invariants();
}

TEST(DedupCompactIndex, AStaleNewestHealsWithinABlockOfWrites) {
    // The block's newest bound outlives the entry that set it, so a write far
    // behind it is stored expired without a pass over the block -- but only
    // until a block's worth of writes, after which it is re-based and exact.
    index_t idx;
    ASSERT_TRUE(idx.push_back({.hi = 1, .lo = 2}, 1'000'000));
    idx.set_timestamp_at(0, 1'000'000 + index_t::block_span);
    idx.set_timestamp_at(0, 0);
    EXPECT_EQ(idx.timestamp_at(0), expired);
    size_t writes = 1;
    while (idx.timestamp_at(0) != 0) {
        idx.set_timestamp_at(0, 0);
        ASSERT_LE(++writes, index_t::block_entries);
    }
    EXPECT_EQ(idx.timestamp_at(0), 0);
}

TEST(DedupCompactIndex, RebasesAreRareAsTimeMovesForward) {
    index_t idx;
    uint64_t keys = 77;
    for (size_t i = 0; i < 4000; ++i) {
        ASSERT_TRUE(idx.push_back(random_digest(keys), 0));
    }
    // Two years in one-hour steps, writing a spread of entries each step.
    for (int64_t t = 0; t < 730 * day_ms; t += 3'600'000) {
        for (size_t pos = 0; pos < idx.size(); pos += 97) {
            idx.set_timestamp_at(pos, t);
            ASSERT_EQ(idx.timestamp_at(pos), t);
        }
    }
    // A re-base leaves at least 3/4 of the headroom ahead, ~9 days.
    EXPECT_LT(idx.stats().rebases, 730 / 9 + 2);
}

TEST(DedupCompactIndex, RebasesAreRareAsTimeMovesBackward) {
    index_t idx;
    uint64_t keys = 78;
    const int64_t start = 365 * day_ms;
    for (size_t i = 0; i < 4000; ++i) {
        ASSERT_TRUE(idx.push_back(
          random_digest(keys), start - static_cast<int64_t>(i) * 600'000));
    }
    // 27.8 days of descent; a re-base leaves at least ~3 days below.
    EXPECT_LE(idx.stats().rebases, 10);
    for (size_t pos = 0; pos < idx.size(); ++pos) {
        ASSERT_EQ(
          idx.timestamp_at(pos), start - static_cast<int64_t>(pos) * 600'000);
    }
}

TEST(DedupCompactIndex, TooOldWritesDoNotRebaseEveryTime) {
    // Alternating new and 60-day-old timestamps: the old ones cannot be kept
    // beside the new ones, and finding that out does not cost a pass over the
    // block per write.
    index_t idx;
    uint64_t keys = 79;
    const int64_t now = 400 * day_ms;
    for (size_t i = 0; i < 4000; ++i) {
        const int64_t ts = i % 2 == 0 ? now + static_cast<int64_t>(i)
                                      : now - 60 * day_ms;
        ASSERT_TRUE(idx.push_back(random_digest(keys), ts));
    }
    EXPECT_LE(idx.stats().rebases, 2);
    for (size_t pos = 0; pos < idx.size(); ++pos) {
        if (pos % 2 == 0) {
            ASSERT_EQ(idx.timestamp_at(pos), now + static_cast<int64_t>(pos));
        } else {
            ASSERT_EQ(idx.timestamp_at(pos), expired);
        }
    }
}

TEST(DedupCompactIndex, CollidingHashesAreRefusedNotLooped) {
    // Same 64-bit lo: one home slot in one segment. Robin hood displacement
    // runs out at 255, and neither growing nor splitting separates them.
    index_t idx;
    size_t refused = 0;
    for (uint64_t i = 0; i < 2000; ++i) {
        const auto before = idx.work();
        if (!idx.push_back({.hi = i, .lo = 0x1234'5678'9abc'def0ULL}, 1000)) {
            ++refused;
        }
        // Bounded by one directory at its deepest plus a segment's worth.
        ASSERT_LT(idx.work() - before, size_t{1} << 18);
    }
    idx.check_invariants();
    size_t found = 0;
    for (uint64_t i = 0; i < 2000; ++i) {
        if (
          idx.find({.hi = i, .lo = 0x1234'5678'9abc'def0ULL})
          != index_t::npos) {
            ++found;
        }
    }
    EXPECT_EQ(found, idx.size());
    EXPECT_EQ(found + refused, 2000);
    EXPECT_GT(refused, 0);
    EXPECT_EQ(idx.stats().refused_inserts, refused);
    while (!idx.empty()) {
        idx.erase_at(idx.size() / 2);
    }
    idx.check_invariants();
}

TEST(DedupCompactIndex, APrefixBeyondTheDirectorysReachIsRefusedCheaply) {
    // The top 24 bits, which are all the directory can split on, the same for
    // every key: one segment fills and the rest are refused. A refusal reads
    // the segment's keys back once, not once per refused key.
    index_t idx;
    uint64_t keys = 5;
    size_t refused = 0;
    size_t total_work = 0;
    for (size_t i = 0; i < 100'000; ++i) {
        const uint64_t lo = splitmix(keys) & ((uint64_t{1} << 40) - 1);
        const auto before = idx.work();
        if (!idx.push_back({.hi = splitmix(keys), .lo = lo}, 1000)) {
            ++refused;
        }
        const auto work = idx.work() - before;
        ASSERT_LT(work, 4 * index_t::max_segment_slots);
        total_work += work;
    }
    idx.check_invariants();
    EXPECT_EQ(idx.size() + refused, 100'000);
    EXPECT_EQ(idx.size(), index_t::max_segment_slots / 8 * 7);
    EXPECT_EQ(idx.stats().segments, 1);
    // A few units per refused key once the first refusal has been made.
    EXPECT_LT(total_work, 20 * 100'000);
    // Erasing one makes room again, and the next key goes in.
    idx.erase_at(0);
    EXPECT_TRUE(idx.push_back({.hi = 1, .lo = 1}, 1000));
    idx.check_invariants();
}

TEST(DedupCompactIndex, AnySharedPrefixSplitsStraightPastIt) {
    // The top 8 bits the same for every key: a split goes straight to the
    // first bit that tells them apart instead of eight splits that each move
    // every key and leave an empty half behind.
    index_t idx;
    uint64_t keys = 6;
    constexpr size_t n = 200'000;
    for (size_t i = 0; i < n; ++i) {
        const uint64_t lo = splitmix(keys) & ((uint64_t{1} << 56) - 1);
        const auto before = idx.work();
        ASSERT_TRUE(idx.push_back({.hi = splitmix(keys), .lo = lo}, 1000));
        // One split's worth, and the directory deepened by up to 8 levels at
        // once (2^14 entries at most here).
        ASSERT_LT(
          idx.work() - before, 6 * index_t::max_segment_slots + (1u << 14));
    }
    idx.check_invariants();
    const double per_entry = static_cast<double>(idx.memory_usage()) / n;
    EXPECT_LT(per_entry, 30.0);
}

TEST(DedupCompactIndex, NoOperationDoesWorkProportionalToTheIndex) {
    // Most slots, entries and directory entries any one call touches, at
    // index sizes two orders of magnitude apart. A segment split is the
    // largest unit: two fresh segments and one walk of the old one.
    struct worst {
        size_t push{0}, erase{0}, set{0}, find{0};
    };
    auto measure = [](size_t n) {
        index_t idx;
        worst w;
        uint64_t keys = n;
        std::vector<dedup_identity_digest> sample;
        auto track = [&idx](size_t& into, auto&& op) {
            const auto before = idx.work();
            op();
            into = std::max(into, idx.work() - before);
        };
        int64_t now = 1'700'000'000'000;
        for (size_t i = 0; i < n; ++i) {
            const auto d = random_digest(keys);
            if (i % 16 == 0) {
                sample.push_back(d);
            }
            track(w.push, [&] {
                ASSERT_TRUE(idx.push_back(d, now + static_cast<int64_t>(i)));
            });
        }
        std::mt19937_64 rng(3);
        for (size_t i = 0; i < 200'000; ++i) {
            now += 7;
            track(
              w.find, [&] { (void)idx.find(sample[rng() % sample.size()]); });
            size_t pos = rng() % idx.size();
            track(w.erase, [&] { idx.erase_at(pos); });
            const auto d = random_digest(keys);
            track(w.push, [&] { ASSERT_TRUE(idx.push_back(d, now)); });
            pos = rng() % idx.size();
            // Every so often far ahead or behind, forcing a re-base.
            const int64_t ts = i % 997 == 0   ? now + 40 * day_ms
                               : i % 991 == 0 ? now - 20 * day_ms
                                              : now;
            track(w.set, [&] { idx.set_timestamp_at(pos, ts); });
        }
        return w;
    };
    const auto small = measure(20'000);
    const auto large = measure(2'000'000);
    // A segment split: reading its slots, two new segments at most as large,
    // re-inserting its keys (a few probes and shifts each), and doubling the
    // directory, which at these sizes has at most 2^11 entries.
    constexpr size_t split_bound = 6 * index_t::max_segment_slots
                                   + (size_t{1} << 12);
    EXPECT_LT(small.push, split_bound);
    EXPECT_LT(large.push, split_bound);
    EXPECT_GT(large.push, index_t::max_segment_slots) << "no split measured";
    // A block re-base.
    EXPECT_LE(small.set, 2 * index_t::block_entries + 2);
    EXPECT_LE(large.set, 2 * index_t::block_entries + 2);
    // Probe runs and backward shifts, plus a block re-base.
    EXPECT_LT(small.erase, 2 * index_t::block_entries + 1024);
    EXPECT_LT(large.erase, 2 * index_t::block_entries + 1024);
    EXPECT_LE(small.find, index_t::max_probe_distance + 1);
    EXPECT_LE(large.find, index_t::max_probe_distance + 1);
}

TEST(DedupCompactIndex, MemoryPerEntry) {
    // 16 bytes of entry and a 4-byte slot at a load between 7/16 and 7/8.
    for (size_t n : {10'000ul, 100'000ul, 1'000'000ul}) {
        index_t idx;
        uint64_t keys = n;
        for (size_t i = 0; i < n; ++i) {
            ASSERT_TRUE(idx.push_back(random_digest(keys), 1));
        }
        const double per_entry = static_cast<double>(idx.memory_usage())
                                 / static_cast<double>(n);
        EXPECT_LT(per_entry, 27.0) << n;
        EXPECT_GT(per_entry, 20.0) << n;
    }
    // A small index stays small.
    index_t one;
    ASSERT_TRUE(one.push_back({.hi = 1, .lo = 2}, 1));
    EXPECT_LT(one.memory_usage(), 512);
}

TEST(DedupCompactIndex, ReserveThenFill) {
    for (size_t n : {0ul, 1ul, 10ul, 100ul, 5000ul, 100'000ul}) {
        index_t idx;
        idx.reserve(n);
        uint64_t keys = n + 1;
        for (size_t i = 0; i < n + 10; ++i) {
            ASSERT_TRUE(
              idx.push_back(random_digest(keys), static_cast<int64_t>(i)));
        }
        idx.check_invariants();
        for (size_t i = 0; i < idx.size(); ++i) {
            ASSERT_EQ(idx.timestamp_at(i), static_cast<int64_t>(i));
        }
    }
}

TEST(DedupCompactIndex, EmptiesAndRefillsAcrossBlocks) {
    index_t idx;
    uint64_t keys = 3;
    const size_t n = 3 * index_t::block_entries + 5;
    for (size_t i = 0; i < n; ++i) {
        // The first block twenty days behind the rest.
        const int64_t ts = i < index_t::block_entries
                             ? 1000
                             : 1000 + 20 * day_ms + static_cast<int64_t>(i);
        ASSERT_TRUE(idx.push_back(random_digest(keys), ts));
    }
    for (size_t i = 0; i < 100; ++i) {
        const auto moved_identity = idx.identity_at(idx.size() - 1);
        const auto moved = idx.timestamp_at(idx.size() - 1);
        idx.erase_at(i);
        ASSERT_EQ(idx.identity_at(i), moved_identity);
        ASSERT_EQ(idx.timestamp_at(i), moved);
        ASSERT_EQ(idx.find(moved_identity), i);
    }
    idx.check_invariants();
    while (!idx.empty()) {
        idx.erase_at(idx.size() - 1);
    }
    idx.check_invariants();
    for (size_t i = 0; i < 2 * index_t::block_entries; ++i) {
        ASSERT_TRUE(
          idx.push_back(random_digest(keys), static_cast<int64_t>(i)));
    }
    idx.check_invariants();
    // Moved-from and moved-to leave nothing behind.
    index_t moved = std::move(idx);
    EXPECT_EQ(moved.size(), 2 * index_t::block_entries);
    moved.check_invariants();
    moved = index_t{};
    EXPECT_TRUE(moved.empty());
    EXPECT_EQ(moved.find({.hi = 1, .lo = 1}), index_t::npos);
    ASSERT_TRUE(moved.push_back({.hi = 1, .lo = 1}, 5));
    EXPECT_EQ(moved.find({.hi = 1, .lo = 1}), 0);
}

TEST(DedupCompactIndex, CanonicalDigestKeeps96Bits) {
    const dedup_identity_digest d{
      .hi = 0xaaaa'bbbb'cccc'ddddULL, .lo = 0x1111'2222'3333'4444ULL};
    const auto c = dedup_canonical_digest(d);
    EXPECT_EQ(c.hi, 0xcccc'ddddULL);
    EXPECT_EQ(c.lo, d.lo);
    EXPECT_EQ(dedup_canonical_digest(c), c);
    index_t idx;
    ASSERT_TRUE(idx.push_back(d, 1));
    EXPECT_EQ(idx.identity_at(0), c);
    EXPECT_EQ(idx.find(c), 0);
    EXPECT_EQ(idx.find(d), 0);
}

/// Drive the selector the way the restore does: every value, every pass.
int64_t select_kth(const std::vector<int64_t>& values, size_t k, int* passes) {
    const auto [lo, hi] = std::minmax_element(values.begin(), values.end());
    dedup_timestamp_selector select(*lo, *hi, k);
    *passes = 0;
    while (!select.done()) {
        for (auto v : values) {
            select.add(v);
        }
        select.finish_pass();
        ++*passes;
    }
    return select.value();
}

int64_t reference_kth(std::vector<int64_t> values, size_t k) {
    std::nth_element(
      values.begin(),
      values.begin() + static_cast<std::ptrdiff_t>(k),
      values.end());
    return values[k];
}

TEST(DedupTimestampSelector, MatchesNthElement) {
    std::mt19937_64 rng(17);
    auto check = [&](const std::vector<int64_t>& values) {
        for (size_t k :
             {size_t{0},
              values.size() / 4,
              values.size() / 2,
              values.size() - 1,
              static_cast<size_t>(rng() % values.size())}) {
            int passes = 0;
            ASSERT_EQ(select_kth(values, k, &passes), reference_kth(values, k))
              << k;
            ASSERT_LE(passes, 6);
        }
    };
    for (int round = 0; round < 50; ++round) {
        const size_t n = 1 + rng() % 5000;
        std::vector<int64_t> narrow, wide, ties, extreme;
        for (size_t i = 0; i < n; ++i) {
            narrow.push_back(
              1'700'000'000'000 + static_cast<int64_t>(rng() % 3'600'000));
            wide.push_back(static_cast<int64_t>(rng()));
            ties.push_back(static_cast<int64_t>(rng() % 3) * 1000);
            switch (rng() % 3) {
            case 0:
                extreme.push_back(std::numeric_limits<int64_t>::min());
                break;
            case 1:
                extreme.push_back(std::numeric_limits<int64_t>::max());
                break;
            default:
                extreme.push_back(static_cast<int64_t>(rng()));
            }
        }
        check(narrow);
        check(wide);
        check(ties);
        check(extreme);
    }
    // A single distinct value needs no pass at all.
    int passes = -1;
    EXPECT_EQ(select_kth({5, 5, 5}, 1, &passes), 5);
    EXPECT_EQ(passes, 0);
}

TEST(DedupTimestampSelector, NarrowRangesTakeFewPasses) {
    // An hour of millisecond timestamps is 22 bits: two passes.
    std::vector<int64_t> values;
    std::mt19937_64 rng(3);
    for (size_t i = 0; i < 100'000; ++i) {
        values.push_back(
          1'700'000'000'000 + static_cast<int64_t>(rng() % 3'600'000));
    }
    int passes = 0;
    EXPECT_EQ(
      select_kth(values, 70'000, &passes), reference_kth(values, 70'000));
    EXPECT_LE(passes, 2);
}

} // namespace
} // namespace cluster
