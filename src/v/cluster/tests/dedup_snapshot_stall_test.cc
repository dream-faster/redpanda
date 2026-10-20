// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf.h"
#include "cluster/dedup_stm.h"
#include "cluster/dedup_window_filter.h"
#include "model/timestamp.h"
#include "serde/envelope.h"
#include "serde/rw/envelope.h"
#include "serde/rw/iobuf.h"
#include "serde/rw/vector.h"
#include "test_utils/test.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/thread_cputime_clock.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/later.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>

using namespace std::chrono_literals;

namespace cluster {

// Reaches dedup_stm::serialize_snapshot(), which is private. Each test binary
// links its own definition of this friend, so the one in dedup_stm_test.cc is
// a different struct in a different binary rather than a redefinition of this
// one.
struct dedup_stm_test_accessor {
    // Coroutines, not plain forwarders: serialize_snapshot() reads the
    // generation by reference after its last yield, so the referent has to
    // outlive the call, and a forwarder's parameter does not.
    static ss::future<iobuf>
    serialize(dedup_window_filter& state, int64_t generation) {
        co_return co_await dedup_stm::serialize_snapshot(state, generation);
    }
    static ss::future<>
    restore(dedup_window_filter& state, int64_t& generation, iobuf buffer) {
        co_await dedup_stm::restore_snapshot(
          state, generation, dedup_stm::decode_snapshot(std::move(buffer)));
    }
};

namespace {

/// The version 1 wire layout, reproduced here because the STM no longer
/// carries it: one serde envelope per entry.
struct legacy_wire_entry
  : serde::
      envelope<legacy_wire_entry, serde::version<1>, serde::compat_version<1>> {
    uint64_t identity_hi{0};
    uint64_t identity_lo{0};
    model::timestamp timestamp;

    auto serde_fields() {
        return std::tie(identity_hi, identity_lo, timestamp);
    }
};

struct legacy_state_snapshot
  : serde::envelope<
      legacy_state_snapshot,
      serde::version<1>,
      serde::compat_version<1>> {
    int64_t window_ms{0};
    int64_t generation{0};
    model::timestamp max_timestamp{model::timestamp::min()};
    uint64_t legacy_inserts_since_evict{0};
    chunked_vector<legacy_wire_entry> entries;

    auto serde_fields() {
        return std::tie(
          window_ms,
          generation,
          max_timestamp,
          legacy_inserts_since_evict,
          entries);
    }
};

/// The snapshot path before the packed format, without its yields: build the
/// intermediate dedup_index_snapshot, copy that into wire entries, then
/// serialize. Three walks of the index and three copies of it.
iobuf legacy_snapshot(const dedup_window_filter& state, int64_t generation) {
    auto snapshot = state.snapshot();
    legacy_state_snapshot result{
      .window_ms = state.window().count(),
      .generation = generation,
      .max_timestamp = snapshot.max_timestamp};
    result.entries.reserve(snapshot.entries.size());
    for (const auto& entry : snapshot.entries) {
        result.entries.push_back(
          {.identity_hi = entry.identity.hi,
           .identity_lo = entry.identity.lo,
           .timestamp = entry.timestamp});
    }
    return serde::to_iobuf(std::move(result));
}

} // namespace

namespace {

/// The reactor thread's CPU time, user and system, in place of the wall
/// clock. A stall is the reactor running without reaching a preemption point,
/// which this counts in full; what it leaves out is time the host gave the
/// CPU to someone else. On a busy shared CI runner that is tens of
/// milliseconds at a time, and it made these limits fail on code that
/// passes them on a quiet one.
using clock = ss::thread_cputime_clock;

int64_t us(clock::duration d) {
    return std::chrono::duration_cast<std::chrono::microseconds>(d).count();
}

/// \brief Longest stretch the reactor spent inside a single task.
///
/// A fiber that does nothing but yield can only be scheduled when whatever
/// else is runnable gives the CPU up, so the reactor CPU time between its own
/// turns is how long the work under test ran without reaching a preemption
/// point.
/// That gap is the quantity a reactor stall report prints, measured here
/// directly rather than inferred from the stall detector.
class stall_probe {
public:
    void start() {
        _running = true;
        _loop = run();
    }

    ss::future<clock::duration> stop() {
        _running = false;
        co_await std::move(_loop);
        co_return _worst;
    }

private:
    ss::future<> run() {
        auto last = clock::now();
        while (_running) {
            co_await ss::yield();
            const auto now = clock::now();
            _worst = std::max(_worst, now - last);
            last = now;
        }
    }

    bool _running{false};
    ss::future<> _loop{ss::now()};
    clock::duration _worst{};
};

struct measurement {
    clock::duration stall{};
    clock::duration total{};
};

// Far enough in the past that restore()'s clamp to broker_now() leaves the
// watermark alone, and spread narrowly enough that an hour-long window keeps
// every entry -- so neither path filters anything and both serialize the same
// entries.
constexpr int64_t base_timestamp = 1'000'000'000;

void fill(dedup_window_filter& filter, size_t entries) {
    dedup_index_snapshot seed;
    seed.entries.reserve(entries);
    for (size_t i = 0; i < entries; ++i) {
        // Both halves are a multiplication by an odd constant, which is a
        // bijection mod 2^64 -- so every identity is distinct and the index
        // really does hold `entries` of them.
        //
        // `lo` matters more than it looks: the index uses it verbatim as the
        // hash, picking a segment by its high bits and a slot by its low
        // ones. Anything whose high bits barely move across the range -- a
        // counter, or ~counter -- puts every key in one segment, which then
        // cannot split and refuses most of them. A real XXH3-128 digest is
        // spread; the multiplier here is what keeps the synthetic one spread
        // too.
        seed.entries.push_back(
          {.identity
           = {.hi = 0xbf58'476d'1ce4'e5b9ULL * (i + 1), .lo = 0x9e37'79b9'7f4a'7c15ULL * (i + 1)},
           .timestamp = model::timestamp{
             base_timestamp + static_cast<int64_t>(i % 1000)}});
    }
    seed.max_timestamp = model::timestamp{base_timestamp + 999};
    filter.restore(seed);
}

ss::future<> compare_at(size_t entries) {
    std::cout << "\nfilling " << entries << " entries" << std::endl;
    dedup_window_filter filter(1h, entries);
    fill(filter, entries);
    ASSERT_EQ_CORO(filter.map_size(), entries);

    measurement legacy_m;
    size_t legacy_bytes = 0;
    {
        stall_probe probe;
        probe.start();
        const auto start = clock::now();
        legacy_bytes = legacy_snapshot(filter, 7).size_bytes();
        legacy_m.total = clock::now() - start;
        legacy_m.stall = co_await probe.stop();
    }

    // The indivisible part of the snapshot before this format: a copy of
    // every live entry into a vector, which the serializer then walked with
    // yields.
    measurement copy_m;
    {
        stall_probe probe;
        probe.start();
        const auto start = clock::now();
        const auto copy = filter.snapshot();
        copy_m.total = clock::now() - start;
        copy_m.stall = co_await probe.stop();
        ASSERT_EQ_CORO(copy.entries.size(), entries);
    }

    measurement packed_m;
    iobuf packed;
    {
        stall_probe probe;
        probe.start();
        const auto start = clock::now();
        packed = co_await dedup_stm_test_accessor::serialize(filter, 7);
        packed_m.total = clock::now() - start;
        packed_m.stall = co_await probe.stop();
    }

    std::cout << "\ndedup snapshot of " << entries << " entries\n"
              << "  version 1  " << legacy_bytes << " bytes   worst stall "
              << us(legacy_m.stall) << "us   total " << us(legacy_m.total)
              << "us\n"
              << "  version 2  " << packed.size_bytes()
              << " bytes   worst stall " << us(packed_m.stall) << "us   total "
              << us(packed_m.total) << "us\n"
              << "  plain copy of the entries   worst stall "
              << us(copy_m.stall) << "us   total " << us(copy_m.total) << "us\n"
              << std::flush;

    // The serialized copy is the snapshot's whole memory cost: 16 bytes per
    // entry plus a fixed header, against the 30 of a per-entry envelope.
    ASSERT_LE_CORO(
      packed.size_bytes(), entries * dedup_stm::packed_entry_size + 64);
    ASSERT_LT_CORO(packed.size_bytes(), legacy_bytes);

    // The packed write yields between chunks of the index, so its longest
    // stretch without a preemption point is a task quota's worth of work
    // plus whatever the runner's scheduler adds, however large the index --
    // where the passes it replaced ran for hundreds of milliseconds at this
    // size. The bound is loose enough that a loaded runner cannot turn a
    // real improvement into a red build.
    ASSERT_LT_CORO(us(packed_m.stall), us(legacy_m.stall));
    ASSERT_LT_CORO(us(packed_m.stall), 20'000);
}

} // namespace

// The shard budget turns memory into an entry limit at
// DEDUP_SHARD_BYTES_PER_ENTRY, which has to cover what the index itself takes
// per entry (entries and lookup table, at the table's worst load) with room
// for the snapshot's packed copy. Measured rather than derived, since the
// table's load moves with its size: right after its segments split it is at
// its emptiest, just before at its fullest. The sizes below land on both.
TEST_CORO(dedup_snapshot_stall, index_memory_per_entry_fits_the_budget) {
    size_t worst = 0;
    for (size_t entries : {460'000ul, 920'000ul, 1'000'000ul, 1'800'000ul}) {
        const auto before = ss::memory::stats().allocated_memory();
        auto filter = std::make_unique<dedup_window_filter>(1h, entries);
        fill(*filter, entries);
        const auto after = ss::memory::stats().allocated_memory();
        const auto per_entry = (after - before) / entries;
        const auto stats = filter->index_stats();
        std::cout << "\nindex memory: " << per_entry << " bytes per entry at "
                  << entries << " entries (table's own count "
                  << filter->memory_usage() / entries << "), lookup table "
                  << static_cast<double>(entries)
                       / static_cast<double>(stats.slots)
                  << " full (budget assumes " << DEDUP_SHARD_BYTES_PER_ENTRY
                  << ")\n"
                  << std::flush;
        ASSERT_EQ_CORO(filter->map_size(), entries);
        ASSERT_EQ_CORO(stats.refused_inserts, 0u);
        // Not compared with the table's own count: the allocator also counts
        // the spans its pools take on first use, and reuses what an earlier
        // round freed, so the two differ by a few bytes an entry either way.
        worst = std::max(worst, per_entry);
    }
    // The point of the table: well under the ~50 bytes the map it replaced
    // took, measured the same way.
    ASSERT_LE_CORO(worst, 32u);
    // The packed snapshot adds dedup_stm::packed_entry_size on top while it
    // is written; the budget's figure must cover both.
    ASSERT_LE_CORO(
      worst + dedup_stm::packed_entry_size, DEDUP_SHARD_BYTES_PER_ENTRY);
}

// Growing an index never stops the reactor for long: the lookup table grows a
// segment at a time instead of rehashing everything once it doubles, which at
// millions of entries took tens of milliseconds in one go. The produce and
// apply paths call into the filter once per record without yielding in
// between, so this drives populate() the same way, yielding only every
// `per_task` records, and measures the longest stretch: a task's worth of
// records plus the largest single step, however large the index.
TEST_CORO(dedup_snapshot_stall, growth_and_churn_stay_off_the_reactor) {
    constexpr size_t entries = 6'000'000;
    constexpr size_t per_task = 1024;
    dedup_window_filter filter(1h, entries);
    auto digest = [](size_t i) {
        return dedup_identity_digest{
          .hi = 0xbf58'476d'1ce4'e5b9ULL * (i + 1),
          .lo = 0x9e37'79b9'7f4a'7c15ULL * (i + 1)};
    };
    // A millisecond per thousand identities, so the capacity cutoff has a
    // spread of ages to trim at the ceiling.
    auto drive = [&](size_t from, size_t to) -> ss::future<> {
        for (size_t i = from; i < to; ++i) {
            filter.populate(
              digest(i),
              model::timestamp{
                base_timestamp + static_cast<int64_t>(i / 1000)});
            if ((i + 1) % per_task == 0) {
                co_await ss::coroutine::maybe_yield();
            }
        }
    };
    {
        stall_probe probe;
        probe.start();
        const auto start = clock::now();
        co_await drive(0, entries);
        const auto total = clock::now() - start;
        const auto stall = co_await probe.stop();
        const auto stats = filter.index_stats();
        std::cout << "\ngrowth to " << entries << " entries: worst stall "
                  << us(stall) << "us   total " << us(total) << "us   "
                  << stats.segment_splits << " segment splits\n"
                  << std::flush;
        // From 90% of the ceiling the sweep trims as it goes.
        ASSERT_LE_CORO(filter.map_size(), entries);
        ASSERT_GT_CORO(filter.map_size(), entries / 10 * 8);
        ASSERT_LT_CORO(us(stall), 20'000);
    }
    {
        // At the ceiling: every new identity sweeps, evicts and inserts.
        stall_probe probe;
        probe.start();
        const auto start = clock::now();
        co_await drive(entries, entries + 2'000'000);
        const auto total = clock::now() - start;
        const auto stall = co_await probe.stop();
        std::cout << "churn of 2000000 identities at the ceiling: worst stall "
                  << us(stall) << "us   total " << us(total) << "us   "
                  << filter.capacity_evicted_entries() << " evicted\n"
                  << std::flush;
        ASSERT_LE_CORO(filter.map_size(), entries);
        ASSERT_LT_CORO(us(stall), 20'000);
    }
    {
        // Giving it all back (dedup disabled, a generation change, a restore)
        // frees blocks and segments, a few thousand allocations, not entries.
        const auto start = clock::now();
        filter.clear();
        const auto took = clock::now() - start;
        std::cout << "clear: " << us(took) << "us\n" << std::flush;
        ASSERT_EQ_CORO(filter.map_size(), 0u);
        ASSERT_LT_CORO(filter.memory_usage(), 1024u);
        ASSERT_LT_CORO(us(took), 20'000);
    }
}

// The restore is the other half: at startup every dedup partition on the
// shard restores in turn, and one that held the reactor for its whole
// snapshot would hold the others behind the restore semaphore with it. Its
// passes over the packed entries yield, so the longest stretch is bounded
// however large the index -- including a restore that has to trim, which
// selects the newest entries in passes too, where an nth_element over every
// timestamp held the reactor for tens of milliseconds at this size.
TEST_CORO(dedup_snapshot_stall, restore_yields_however_large_the_index) {
    constexpr size_t entries = 6'000'000;
    auto source = std::make_unique<dedup_window_filter>(1h, entries);
    fill(*source, entries);
    auto packed = co_await dedup_stm_test_accessor::serialize(*source, 7);
    source.reset();

    {
        dedup_window_filter target(1h, entries);
        int64_t generation = 0;
        // Copied before the probe starts: a 144 MB memcpy is a stall of its
        // own, and not the restore's.
        auto buffer = packed.copy();
        stall_probe probe;
        probe.start();
        const auto start = clock::now();
        co_await dedup_stm_test_accessor::restore(
          target, generation, std::move(buffer));
        const auto total = clock::now() - start;
        const auto stall = co_await probe.stop();
        std::cout << "\nrestore of " << entries << " entries: worst stall "
                  << us(stall) << "us   total " << us(total) << "us\n"
                  << std::flush;
        ASSERT_EQ_CORO(target.map_size(), entries);
        ASSERT_EQ_CORO(generation, 7);
        ASSERT_LT_CORO(us(stall), 20'000);
    }
    {
        // Into half the room: the trimming restore.
        dedup_window_filter target(1h, entries / 2);
        int64_t generation = 0;
        stall_probe probe;
        probe.start();
        const auto start = clock::now();
        co_await dedup_stm_test_accessor::restore(
          target, generation, std::move(packed));
        const auto total = clock::now() - start;
        const auto stall = co_await probe.stop();
        std::cout << "trimming restore of " << entries << " entries into "
                  << entries / 2 << ": worst stall " << us(stall)
                  << "us   total " << us(total) << "us\n"
                  << std::flush;
        ASSERT_EQ_CORO(target.map_size(), entries / 2);
        ASSERT_LT_CORO(us(stall), 20'000);
    }
}

// An index this size is what put 123-124ms reactor stalls in the produce and
// raft_recv scheduling groups: request_make_snapshot_in_background() spawns
// the write from whichever append crossed the segment or dirty-byte
// threshold, so it runs in that fiber's scheduling group.
TEST_CORO(dedup_snapshot_stall, packed_write_is_smaller_and_stalls_less) {
    co_await compare_at(1'000'000);
    co_await compare_at(6'000'000);
}

} // namespace cluster
