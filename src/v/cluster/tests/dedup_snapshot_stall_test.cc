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
#include "serde/async.h"
#include "test_utils/test.h"

#include <seastar/core/future.hh>
#include <seastar/util/later.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>

using namespace std::chrono_literals;

namespace cluster {

// Reaches dedup_stm::state_snapshot and to_snapshot(), both private. Each
// test binary links its own definition of this friend, so the one in
// dedup_stm_test.cc is a different struct in a different binary rather than a
// redefinition of this one.
struct dedup_stm_test_accessor {
    /// The snapshot path as it stood before it learned to yield: build the
    /// intermediate dedup_index_snapshot, copy that into the wire entries,
    /// then serialize -- three walks of the index with no preemption point
    /// anywhere between them.
    ///
    /// The original did a fourth ahead of these: evict_expired()'s erase_if
    /// over the whole index. That method is gone, so it cannot be reproduced
    /// here, which makes every "before" number below an understatement of
    /// what the change removed.
    static iobuf
    legacy_snapshot(const dedup_window_filter& state, int64_t generation) {
        auto snapshot = state.snapshot();
        dedup_stm::state_snapshot result{
          .window_ms = state.window().count(),
          .generation = generation,
          .max_timestamp = snapshot.max_timestamp,
          .inserts_since_evict = static_cast<uint64_t>(
            snapshot.inserts_since_evict)};
        result.entries.reserve(snapshot.entries.size());
        for (const auto& entry : snapshot.entries) {
            result.entries.push_back(
              {.identity_hi = entry.identity.hi,
               .identity_lo = entry.identity.lo,
               .timestamp = entry.timestamp});
        }
        return serde::to_iobuf(std::move(result));
    }

    /// The first half of the path in the tree, and the half that still cannot
    /// yield: it is what hands the serializer a view of the index the produce
    /// path cannot move underneath it.
    static dedup_stm::state_snapshot
    wire_entries(const dedup_window_filter& state, int64_t generation) {
        return dedup_stm::to_snapshot(state, generation);
    }

    /// The second half: a serialization that yields every 1024 entries.
    static ss::future<iobuf> serialize(dedup_stm::state_snapshot snapshot) {
        iobuf out;
        co_await serde::write_async(out, std::move(snapshot));
        co_return std::move(out);
    }
};

namespace {

using clock = std::chrono::steady_clock;

int64_t us(clock::duration d) {
    return std::chrono::duration_cast<std::chrono::microseconds>(d).count();
}

/// \brief Longest stretch the reactor spent inside a single task.
///
/// A fiber that does nothing but yield can only be scheduled when whatever
/// else is runnable gives the CPU up, so the gap between its own turns is
/// how long the work under test ran without reaching a preemption point.
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
// every entry -- so neither path filters anything and their output has to
// match byte for byte.
constexpr int64_t base_timestamp = 1'000'000'000;

void fill(dedup_window_filter& filter, size_t entries) {
    dedup_index_snapshot seed;
    seed.entries.reserve(entries);
    for (size_t i = 0; i < entries; ++i) {
        // An odd multiplier is a bijection mod 2^64, so every identity is
        // distinct and the index really does hold `entries` of them.
        seed.entries.push_back(
          {.identity
           = {.hi = 0x9e37'79b9'7f4a'7c15ULL * (i + 1), .lo = ~uint64_t{i}},
           .timestamp = model::timestamp{
             base_timestamp + static_cast<int64_t>(i % 1000)}});
    }
    seed.max_timestamp = model::timestamp{base_timestamp + 999};
    filter.restore(seed);
}

ss::future<> compare_at(size_t entries) {
    dedup_window_filter filter(1h, entries);
    fill(filter, entries);
    ASSERT_EQ_CORO(filter.map_size(), entries);

    measurement legacy_m;
    iobuf legacy;
    {
        stall_probe probe;
        probe.start();
        const auto start = clock::now();
        legacy = dedup_stm_test_accessor::legacy_snapshot(filter, 7);
        legacy_m.total = clock::now() - start;
        legacy_m.stall = co_await probe.stop();
    }

    // Measured in its two halves, because they are no longer the same kind
    // of work: the copy is one indivisible pass, the serialization yields.
    measurement copy_m;
    measurement serialize_m;
    iobuf current;
    {
        stall_probe probe;
        probe.start();
        const auto start = clock::now();
        auto wire = dedup_stm_test_accessor::wire_entries(filter, 7);
        copy_m.total = clock::now() - start;
        copy_m.stall = co_await probe.stop();

        stall_probe serialize_probe;
        serialize_probe.start();
        const auto serialize_start = clock::now();
        current = co_await dedup_stm_test_accessor::serialize(std::move(wire));
        serialize_m.total = clock::now() - serialize_start;
        serialize_m.stall = co_await serialize_probe.stop();
    }

    // One answer produced two ways, so the comparison is between two costs
    // and not between two different amounts of work. Compared as a bool: on
    // a mismatch gtest would otherwise try to print both buffers.
    ASSERT_EQ_CORO(legacy.size_bytes(), current.size_bytes());
    ASSERT_TRUE_CORO(legacy == current);

    const auto worst_now = std::max(copy_m.stall, serialize_m.stall);
    std::cout << "\ndedup snapshot of " << entries << " entries ("
              << legacy.size_bytes() << " wire bytes)\n"
              << "  before  worst stall " << us(legacy_m.stall) << "us   total "
              << us(legacy_m.total) << "us\n"
              << "  after   worst stall " << us(worst_now) << "us   total "
              << us(copy_m.total + serialize_m.total) << "us\n"
              << "    of which index copy (cannot yield) stall "
              << us(copy_m.stall) << "us   total " << us(copy_m.total) << "us\n"
              << "    of which serialization (yields)     stall "
              << us(serialize_m.stall) << "us   total " << us(serialize_m.total)
              << "us\n"
              << std::flush;

    // The claim under test is that the work is no longer one indivisible
    // task. Half rather than the factor actually observed, because the
    // baseline here is three of the original four passes while the copy pass
    // survives in full -- and because a loaded runner must not be able to
    // turn a real improvement into a red build.
    ASSERT_LT_CORO(us(worst_now) * 2, us(legacy_m.stall));

    // The yielding half is the part that should have stopped mattering: its
    // stall is bounded by the task quota plus one 1024-entry batch, whatever
    // the index size, so it must no longer scale with the index.
    ASSERT_LT_CORO(us(serialize_m.stall), 10'000);
}

} // namespace

// An index this size is what put 123-124ms reactor stalls in the produce and
// raft_recv scheduling groups: request_make_snapshot_in_background() spawns
// the write from whichever append crossed the segment or dirty-byte
// threshold, so it runs in that fiber's scheduling group.
TEST_CORO(dedup_snapshot_stall, yielding_write_bounds_the_stall) {
    co_await compare_at(1'000'000);
    co_await compare_at(6'000'000);
}

} // namespace cluster
