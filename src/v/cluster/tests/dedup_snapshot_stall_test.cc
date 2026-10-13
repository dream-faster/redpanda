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
#include "serde/rw/vector.h"
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

// Reaches dedup_stm::serialize_snapshot(), which is private. Each test binary
// links its own definition of this friend, so the one in dedup_stm_test.cc is
// a different struct in a different binary rather than a redefinition of this
// one.
struct dedup_stm_test_accessor {
    static iobuf
    serialize(const dedup_window_filter& state, int64_t generation) {
        return dedup_stm::serialize_snapshot(state, generation);
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
        // `lo` matters more than it looks: dedup_identity_digest_hash returns
        // it verbatim and declares is_avalanching, so the map does not re-mix
        // it, and unordered_dense takes the bucket index from the hash's
        // *high* bits. Anything whose high bits barely move across the range
        // -- a counter, or ~counter -- puts every key in one bucket
        // neighbourhood and turns these inserts quadratic. A real xxhash64
        // digest is spread; the multiplier here is what keeps the synthetic
        // one spread too.
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

    measurement packed_m;
    iobuf packed;
    {
        stall_probe probe;
        probe.start();
        const auto start = clock::now();
        packed = dedup_stm_test_accessor::serialize(filter, 7);
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
              << std::flush;

    // The serialized copy is the snapshot's whole memory cost: 24 bytes per
    // entry plus a fixed header, against the 30 of a per-entry envelope.
    ASSERT_LE_CORO(
      packed.size_bytes(), entries * dedup_stm::packed_entry_size + 64);
    ASSERT_LT_CORO(packed.size_bytes(), legacy_bytes);

    // The packed write is one pass that cannot yield, since the produce path
    // mutates the index without the apply lock. It replaces three such
    // passes, so it must stall the reactor for less than they did together.
    // Not by the factor observed, so a loaded runner cannot turn a real
    // improvement into a red build.
    ASSERT_LT_CORO(us(packed_m.stall), us(legacy_m.stall));
}

} // namespace

// An index this size is what put 123-124ms reactor stalls in the produce and
// raft_recv scheduling groups: request_make_snapshot_in_background() spawns
// the write from whichever append crossed the segment or dirty-byte
// threshold, so it runs in that fiber's scheduling group.
TEST_CORO(dedup_snapshot_stall, packed_write_is_smaller_and_stalls_less) {
    co_await compare_at(1'000'000);
    co_await compare_at(6'000'000);
}

} // namespace cluster
