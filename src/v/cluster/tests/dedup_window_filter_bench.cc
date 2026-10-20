// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// What dedup costs per record, next to not having it. Drives
// cluster::dedup_window_filter directly (no Raft/partition dependencies),
// mirroring what dedup_stm.cc calls: filter_request() on the leader for every
// plain produce, the identity walk plus populate() on every replica for every
// committed batch, and the snapshot write and restore.
//
//   bazel run --config=release
//   //src/v/cluster/tests:dedup_window_filter_rpbench

#include "bytes/iobuf.h"
#include "cluster/dedup_stm.h"
#include "cluster/dedup_window_filter.h"
#include "model/batch_builder.h"
#include "model/record.h"
#include "random/generators.h"
#include "serde/rw/envelope.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sstring.hh>
#include <seastar/testing/perf_tests.hh>

#include <chrono>
#include <vector>

using namespace std::chrono_literals;

namespace cluster {

// Reaches dedup_stm's private snapshot functions; see dedup_stm.h.
struct dedup_stm_test_accessor {
    // Coroutines, not plain forwarders: serialize_snapshot() reads the
    // generation by reference after its last yield.
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

} // namespace cluster

namespace {

constexpr size_t records_per_batch = 128;
constexpr size_t inner_iters = 2000;
constexpr size_t key_size = 16;
constexpr size_t value_size = 128;
constexpr model::timestamp batch_ts{1000};
const ss::sstring identity_header = "dedup-id";

model::record_batch make_batch(size_t n, bool unique_keys) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset{0});
    builder.set_timestamp(batch_ts);
    auto value = random_generators::gen_alphanum_string(value_size);
    for (size_t i = 0; i < n; ++i) {
        ss::sstring key = unique_keys
                            ? random_generators::gen_alphanum_string(key_size)
                            : ss::sstring{"fixed-dedup-key"};
        builder.add_raw_kv(iobuf::from(key), iobuf::from(value));
    }
    return std::move(builder).build();
}

// A batch whose records carry three headers, the identity in the middle, as a
// client propagating tracing headers next to its own id would send.
model::record_batch make_header_batch(size_t n) {
    model::batch_builder builder;
    builder.set_batch_type(model::record_batch_type::raft_data);
    builder.set_batch_timestamp(model::timestamp_type::create_time, batch_ts);
    auto value = random_generators::gen_alphanum_string(value_size);
    for (size_t i = 0; i < n; ++i) {
        chunked_vector<model::record_header> headers;
        headers.emplace_back(
          iobuf::from("trace-id"), iobuf::from("0123456789abcdef"));
        headers.emplace_back(
          iobuf::from(identity_header),
          iobuf::from(random_generators::gen_alphanum_string(key_size)));
        headers.emplace_back(iobuf::from("content-type"), iobuf::from("json"));
        builder.add_record(
          model::record(
            {},
            0,
            static_cast<int32_t>(i),
            iobuf::from(random_generators::gen_alphanum_string(key_size)),
            iobuf::from(value),
            std::move(headers)));
    }
    return std::move(builder).build_sync();
}

std::vector<model::record_batch> make_batches(size_t count, bool headers) {
    std::vector<model::record_batch> batches;
    batches.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        batches.push_back(
          headers ? make_header_batch(records_per_batch)
                  : make_batch(records_per_batch, /*unique_keys=*/true));
    }
    return batches;
}

// The apply path before the identity walk: every record parsed into a
// model::record (a copy of its key, value and headers), then digested.
void parse_records_then_populate(
  cluster::dedup_window_filter& filter, const model::record_batch& batch) {
    const auto base_ts = batch.header().first_timestamp;
    batch.for_each_record([&](model::record r) {
        if (r.has_key()) {
            filter.populate(
              cluster::dedup_digest_of(r.key()),
              model::timestamp{base_ts.value() + r.timestamp_delta()});
        }
    });
}

void walk_identities_then_populate(
  cluster::dedup_window_filter& filter,
  const model::record_batch& batch,
  const std::optional<ss::sstring>& key_header) {
    cluster::for_each_dedup_identity(
      batch,
      key_header,
      [&filter](
        cluster::dedup_identity_lookup lookup,
        cluster::dedup_identity_digest digest,
        model::timestamp ts) {
          if (lookup == cluster::dedup_identity_lookup::identity) {
              filter.populate(digest, ts);
          }
          return true;
      });
}

// What dedup_stm::do_apply() runs: the identities digested first, then
// indexed in one populate(), which prefetches each lookup a few ahead.
void walk_identities_then_batch_populate(
  cluster::dedup_window_filter& filter,
  const model::record_batch& batch,
  const std::optional<ss::sstring>& key_header) {
    chunked_vector<cluster::dedup_index_entry> entries;
    entries.reserve(static_cast<size_t>(batch.record_count()));
    cluster::for_each_dedup_identity(
      batch,
      key_header,
      [&entries](
        cluster::dedup_identity_lookup lookup,
        cluster::dedup_identity_digest digest,
        model::timestamp ts) {
          if (lookup == cluster::dedup_identity_lookup::identity) {
              entries.push_back({.identity = digest, .timestamp = ts});
          }
          return true;
      });
    filter.populate(entries);
}

constexpr size_t snapshot_entries = 1'000'000;

void fill(cluster::dedup_window_filter& filter, size_t entries) {
    cluster::dedup_index_snapshot seed;
    seed.entries.reserve(entries);
    for (size_t i = 0; i < entries; ++i) {
        // Odd multipliers are bijections mod 2^64: distinct identities whose
        // high bits move, which is what the index picks a segment by.
        seed.entries.push_back(
          {.identity
           = {.hi = 0xbf58'476d'1ce4'e5b9ULL * (i + 1), .lo = 0x9e37'79b9'7f4a'7c15ULL * (i + 1)},
           .timestamp = model::timestamp{
             1'000'000'000 + static_cast<int64_t>(i % 1000)}});
    }
    seed.max_timestamp = model::timestamp{1'000'000'000 + 999};
    filter.restore(seed);
}

} // namespace

// Baseline: what the produce path does when dedup is disabled --
// dedup_stm::do_replicate skips filter_request entirely and just moves the
// batch through. This is the floor everything else is measured against.
PERF_TEST(dedup_produce_path, dedup_disabled_baseline) {
    auto batch = make_batch(records_per_batch, /*unique_keys=*/true);
    perf_tests::start_measuring_time();
    for (size_t i = inner_iters; i--;) {
        auto moved = batch.share();
        perf_tests::do_not_optimize(moved);
    }
    perf_tests::stop_measuring_time();
    return inner_iters * records_per_batch;
}

// Worst case on the leader: every record has a key never seen before, so
// filter_request() digests it, inserts it and takes a sweep step for each.
// Batches are pre-built outside the timed region.
PERF_TEST(dedup_produce_path, filter_all_new_keys) {
    cluster::dedup_window_filter filter(60s);
    auto batches = make_batches(inner_iters, /*headers=*/false);
    perf_tests::start_measuring_time();
    for (auto& b : batches) {
        auto result = filter.filter_request(std::move(b));
        perf_tests::do_not_optimize(result);
    }
    perf_tests::stop_measuring_time();
    return inner_iters * records_per_batch;
}

// The same, with the identity in a header: the walk skips the key and the
// other headers to reach it.
PERF_TEST(dedup_produce_path, filter_all_new_header_identities) {
    cluster::dedup_window_filter filter(60s);
    filter.set_key_header(identity_header);
    auto batches = make_batches(inner_iters, /*headers=*/true);
    perf_tests::start_measuring_time();
    for (auto& b : batches) {
        auto result = filter.filter_request(std::move(b));
        perf_tests::do_not_optimize(result);
    }
    perf_tests::stop_measuring_time();
    return inner_iters * records_per_batch;
}

// Best case on the leader: every record's key was already admitted, so each
// is a lookup hit and the batch is rebuilt without them.
PERF_TEST(dedup_produce_path, filter_all_duplicates) {
    cluster::dedup_window_filter filter(60s);
    auto batch = make_batch(records_per_batch, /*unique_keys=*/false);
    auto warmup = filter.filter_request(batch.share());
    perf_tests::do_not_optimize(warmup);

    perf_tests::start_measuring_time();
    for (size_t i = inner_iters; i--;) {
        auto result = filter.filter_request(batch.share());
        perf_tests::do_not_optimize(result);
    }
    perf_tests::stop_measuring_time();
    return inner_iters * records_per_batch;
}

// Reading the identities and nothing else: the parse cost of the apply path,
// without the index.
PERF_TEST(dedup_apply_path, identity_walk_only) {
    auto batches = make_batches(inner_iters, /*headers=*/false);
    perf_tests::start_measuring_time();
    for (auto& b : batches) {
        cluster::for_each_dedup_identity(
          b,
          std::nullopt,
          [](
            cluster::dedup_identity_lookup,
            cluster::dedup_identity_digest digest,
            model::timestamp) {
              perf_tests::do_not_optimize(digest);
              return true;
          });
    }
    perf_tests::stop_measuring_time();
    return inner_iters * records_per_batch;
}

// What the apply path parsed before the identity walk: every record into a
// model::record, copying its key, value and headers.
PERF_TEST(dedup_apply_path, record_parse_only) {
    auto batches = make_batches(inner_iters, /*headers=*/false);
    perf_tests::start_measuring_time();
    for (auto& b : batches) {
        b.for_each_record([](model::record r) {
            auto digest = cluster::dedup_digest_of(r.key());
            perf_tests::do_not_optimize(digest);
        });
    }
    perf_tests::stop_measuring_time();
    return inner_iters * records_per_batch;
}

// The apply path with one populate() per identity as it is walked, for the
// before/after of the batched populate() below.
PERF_TEST(dedup_apply_path, identity_walk_then_populate) {
    cluster::dedup_window_filter filter(60s);
    auto batches = make_batches(inner_iters, /*headers=*/false);
    perf_tests::start_measuring_time();
    for (auto& b : batches) {
        walk_identities_then_populate(filter, b, std::nullopt);
    }
    perf_tests::stop_measuring_time();
    return inner_iters * records_per_batch;
}

// The apply path as a follower runs it for every committed batch: walk the
// identities, then populate() them all at once.
PERF_TEST(dedup_apply_path, identity_walk_then_batch_populate) {
    cluster::dedup_window_filter filter(60s);
    auto batches = make_batches(inner_iters, /*headers=*/false);
    perf_tests::start_measuring_time();
    for (auto& b : batches) {
        walk_identities_then_batch_populate(filter, b, std::nullopt);
    }
    perf_tests::stop_measuring_time();
    return inner_iters * records_per_batch;
}

// The apply path before the walk, for the before/after.
PERF_TEST(dedup_apply_path, record_parse_then_populate) {
    cluster::dedup_window_filter filter(60s);
    auto batches = make_batches(inner_iters, /*headers=*/false);
    perf_tests::start_measuring_time();
    for (auto& b : batches) {
        parse_records_then_populate(filter, b);
    }
    perf_tests::stop_measuring_time();
    return inner_iters * records_per_batch;
}

PERF_TEST(dedup_apply_path, header_identity_walk_then_populate) {
    cluster::dedup_window_filter filter(60s);
    filter.set_key_header(identity_header);
    auto batches = make_batches(inner_iters, /*headers=*/true);
    perf_tests::start_measuring_time();
    for (auto& b : batches) {
        walk_identities_then_populate(filter, b, identity_header);
    }
    perf_tests::stop_measuring_time();
    return inner_iters * records_per_batch;
}

// The index at its ceiling: every new identity has to make room, so each
// insert pays the bounded sweep steps and the capacity bookkeeping. This is
// the steady state of a busy partition, and of every partition on a shard at
// its limit.
PERF_TEST(dedup_index, insert_at_the_ceiling) {
    constexpr size_t ceiling = 200'000;
    cluster::dedup_window_filter filter(1h, ceiling);
    for (size_t i = 0; i < ceiling; ++i) {
        filter.populate(
          cluster::dedup_identity_digest{
            .hi = 0xbf58'476d'1ce4'e5b9ULL * (i + 1),
            .lo = 0x9e37'79b9'7f4a'7c15ULL * (i + 1)},
          model::timestamp{1'000'000'000 + static_cast<int64_t>(i)});
    }
    constexpr size_t inserts = inner_iters * records_per_batch;
    std::vector<cluster::dedup_identity_digest> incoming;
    incoming.reserve(inserts);
    for (size_t i = 0; i < inserts; ++i) {
        incoming.push_back(
          {.hi = 0x94d0'49bb'1331'11ebULL * (i + 1),
           .lo = 0xd6e8'feb8'6659'fd93ULL * (i + 1)});
    }
    perf_tests::start_measuring_time();
    int64_t ts = 1'000'000'000 + static_cast<int64_t>(ceiling);
    for (const auto& identity : incoming) {
        filter.populate(identity, model::timestamp{ts++});
    }
    perf_tests::stop_measuring_time();
    return inserts;
}

struct dedup_snapshot {
    dedup_snapshot() { fill(filter, snapshot_entries); }
    cluster::dedup_window_filter filter{1h, 2 * snapshot_entries};
};

// The snapshot write: one pass over a million entries into the packed
// format, yielding between chunks. Per entry.
PERF_TEST_CN(dedup_snapshot, serialize_1m_entries) {
    perf_tests::start_measuring_time();
    auto out = co_await cluster::dedup_stm_test_accessor::serialize(filter, 1);
    perf_tests::stop_measuring_time();
    perf_tests::do_not_optimize(out);
    co_return snapshot_entries;
}

// The restore of that snapshot into an empty index at startup. Per entry.
PERF_TEST_CN(dedup_snapshot, restore_1m_entries) {
    auto buffer = co_await cluster::dedup_stm_test_accessor::serialize(
      filter, 1);
    cluster::dedup_window_filter target(1h, 2 * snapshot_entries);
    int64_t generation = 0;
    perf_tests::start_measuring_time();
    co_await cluster::dedup_stm_test_accessor::restore(
      target, generation, std::move(buffer));
    perf_tests::stop_measuring_time();
    perf_tests::do_not_optimize(target.map_size());
    co_return snapshot_entries;
}
