// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Quick produce-path comparison of dedup on vs. dedup off. Drives
// cluster::dedup_window_filter directly (no Raft/partition dependencies),
// mirroring what dedup_stm.cc actually calls on the leader (filter_request)
// and on every replica (populate).

#include "bytes/iobuf.h"
#include "cluster/dedup_window_filter.h"
#include "model/record.h"
#include "random/generators.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/sstring.hh>
#include <seastar/testing/perf_tests.hh>

#include <chrono>
#include <vector>

using namespace std::chrono_literals;

namespace {

constexpr size_t records_per_batch = 128;
constexpr size_t inner_iters = 2000;
constexpr size_t key_size = 16;
constexpr size_t value_size = 128;

model::record_batch make_batch(size_t n, bool unique_keys) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset{0});
    builder.set_timestamp(model::timestamp{1000});
    auto value = random_generators::gen_alphanum_string(value_size);
    for (size_t i = 0; i < n; ++i) {
        ss::sstring key = unique_keys
                            ? random_generators::gen_alphanum_string(key_size)
                            : ss::sstring{"fixed-dedup-key"};
        builder.add_raw_kv(iobuf::from(key), iobuf::from(value));
    }
    return std::move(builder).build();
}

} // namespace

// Baseline: what the produce path actually does when dedup is disabled --
// dedup_stm::do_replicate skips filter_request entirely and just moves the
// batch through. This is the floor everything else below is measured
// against.
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

// Worst case with dedup enabled: every record has a key never seen before,
// so filter_request() must do a full hash-map insertion for each one and
// never gets to short-circuit on a lookup hit. Batches are pre-built outside
// the timed region so random key generation doesn't pollute the
// measurement; keys are globally unique across the whole run so nothing
// collides and the insert path is exercised every time.
PERF_TEST(dedup_produce_path, dedup_enabled_all_new_keys) {
    cluster::dedup_window_filter filter(60s);
    std::vector<model::record_batch> batches;
    batches.reserve(inner_iters);
    for (size_t i = 0; i < inner_iters; ++i) {
        batches.push_back(make_batch(records_per_batch, /*unique_keys=*/true));
    }
    perf_tests::start_measuring_time();
    for (auto& b : batches) {
        auto result = filter.filter_request(std::move(b));
        perf_tests::do_not_optimize(result);
    }
    perf_tests::stop_measuring_time();
    return inner_iters * records_per_batch;
}

// Best case with dedup enabled: every record's key was already admitted, so
// filter_request() hits the map on the first lookup and drops the record
// without inserting. One warmup call (outside the timed region) seeds the
// map; every record in the batch shares a fixed timestamp equal to what's
// now stored, so every later pass sees diff_ms == 0 <= window and rejects
// on the fast path.
PERF_TEST(dedup_produce_path, dedup_enabled_all_duplicates) {
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

// The apply-path tax: populate() runs unconditionally on every replica
// (leader included, via its own committed batches) for every record when
// dedup is configured, regardless of how many duplicates the leader caught.
// This is the cost dedup adds to followers, which never call
// filter_request() at all.
PERF_TEST(dedup_produce_path, dedup_apply_path_populate) {
    cluster::dedup_window_filter filter(60s);
    std::vector<iobuf> identities;
    identities.reserve(inner_iters * records_per_batch);
    for (size_t i = 0; i < inner_iters * records_per_batch; ++i) {
        identities.push_back(
          iobuf::from(random_generators::gen_alphanum_string(key_size)));
    }
    perf_tests::start_measuring_time();
    for (auto& identity : identities) {
        filter.populate(identity, model::timestamp{1000});
    }
    perf_tests::stop_measuring_time();
    return inner_iters * records_per_batch;
}
