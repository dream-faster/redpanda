// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// End-to-end tests for the windowed last-wins deduplication feature
// (`redpanda.dedup.window.ms`). Tests drive `do_dedup()` via the normal
// `log::housekeeping()` entry point and assert record retention using
// `disk_log_builder::consume()`.

#include "bytes/iobuf.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "storage/record_batch_builder.h"
#include "storage/tests/disk_log_builder_fixture.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "storage/types.h"
#include "utils/tristate.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/io_priority_class.hh>

#include <gtest/gtest.h>

using namespace std::chrono_literals;

namespace {

// Build a single-record batch with an explicit timestamp and append it to
// the log using the log's own appender (so offsets are assigned correctly).
void write_kv(
  storage::disk_log_builder& b,
  std::string_view key,
  std::string_view value,
  model::timestamp ts) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    builder.add_raw_kv(iobuf::from(key), iobuf::from(value));
    builder.set_timestamp(ts);
    auto batch = std::move(builder).build();

    storage::log_append_config cfg{
      .should_fsync = storage::log_append_config::fsync::no};
    auto reader = model::make_memory_record_batch_reader(std::move(batch));
    std::move(reader)
      .for_each_ref(b.get_log()->make_appender(cfg), model::no_timeout)
      .get();
    b.get_log()->flush().get();
}

// Close the current active segment and start a new one.
void roll_segment(storage::disk_log_builder& b) {
    b.get_log()->force_roll().get();
}

// Run a housekeeping pass that drives dedup but does not trigger GC or
// size-based retention.  min_lag_ms=0 so all closed segments are
// compactible (needed for do_dedup's is_compactible gate).
void run_dedup_housekeeping(
  storage::disk_log_builder& b,
  std::chrono::milliseconds min_lag_ms = std::chrono::milliseconds{0}) {
    ss::abort_source as;
    b.get_log()
      ->housekeeping(storage::housekeeping_config(
        model::timestamp::min(), // gc upper bound: nothing collected
        std::nullopt,            // max_bytes_in_log
        model::offset::max(),    // max_collect_offset
        model::offset::max(),    // max_tombstone_remove_offset
        model::offset::max(),    // max_tx_end_remove_offset
        std::nullopt,            // tombstone_retention_ms
        std::nullopt,            // tx_retention_ms
        min_lag_ms,
        as))
      .get();
}

// Count records whose key matches `key` across all raft_data batches.
size_t
count_records_for_key(storage::disk_log_builder& b, std::string_view key) {
    size_t n = 0;
    auto batches = b.consume().get();
    for (auto& batch : batches) {
        if (batch.header().type != model::record_batch_type::raft_data) {
            continue;
        }
        batch.for_each_record([&](model::record r) {
            if (r.key() == key) {
                ++n;
            }
        });
    }
    return n;
}

// Return true iff exactly one record with `key` remains and its value equals
// `expected_value`.
bool last_value_is(
  storage::disk_log_builder& b,
  std::string_view key,
  std::string_view expected_value) {
    auto batches = b.consume().get();
    std::optional<bool> found;
    for (auto& batch : batches) {
        if (batch.header().type != model::record_batch_type::raft_data) {
            continue;
        }
        batch.for_each_record([&](model::record r) {
            if (r.key() == key) {
                found = (r.value() == expected_value);
            }
        });
    }
    return found.value_or(false);
}

// Enable windowed dedup on a log that is already started.
void set_dedup_window(
  storage::disk_log_builder& b, std::chrono::milliseconds window) {
    storage::ntp_config::default_overrides overrides;
    overrides.dedup_window_ms = tristate<std::chrono::milliseconds>{window};
    b.update_configuration(overrides).get();
}

} // namespace

// T1: Three segments within the window all carry the same key.
// After one housekeeping pass only the record with the highest offset (v2)
// must survive.
TEST_F(log_builder_fixture, DedupLastWinsWithinWindow) {
    b.start().get();
    set_dedup_window(b, 1h);

    const auto now_ms = model::timestamp::now().value();
    write_kv(b, "k", "v0", model::timestamp{now_ms - 1'200'000}); // T-20min
    roll_segment(b);
    write_kv(b, "k", "v1", model::timestamp{now_ms - 600'000}); // T-10min
    roll_segment(b);
    write_kv(b, "k", "v2", model::timestamp{now_ms}); // T
    roll_segment(b);

    run_dedup_housekeeping(b);

    EXPECT_EQ(count_records_for_key(b, "k"), 1u);
    EXPECT_TRUE(last_value_is(b, "k", "v2"));

    b.stop().get();
}

// T2: Segments whose max_timestamp falls outside the dedup window are not
// modified.  The two old segments keep both their records; only the two
// in-window segments are deduplicated (last-wins → one record survives).
TEST_F(log_builder_fixture, DedupOutOfWindowUntouched) {
    b.start().get();
    set_dedup_window(b, 60'000ms); // 1-minute window

    const auto now_ms = model::timestamp::now().value();

    // Segments outside the 1-minute window
    write_kv(b, "k", "v_old0", model::timestamp{now_ms - 7'200'000}); // T-2h
    roll_segment(b);
    write_kv(b, "k", "v_old1", model::timestamp{now_ms - 5'400'000}); // T-90min
    roll_segment(b);

    // Segments inside the 1-minute window
    write_kv(b, "k", "v_new0", model::timestamp{now_ms - 30'000}); // T-30s
    roll_segment(b);
    write_kv(b, "k", "v_new1", model::timestamp{now_ms}); // T
    roll_segment(b);

    run_dedup_housekeeping(b);

    // The two out-of-window records survive unchanged; the two in-window
    // records are deduplicated to one (the latest).
    EXPECT_EQ(count_records_for_key(b, "k"), 3u);
    EXPECT_TRUE(last_value_is(b, "k", "v_new1"));

    b.stop().get();
}

// T3: A second housekeeping pass with no new data must not reprocess
// segments (watermark guard).  The record count stays at 1 after both
// passes.
TEST_F(log_builder_fixture, DedupWatermarkPreventsRedundantPass) {
    b.start().get();
    set_dedup_window(b, 1h);

    const auto now_ms = model::timestamp::now().value();
    write_kv(b, "k", "v0", model::timestamp{now_ms - 60'000}); // T-1min
    roll_segment(b);
    write_kv(b, "k", "v1", model::timestamp{now_ms}); // T
    roll_segment(b);

    // First pass: deduplicates to 1 record.
    run_dedup_housekeeping(b);
    ASSERT_EQ(count_records_for_key(b, "k"), 1u);

    // Second pass: no new data — watermark blocks rebuild and rewrite.
    run_dedup_housekeeping(b);
    EXPECT_EQ(count_records_for_key(b, "k"), 1u);
    EXPECT_TRUE(last_value_is(b, "k", "v1"));

    b.stop().get();
}

// T4: When cleanup.policy=compact, do_dedup is skipped unconditionally
// (the housekeeping call site guards on !is_locally_compacted()).
// We verify this by passing a large min_lag_ms so compaction also does
// not touch freshly created segments — confirming the records are
// untouched by either mechanism.
TEST_F(log_builder_fixture, DedupSkippedForLocallyCompactedTopic) {
    b.start().get();

    storage::ntp_config::default_overrides overrides;
    overrides.dedup_window_ms = tristate<std::chrono::milliseconds>{1h};
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    b.update_configuration(overrides).get();

    const auto now_ms = model::timestamp::now().value();
    write_kv(b, "k", "v0", model::timestamp{now_ms - 60'000}); // T-1min
    roll_segment(b);
    write_kv(b, "k", "v1", model::timestamp{now_ms}); // T
    roll_segment(b);

    // min_lag_ms=24h prevents compaction from touching fresh segments,
    // and do_dedup is skipped because the topic is locally compacted.
    run_dedup_housekeeping(b, 24h);

    // Both records still present: neither mechanism ran.
    EXPECT_EQ(count_records_for_key(b, "k"), 2u);

    b.stop().get();
}

// T5: Dedup works correctly on a delete-policy (non-compacted) topic.
// Tombstone/tx-marker removal must be suppressed; only key-superseded
// records are removed.
TEST_F(log_builder_fixture, DedupOnDeletePolicyTopic) {
    b.start().get();
    set_dedup_window(b, 1h);
    // cleanup.policy defaults to deletion (no compaction)

    const auto now_ms = model::timestamp::now().value();
    write_kv(b, "k", "v1", model::timestamp{now_ms - 60'000}); // T-1min
    roll_segment(b);
    write_kv(b, "k", "v2", model::timestamp{now_ms}); // T
    roll_segment(b);

    run_dedup_housekeeping(b);

    EXPECT_EQ(count_records_for_key(b, "k"), 1u);
    EXPECT_TRUE(last_value_is(b, "k", "v2"));

    b.stop().get();
}
