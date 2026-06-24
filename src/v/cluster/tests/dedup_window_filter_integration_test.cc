// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Integration tests for the write-path dedup logic against a real Raft group.
//
// These tests extend simple_raft_fixture with a dedup_window_filter and
// simulate the decision that partition::replicate_in_stages makes:
//   - plain batches  : passed through dedup_window_filter before replication
//   - idempotent/txn : bypassed — replicated directly regardless of key overlap
//
// Each test verifies that committed_offset advances (replicated) or stays fixed
// (skipped).

#include "bytes/iobuf.h"
#include "cluster/dedup_window_filter.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "raft/replicate.h"
#include "raft/tests/simple_raft_fixture.h"
#include "storage/record_batch_builder.h"
#include "test_utils/boost_fixture.h"

#include <boost/test/unit_test.hpp>

#include <chrono>
#include <optional>

using namespace std::chrono_literals;

namespace {

model::timestamp ts(int64_t ms) { return model::timestamp{ms}; }

model::record_batch make_plain_batch(
  std::string_view key, std::string_view value, model::timestamp batch_ts) {
    storage::record_batch_builder b(
      model::record_batch_type::raft_data, model::offset{0});
    b.set_timestamp(batch_ts);
    b.add_raw_kv(iobuf::from(key), iobuf::from(value));
    return std::move(b).build();
}

// Builds a batch with producer_id > -1 so batch_identity::is_idempotent()
// returns true — the condition that triggers bypass in replicate_in_stages.
model::record_batch make_idempotent_batch(
  std::string_view key, std::string_view value, model::timestamp batch_ts) {
    storage::record_batch_builder b(
      model::record_batch_type::raft_data, model::offset{0});
    b.set_timestamp(batch_ts);
    b.set_producer_identity(42, 0); // pid.id=42 > no_producer_id(-1)
    b.add_raw_kv(iobuf::from(key), iobuf::from(value));
    return std::move(b).build();
}

} // namespace

// Fixture: one-node Raft group + a dedup_window_filter.
// The two helper methods mirror the branching in
// partition::replicate_in_stages.
struct dedup_raft_fixture : simple_raft_fixture {
    static constexpr std::chrono::milliseconds window{1000};

    dedup_raft_fixture()
      : filter(window) {
        start_raft();
        wait_for_confirmed_leader();
        wait_for_meta_initialized();
    }

    // Plain produce path: apply filter, replicate only if batch survives.
    // Returns the committed_offset after the call (unchanged if all-dropped).
    model::offset do_plain_produce(model::record_batch batch) {
        auto filtered = filter.filter(std::move(batch));
        if (!filtered) {
            return _raft->committed_offset();
        }
        auto res = _raft
                     ->replicate(
                       std::move(*filtered),
                       raft::replicate_options(
                         raft::consistency_level::quorum_ack))
                     .get();
        BOOST_REQUIRE(res.has_value());
        return _raft->committed_offset();
    }

    // Idempotent/transactional bypass: replicate directly, no filter.
    model::offset do_bypass_produce(model::record_batch batch) {
        auto res = _raft
                     ->replicate(
                       std::move(batch),
                       raft::replicate_options(
                         raft::consistency_level::quorum_ack))
                     .get();
        BOOST_REQUIRE(res.has_value());
        return _raft->committed_offset();
    }

    cluster::dedup_window_filter filter;
};

// First produce commits to the log; a duplicate within the window does not
// advance committed_offset; the same key after window expiry commits again.
FIXTURE_TEST(plain_batch_dedup_through_raft, dedup_raft_fixture) {
    auto before = _raft->committed_offset();

    auto off1 = do_plain_produce(make_plain_batch("k", "v1", ts(1000)));
    BOOST_CHECK_GT(off1, before);

    // Duplicate within window: skipped — committed_offset unchanged.
    auto off2 = do_plain_produce(make_plain_batch("k", "v2", ts(1500)));
    BOOST_CHECK_EQUAL(off2, off1);

    // Window expired (delta 1100ms > 1000ms): committed again.
    auto off3 = do_plain_produce(make_plain_batch("k", "v3", ts(2100)));
    BOOST_CHECK_GT(off3, off1);
}

// An idempotent batch (producer_id > -1) is never passed through the filter,
// so it commits to the log even when its key is a duplicate within the window.
FIXTURE_TEST(idempotent_batch_bypasses_dedup_filter, dedup_raft_fixture) {
    auto off1 = do_plain_produce(make_plain_batch("k", "v1", ts(1000)));
    BOOST_CHECK_GT(off1, model::offset{-1});

    // Confirm plain duplicate is filtered.
    auto off_dup = do_plain_produce(make_plain_batch("k", "v2", ts(1200)));
    BOOST_CHECK_EQUAL(off_dup, off1);

    // Idempotent producer with same key in same window: committed.
    auto off_idm = do_bypass_produce(
      make_idempotent_batch("k", "v2", ts(1300)));
    BOOST_CHECK_GT(off_idm, off1);
}

// Different keys are tracked independently: each first-occurrence commits and
// each within-window duplicate is skipped.
FIXTURE_TEST(independent_keys_commit_once_per_window, dedup_raft_fixture) {
    auto off_a1 = do_plain_produce(make_plain_batch("a", "1", ts(1000)));
    auto off_b1 = do_plain_produce(make_plain_batch("b", "2", ts(1000)));
    BOOST_CHECK_GT(off_b1, off_a1);

    // Both "a" and "b" dups are skipped — offset stays at off_b1.
    auto off_a2 = do_plain_produce(make_plain_batch("a", "3", ts(1200)));
    BOOST_CHECK_EQUAL(off_a2, off_b1);
    auto off_b2 = do_plain_produce(make_plain_batch("b", "4", ts(1200)));
    BOOST_CHECK_EQUAL(off_b2, off_b1);

    // Both keys expire together; their next produces both commit.
    auto off_a3 = do_plain_produce(make_plain_batch("a", "5", ts(2100)));
    BOOST_CHECK_GT(off_a3, off_b1);
    auto off_b3 = do_plain_produce(make_plain_batch("b", "6", ts(2100)));
    BOOST_CHECK_GT(off_b3, off_a3);
}

// clear() drops all state: a subsequent produce of the same key within what
// would have been the window commits as a new first occurrence.
FIXTURE_TEST(clear_resets_raft_state, dedup_raft_fixture) {
    auto off1 = do_plain_produce(make_plain_batch("k", "v1", ts(1000)));
    BOOST_CHECK_GT(off1, model::offset{-1});

    filter.clear();

    // Key "k" is now unknown — commits even inside original window.
    auto off2 = do_plain_produce(make_plain_batch("k", "v2", ts(1500)));
    BOOST_CHECK_GT(off2, off1);
}
