// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/dedup_stm.h"
#include "cluster/logger.h"
#include "config/mock_property.h"
#include "raft/tests/raft_fixture.h"
#include "storage/record_batch_builder.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

#include <string_view>

using namespace std::chrono_literals;

namespace cluster {

struct dedup_stm_test_accessor {
    static size_t snapshot_size(iobuf snapshot) {
        return serde::from_iobuf<dedup_stm::state_snapshot>(std::move(snapshot))
          .entries.size();
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

struct dedup_stm_fixture : raft::stm_raft_fixture<dedup_stm> {
    stm_shptrs_t create_stms(
      raft::state_machine_manager_builder& builder,
      raft::raft_node_instance& node) final {
        return builder.create_stm<dedup_stm>(
          node.raft().get(),
          clusterlog,
          node.get_kvstore(),
          sync_timeout.bind());
    }

    // The STM apply path reads the dedup window and generation from the
    // partition's ntp_config; in production these arrive via topic config
    // propagation. Mirror that here by setting the log overrides on every
    // node.
    void
    set_dedup_config(std::chrono::milliseconds window, int64_t generation = 0) {
        for (auto& [_, n] : nodes()) {
            storage::ntp_config::default_overrides overrides;
            overrides.dedup_window_ms = tristate<std::chrono::milliseconds>(
              window);
            overrides.dedup_generation = generation;
            n->raft()->log()->set_overrides(overrides);
        }
    }

    ss::future<kafka_result> produce(
      model::node_id leader,
      std::string_view key,
      std::string_view value,
      model::timestamp timestamp,
      int64_t generation = 0) {
        auto stm = get_stm<0>(node(leader));
        auto stages = stm->replicate_in_stages(
          make_batch(key, value, timestamp),
          raft::replicate_options(raft::consistency_level::quorum_ack),
          1000ms,
          generation);
        co_await std::move(stages.request_enqueued);
        auto result = co_await std::move(stages.replicate_finished);
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

TEST_F_CORO(dedup_stm_fixture, generation_change_invalidates_old_state) {
    co_await initialize_state_machines();
    set_dedup_config(1000ms, 0);
    auto leader = co_await wait_for_leader(10s);

    auto first = co_await produce(
      leader, "key", "value-1", model::timestamp{1000}, 0);
    ASSERT_EQ_CORO(first.replicated_record_count, -1);
    auto duplicate = co_await produce(
      leader, "key", "value-2", model::timestamp{1100}, 0);
    ASSERT_EQ_CORO(duplicate.replicated_record_count, 0);

    // A generation bump models disabling and re-enabling dedup: the old
    // index no longer applies and the same key is admitted again.
    set_dedup_config(1000ms, 1);
    auto after_reenable = co_await produce(
      leader, "key", "value-3", model::timestamp{1200}, 1);
    ASSERT_EQ_CORO(after_reenable.replicated_record_count, -1);
    co_await wait_for_stms(node(leader).raft()->committed_offset());

    auto old_term = node(leader).raft()->term();
    auto transfer = co_await node(leader).raft()->transfer_leadership(
      {.group = node(leader).raft()->group(), .timeout = 10s});
    ASSERT_TRUE_CORO(transfer.success);
    auto new_leader = co_await wait_for_leader_change(
      model::timeout_clock::now() + 10s, old_term);

    duplicate = co_await produce(
      new_leader, "key", "value-4", model::timestamp{1300}, 1);
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

} // namespace
} // namespace cluster
