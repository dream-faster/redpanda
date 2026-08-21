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
#include "config/mock_property.h"
#include "model/batch_builder.h"
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

    static ss::future<> apply_local_snapshot(dedup_stm& stm, iobuf buffer) {
        co_await stm.apply_local_snapshot(
          raft::stm_snapshot_header{}, std::move(buffer));
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

    // The STM apply path reads the dedup window, generation, and key header
    // from the partition's ntp_config; in production these arrive via topic
    // config propagation. Mirror that here by setting the log overrides on
    // every node.
    void set_dedup_config(
      std::chrono::milliseconds window,
      int64_t generation = 0,
      std::optional<ss::sstring> key_header = std::nullopt) {
        for (auto& [_, n] : nodes()) {
            storage::ntp_config::default_overrides overrides;
            overrides.dedup_window_ms = tristate<std::chrono::milliseconds>(
              window);
            overrides.dedup_generation = generation;
            overrides.dedup_key_header = key_header;
            n->raft()->log()->set_overrides(overrides);
        }
    }

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

} // namespace
} // namespace cluster

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
    // populated with entries decoded before the failure.
    co_await dedup_stm_test_accessor::apply_local_snapshot(
      *stm, std::move(downgraded));
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

    co_await dedup_stm_test_accessor::apply_local_snapshot(
      *stm, std::move(snapshot));
    ASSERT_EQ_CORO(stm->map_size(), 1);

    // The restored digest still matches the identity it came from, so a
    // duplicate within the window is still caught.
    auto duplicate = co_await produce_batch(
      leader, make_batch("round-trip", "v2", model::timestamp{1500}));
    ASSERT_TRUE_CORO(duplicate.has_value());
    ASSERT_EQ_CORO(duplicate.value().replicated_record_count, 0);
}
