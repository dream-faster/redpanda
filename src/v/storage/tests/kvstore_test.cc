// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/configuration.h"
#include "random/generators.h"
#include "storage/kvstore.h"
#include "storage/tests/kvstore_fixture.h"
#include "test_utils/random_bytes.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <map>
#include <optional>
#include <string_view>
#include <vector>

template<typename T>
static void set_configuration(ss::sstring p_name, T v) {
    ss::smp::invoke_on_all([p_name, v = std::move(v)] {
        config::shard_local_cfg().get(p_name).set_value(v);
    }).get();
}

TEST_F(kvstore_test_fixture, key_space) {
    set_configuration("disable_metrics", true);

    auto kvs = make_kvstore();
    kvs->start().get();

    const auto value_a = bytes_to_iobuf(tests::random_bytes(100));
    const auto value_b = bytes_to_iobuf(tests::random_bytes(100));
    const auto value_c = bytes_to_iobuf(tests::random_bytes(100));
    const auto value_d = bytes_to_iobuf(tests::random_bytes(100));

    const auto empty_key = bytes();
    const auto key = tests::random_bytes(2);

    kvs->put(storage::kvstore::key_space::testing, key, value_a.copy()).get();
    kvs->put(storage::kvstore::key_space::consensus, key, value_b.copy()).get();

    kvs->put(storage::kvstore::key_space::testing, empty_key, value_c.copy())
      .get();
    kvs->put(storage::kvstore::key_space::consensus, empty_key, value_d.copy())
      .get();

    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, key).value(), value_a);
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::consensus, key).value(), value_b);
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, empty_key).value(),
      value_c);
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::consensus, empty_key).value(),
      value_d);

    std::map<bytes, iobuf> testing_kvs;
    kvs
      ->for_each(
        storage::kvstore::key_space::testing,
        [&](bytes_view key, const iobuf& val) {
            EXPECT_TRUE(testing_kvs.emplace(key, val.copy()).second);
        })
      .get();
    EXPECT_EQ(testing_kvs.size(), 2);
    EXPECT_EQ(testing_kvs.at(key), value_a);
    EXPECT_EQ(testing_kvs.at(empty_key), value_c);

    kvs->stop().get();

    // still all true after recovery
    kvs = make_kvstore();
    kvs->start().get();

    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, key).value(), value_a);
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::consensus, key).value(), value_b);
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, empty_key).value(),
      value_c);
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::consensus, empty_key).value(),
      value_d);

    kvs->stop().get();
}

TEST_F(kvstore_test_fixture, kvstore_empty) {
    set_configuration("disable_metrics", true);

    // empty started then stopped
    auto kvs = make_kvstore();
    kvs->start().get();
    kvs->stop().get();

    // and can restart from empty
    kvs = make_kvstore();
    kvs->start().get();
    kvs->stop().get();

    std::unordered_map<bytes, iobuf> truth;

    // now fill it up with some key value pairs
    kvs = make_kvstore();
    kvs->start().get();

    std::vector<ss::future<>> batch;
    for (int i = 0; i < 500; i++) {
        auto key = tests::random_bytes(2);
        auto value = bytes_to_iobuf(tests::random_bytes(100));

        truth[key] = value.copy();
        batch.push_back(kvs->put(
          storage::kvstore::key_space::testing, key, std::move(value)));
        if (batch.size() > 10) {
            ss::when_all(batch.begin(), batch.end()).get();
            batch.clear();
        }
    }
    if (!batch.empty()) {
        ss::when_all(batch.begin(), batch.end()).get();
        batch.clear();
    }

    // equal
    EXPECT_FALSE(truth.empty());
    for (auto& e : truth) {
        EXPECT_EQ(
          kvs->get(storage::kvstore::key_space::testing, e.first).value(),
          e.second);
    }

    // now remove all of the keys
    for (auto& e : truth) {
        kvs->remove(storage::kvstore::key_space::testing, e.first).get();
    }
    truth.clear();

    // the db should be empty now
    EXPECT_TRUE(kvs->empty());
    kvs->stop().get();

    // now restart the db and ensure still empty
    kvs = make_kvstore();
    kvs->start().get();
    EXPECT_TRUE(kvs->empty());
    kvs->stop().get();
}

TEST_F(kvstore_test_fixture, kvstore) {
    set_configuration("disable_metrics", true);

    std::unordered_map<bytes, iobuf> truth;

    auto kvs = make_kvstore();
    kvs->start().get();
    for (int i = 0; i < 500; i++) {
        auto key = tests::random_bytes(2);
        auto value = bytes_to_iobuf(tests::random_bytes(100));

        truth[key] = value.copy();
        kvs->put(storage::kvstore::key_space::testing, key, std::move(value))
          .get();
        EXPECT_EQ(
          kvs->get(storage::kvstore::key_space::testing, key).value(),
          truth[key]);

        // maybe delete something
        auto coin = random_generators::get_int(1000);
        if (coin < 500) {
            auto key = tests::random_bytes(2);
            truth.erase(key);
            kvs->remove(storage::kvstore::key_space::testing, key).get();
        }

        for (auto& e : truth) {
            EXPECT_EQ(
              kvs->get(storage::kvstore::key_space::testing, e.first).value(),
              e.second);
        }
    }
    kvs->stop().get();
    kvs.reset(nullptr);

    // shutdown, restart, and verify all the original key-value pairs
    kvs = make_kvstore();
    kvs->start().get();
    for (auto& e : truth) {
        EXPECT_EQ(
          kvs->get(storage::kvstore::key_space::testing, e.first).value(),
          e.second);
    }
    kvs->stop().get();
}

// persist_pre_start() durably writes a key on a fresh (empty) kvstore, before
// start(), and the value survives a restart.
TEST_F(kvstore_test_fixture, persist_pre_start_empty) {
    set_configuration("disable_metrics", true);

    const auto key = tests::random_bytes(8);
    const auto value = bytes_to_iobuf(tests::random_bytes(128));

    auto kvs = make_kvstore();
    kvs->recover().get();
    kvs
      ->persist_pre_start(
        storage::kvstore::key_space::testing, key, value.copy())
      .get();
    kvs->start().get();
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, key).value(), value);
    kvs->stop().get();

    // Durable across a restart (it was written to a snapshot).
    kvs = make_kvstore();
    kvs->start().get();
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, key).value(), value);
    kvs->stop().get();
}

// persist_pre_start() must not corrupt a kvstore that already has state on disk
// (a snapshot and segments): the pre-existing keys and the injected key must
// both be present, before and after a restart.
TEST_F(kvstore_test_fixture, persist_pre_start_preserves_existing_state) {
    set_configuration("disable_metrics", true);

    // Populate with enough data (>> the 8KiB max segment size) to force segment
    // rolls and at least one snapshot, then stop so it's all on disk.
    std::map<bytes, iobuf> truth;
    auto kvs = make_kvstore();
    kvs->start().get();
    for (int i = 0; i < 200; i++) {
        auto key = tests::random_bytes(8);
        auto value = bytes_to_iobuf(tests::random_bytes(128));
        truth[key] = value.copy();
        kvs->put(storage::kvstore::key_space::testing, key, std::move(value))
          .get();
    }
    kvs->stop().get();

    // Recover that state, inject a new key via persist_pre_start (before
    // start()), then bring the store up.
    const auto sentinel_key = tests::random_bytes(8);
    const auto sentinel_value = bytes_to_iobuf(tests::random_bytes(128));
    kvs = make_kvstore();
    kvs->recover().get();
    kvs
      ->persist_pre_start(
        storage::kvstore::key_space::testing,
        sentinel_key,
        sentinel_value.copy())
      .get();
    kvs->start().get();

    // Pre-existing state is intact and the injected key is present.
    for (const auto& e : truth) {
        EXPECT_EQ(
          kvs->get(storage::kvstore::key_space::testing, e.first).value(),
          e.second);
    }
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, sentinel_key).value(),
      sentinel_value);
    kvs->stop().get();

    // ...and both survive a subsequent restart (snapshot/segment offsets line
    // up on recovery).
    kvs = make_kvstore();
    kvs->start().get();
    for (const auto& e : truth) {
        EXPECT_EQ(
          kvs->get(storage::kvstore::key_space::testing, e.first).value(),
          e.second);
    }
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, sentinel_key).value(),
      sentinel_value);
    kvs->stop().get();
}

// The bootstrap sequence: a pre-start write followed by normal post-start
// writes. This is the key offset-bookkeeping guarantee - the pre-start snapshot
// (at offset 0) and the later segment writes must be consistent on recovery, so
// everything survives a restart.
TEST_F(
  kvstore_test_fixture, persist_pre_start_then_normal_puts_survive_restart) {
    set_configuration("disable_metrics", true);

    const auto early_key = tests::random_bytes(8);
    const auto early_value = bytes_to_iobuf(tests::random_bytes(128));

    auto kvs = make_kvstore();
    kvs->recover().get();
    kvs
      ->persist_pre_start(
        storage::kvstore::key_space::testing, early_key, early_value.copy())
      .get();
    kvs->start().get();

    // Enough normal writes to roll segments and snapshot past the pre-start op.
    std::map<bytes, iobuf> truth;
    for (int i = 0; i < 200; i++) {
        auto key = tests::random_bytes(8);
        auto value = bytes_to_iobuf(tests::random_bytes(128));
        truth[key] = value.copy();
        kvs->put(storage::kvstore::key_space::testing, key, std::move(value))
          .get();
    }
    kvs->stop().get();

    kvs = make_kvstore();
    kvs->start().get();
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, early_key).value(),
      early_value);
    for (const auto& e : truth) {
        EXPECT_EQ(
          kvs->get(storage::kvstore::key_space::testing, e.first).value(),
          e.second);
    }
    kvs->stop().get();
}

// Multiple pre-start writes each advance the offset and snapshot; all must be
// durable across a restart.
TEST_F(kvstore_test_fixture, persist_pre_start_multiple_keys) {
    set_configuration("disable_metrics", true);

    std::map<bytes, iobuf> truth;
    auto kvs = make_kvstore();
    kvs->recover().get();
    for (int i = 0; i < 5; i++) {
        auto key = tests::random_bytes(8);
        auto value = bytes_to_iobuf(tests::random_bytes(128));
        truth[key] = value.copy();
        kvs
          ->persist_pre_start(
            storage::kvstore::key_space::testing, key, std::move(value))
          .get();
    }
    kvs->start().get();
    for (const auto& e : truth) {
        EXPECT_EQ(
          kvs->get(storage::kvstore::key_space::testing, e.first).value(),
          e.second);
    }
    kvs->stop().get();

    kvs = make_kvstore();
    kvs->start().get();
    for (const auto& e : truth) {
        EXPECT_EQ(
          kvs->get(storage::kvstore::key_space::testing, e.first).value(),
          e.second);
    }
    kvs->stop().get();
}

// The crash scenario the feature guards against: a value is persisted before
// start(), then the process dies before doing anything else. A fresh recovery
// must still see the value (it was durably snapshotted).
TEST_F(kvstore_test_fixture, persist_pre_start_survives_crash_before_start) {
    set_configuration("disable_metrics", true);

    const auto key = tests::random_bytes(8);
    const auto value = bytes_to_iobuf(tests::random_bytes(128));

    auto kvs = make_kvstore();
    kvs->recover().get();
    kvs
      ->persist_pre_start(
        storage::kvstore::key_space::testing, key, value.copy())
      .get();
    // Simulate a crash: drop the instance without ever start()ing or stop()ing
    // it. No flush fiber was spawned, so tearing down a recover-only kvstore is
    // clean.
    kvs.reset();

    kvs = make_kvstore();
    kvs->start().get();
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, key).value(), value);
    kvs->stop().get();
}

// A normal write after start() to the same key supersedes the pre-start value,
// and the newer value wins on recovery (the post-start op sits at a later
// offset than the pre-start snapshot).
TEST_F(kvstore_test_fixture, persist_pre_start_overwritten_by_later_put) {
    set_configuration("disable_metrics", true);

    const auto key = tests::random_bytes(8);
    const auto pre_start_value = bytes_to_iobuf(tests::random_bytes(128));
    const auto post_start_value = bytes_to_iobuf(tests::random_bytes(128));

    auto kvs = make_kvstore();
    kvs->recover().get();
    kvs
      ->persist_pre_start(
        storage::kvstore::key_space::testing, key, pre_start_value.copy())
      .get();
    kvs->start().get();
    kvs->put(storage::kvstore::key_space::testing, key, post_start_value.copy())
      .get();
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, key).value(),
      post_start_value);
    kvs->stop().get();

    // The later value wins after a restart.
    kvs = make_kvstore();
    kvs->start().get();
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, key).value(),
      post_start_value);
    kvs->stop().get();
}

// persist_pre_start updates (not just inserts) a key that already exists in the
// recovered db, and the update is durable.
TEST_F(kvstore_test_fixture, persist_pre_start_updates_existing_key) {
    set_configuration("disable_metrics", true);

    const auto key = tests::random_bytes(8);
    const auto old_value = bytes_to_iobuf(tests::random_bytes(128));
    const auto new_value = bytes_to_iobuf(tests::random_bytes(128));

    // Write the key normally and persist it.
    auto kvs = make_kvstore();
    kvs->start().get();
    kvs->put(storage::kvstore::key_space::testing, key, old_value.copy()).get();
    kvs->stop().get();

    // Recover, then update the same key via persist_pre_start before start().
    kvs = make_kvstore();
    kvs->recover().get();
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, key).value(), old_value);
    kvs
      ->persist_pre_start(
        storage::kvstore::key_space::testing, key, new_value.copy())
      .get();
    kvs->start().get();
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, key).value(), new_value);
    kvs->stop().get();

    // The update is durable across a restart.
    kvs = make_kvstore();
    kvs->start().get();
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, key).value(), new_value);
    kvs->stop().get();
}

// persist_pre_start writes into the given key space only; the same key in
// another key space is unaffected (verifies its key namespacing matches the
// normal read path).
TEST_F(kvstore_test_fixture, persist_pre_start_key_space_isolation) {
    set_configuration("disable_metrics", true);

    const auto key = tests::random_bytes(8);
    const auto value = bytes_to_iobuf(tests::random_bytes(128));

    auto kvs = make_kvstore();
    kvs->recover().get();
    kvs
      ->persist_pre_start(
        storage::kvstore::key_space::testing, key, value.copy())
      .get();
    kvs->start().get();
    EXPECT_EQ(
      kvs->get(storage::kvstore::key_space::testing, key).value(), value);
    EXPECT_FALSE(
      kvs->get(storage::kvstore::key_space::consensus, key).has_value());
    kvs->stop().get();
}

// ---------------------------------------------------------------------------
// Discarded stm snapshots
//
// An earlier build kept whole dedup indices in the kvstore, as values tens of
// MiB large. Loading them is what can stop a broker from starting, so recovery
// drops them before they are held in memory, from both the snapshot and the
// replayed segments, and rewrites the snapshot so the next start never reads
// them again.
// ---------------------------------------------------------------------------

namespace {

using key_space = storage::kvstore::key_space;

struct mixed_store {
    // Dropped: the discarded snapshot name, any ntp.
    std::vector<bytes> discarded{
      bytes::from_string("dedup.snapshot/kafka/topic/0"),
      bytes::from_string("dedup.snapshot/kafka/other/12")};
    // Kept: another stm's snapshot, a name that merely starts the same way,
    // and the discarded key itself in another key space.
    std::vector<std::pair<key_space, bytes>> kept{
      {key_space::stms, bytes::from_string("rm_stm.snapshot/kafka/topic/0")},
      {key_space::stms, bytes::from_string("dedup.snapshot.v2/kafka/topic/0")},
      {key_space::stms, bytes::from_string("dedup.snapshotkafka/topic/0")},
      {key_space::testing, bytes::from_string("dedup.snapshot/kafka/topic/0")},
      {key_space::consensus, bytes::from_string("voted_for")}};
    std::map<std::pair<key_space, bytes>, iobuf> kept_values;

    void write(storage::kvstore& kvs, size_t discarded_value_size) {
        for (const auto& key : discarded) {
            kvs
              .put(
                key_space::stms,
                key,
                bytes_to_iobuf(tests::random_bytes(discarded_value_size)))
              .get();
        }
        for (const auto& [ks, key] : kept) {
            auto value = bytes_to_iobuf(tests::random_bytes(64));
            kept_values[{ks, key}] = value.copy();
            kvs.put(ks, key, std::move(value)).get();
        }
    }

    void expect_discarded_gone(storage::kvstore& kvs) const {
        for (const auto& key : discarded) {
            EXPECT_FALSE(kvs.get(key_space::stms, key).has_value());
        }
    }

    void expect_discarded_present(storage::kvstore& kvs) const {
        for (const auto& key : discarded) {
            EXPECT_TRUE(kvs.get(key_space::stms, key).has_value());
        }
    }

    void expect_kept(storage::kvstore& kvs) const {
        for (const auto& [ks, key] : kept) {
            auto value = kvs.get(ks, key);
            ASSERT_TRUE(value.has_value());
            EXPECT_EQ(*value, kept_values.at({ks, key}));
        }
    }
};

std::optional<std::filesystem::path>
find_file(const std::filesystem::path& root, std::string_view name) {
    for (const auto& entry :
         std::filesystem::recursive_directory_iterator(root)) {
        if (entry.is_regular_file() && entry.path().filename() == name) {
            return entry.path();
        }
    }
    return std::nullopt;
}

} // namespace

// Small values stay in the open segment, so recovery finds them while
// replaying segments rather than in the snapshot.
TEST_F(kvstore_test_fixture, discarded_stm_snapshots_dropped_from_segments) {
    set_configuration("disable_metrics", true);
    mixed_store store;

    auto kvs = make_kvstore();
    kvs->start().get();
    store.write(*kvs, 16);
    // Never rolled, so no snapshot: the next recovery reads everything from
    // the segment. (Recovery itself snapshots and drops the segments it
    // replayed, so that has to be the discarding one.)
    store.expect_discarded_present(*kvs);
    kvs->stop().get();
    ASSERT_FALSE(find_file(std::filesystem::path(test_dir()), "snapshot"));

    kvs = make_kvstore({"dedup.snapshot"});
    kvs->start().get();
    store.expect_discarded_gone(*kvs);
    store.expect_kept(*kvs);
    kvs->stop().get();

    // The drop is durable: a store opened without the list does not see them
    // again.
    kvs = make_kvstore();
    kvs->start().get();
    store.expect_discarded_gone(*kvs);
    store.expect_kept(*kvs);
    kvs->stop().get();
}

// Values larger than the segment size roll it, which writes a snapshot, so
// recovery finds them in the snapshot it loads.
TEST_F(kvstore_test_fixture, discarded_stm_snapshots_dropped_from_snapshot) {
    set_configuration("disable_metrics", true);
    mixed_store store;

    auto kvs = make_kvstore();
    kvs->start().get();
    store.write(*kvs, 64 * 1024);
    kvs->stop().get();
    ASSERT_TRUE(find_file(std::filesystem::path(test_dir()), "snapshot"));

    kvs = make_kvstore({"dedup.snapshot"});
    kvs->start().get();
    store.expect_discarded_gone(*kvs);
    store.expect_kept(*kvs);
    kvs->stop().get();

    kvs = make_kvstore();
    kvs->start().get();
    store.expect_discarded_gone(*kvs);
    store.expect_kept(*kvs);
    kvs->stop().get();
}

// A discarded key written again after recovery is an ordinary key: the list
// only filters what recovery reads, never what the running store accepts.
TEST_F(kvstore_test_fixture, discarded_stm_snapshot_can_be_written_again) {
    set_configuration("disable_metrics", true);
    const auto key = bytes::from_string("dedup.snapshot/kafka/topic/0");

    auto kvs = make_kvstore({"dedup.snapshot"});
    kvs->start().get();
    kvs->put(key_space::stms, key, iobuf::from("fresh")).get();
    EXPECT_EQ(kvs->get(key_space::stms, key).value(), iobuf::from("fresh"));
    kvs->stop().get();
}

// Restoring the snapshot record by record must keep its integrity check: a
// corrupted snapshot still fails recovery rather than loading bad data.
TEST_F(kvstore_test_fixture, corrupted_snapshot_still_fails_recovery) {
    set_configuration("disable_metrics", true);
    mixed_store store;

    auto kvs = make_kvstore();
    kvs->start().get();
    store.write(*kvs, 64 * 1024);
    kvs->stop().get();

    auto snapshot = find_file(std::filesystem::path(test_dir()), "snapshot");
    ASSERT_TRUE(snapshot.has_value());
    {
        // Flip a byte well inside the record payload, away from the framing.
        std::fstream f(
          *snapshot, std::ios::in | std::ios::out | std::ios::binary);
        ASSERT_TRUE(f.is_open());
        f.seekg(0, std::ios::end);
        const auto size = static_cast<std::streamoff>(f.tellg());
        ASSERT_GT(size, 4096);
        f.seekg(size / 2);
        char c = 0;
        f.read(&c, 1);
        c = static_cast<char>(c ^ 0x5a);
        f.seekp(size / 2);
        f.write(&c, 1);
    }

    kvs = make_kvstore();
    EXPECT_ANY_THROW(kvs->recover().get());
    kvs->stop().get();
}

// A large store round-trips through the record-by-record snapshot load
// exactly, across several restarts.
TEST_F(kvstore_test_fixture, snapshot_load_round_trips_many_keys) {
    set_configuration("disable_metrics", true);
    std::map<bytes, iobuf> truth;

    auto kvs = make_kvstore();
    kvs->start().get();
    for (int i = 0; i < 300; i++) {
        auto key = bytes::from_string(fmt::format("key-{}", i));
        auto value = bytes_to_iobuf(
          tests::random_bytes(random_generators::get_int(0, 4096)));
        truth[key] = value.copy();
        kvs->put(key_space::testing, key, std::move(value)).get();
    }
    kvs->stop().get();

    for (int restart = 0; restart < 3; ++restart) {
        kvs = make_kvstore({"dedup.snapshot"});
        kvs->start().get();
        for (const auto& [key, value] : truth) {
            auto got = kvs->get(key_space::testing, key);
            ASSERT_TRUE(got.has_value());
            EXPECT_EQ(*got, value);
        }
        kvs->stop().get();
    }
}
