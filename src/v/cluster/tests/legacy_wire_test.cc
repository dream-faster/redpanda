// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "cluster/legacy_wire.h"
#include "cluster/topic_properties.h"
#include "cluster/types.h"
#include "serde/rw/chrono.h"
#include "serde/rw/envelope.h"
#include "serde/rw/optional.h"
#include "serde/rw/rw.h"
#include "serde/rw/scalar.h"
#include "serde/rw/sstring.h"
#include "serde/rw/tristate_rw.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <tuple>

namespace cluster {

namespace {

// iceberg_mode::value_schema_latest, discriminant 3 in v26.2.x.
struct iceberg_latest {
    ss::sstring protobuf_name;
    ss::sstring subject;
};

void tag_invoke(serde::tag_t<serde::write_tag>, iobuf& out, iceberg_latest v) {
    serde::write(out, int32_t{3});
    serde::write(out, v.protobuf_name);
    serde::write(out, v.subject);
}

template<typename T>
using member_t = std::remove_cvref_t<T>;

#define MEMBER(name) member_t<decltype(topic_properties::name)> name

// The field layout of topic_properties in stock v26.2.x. Fields that still
// exist are typed after the live struct; removed ones are spelled out.
//
// WithDedup models the predecessor this branch actually upgrades from: a stock
// v26.2.x broker already carrying the three dedup properties this PR appends
// (version 17). WithDedup=false is plain stock v26.2.x, which an upgrading
// node must still be able to read.
template<typename IcebergMode = int32_t, bool WithDedup = false>
struct stock_topic_properties
  : serde::envelope<
      stock_topic_properties<IcebergMode, WithDedup>,
      serde::version<WithDedup ? 17 : 14>,
      serde::compat_version<0>> {
    MEMBER(compression);
    MEMBER(cleanup_policy_bitflags);
    MEMBER(compaction_strategy);
    MEMBER(timestamp_type);
    MEMBER(segment_size);
    MEMBER(retention_bytes);
    MEMBER(retention_duration);
    std::optional<bool> recovery;
    std::optional<int32_t> shadow_indexing;
    std::optional<bool> read_replica;
    std::optional<ss::sstring> read_replica_bucket;
    std::optional<legacy::remote_topic_properties_wire> remote_topic_properties;
    MEMBER(batch_max_bytes);
    MEMBER(retention_local_target_bytes);
    MEMBER(retention_local_target_ms);
    bool remote_delete{true};
    MEMBER(segment_ms);
    std::optional<bool> record_key_schema_id_validation;
    std::optional<bool> record_key_schema_id_validation_compat;
    std::optional<int32_t> record_key_subject_name_strategy;
    std::optional<int32_t> record_key_subject_name_strategy_compat;
    std::optional<bool> record_value_schema_id_validation;
    std::optional<bool> record_value_schema_id_validation_compat;
    std::optional<int32_t> record_value_subject_name_strategy;
    std::optional<int32_t> record_value_subject_name_strategy_compat;
    MEMBER(initial_retention_local_target_bytes);
    MEMBER(initial_retention_local_target_ms);
    MEMBER(mpx_virtual_cluster_id);
    MEMBER(write_caching);
    MEMBER(flush_ms);
    MEMBER(flush_bytes);
    std::optional<legacy::remote_label_wire> remote_label;
    std::optional<model::topic_namespace> remote_topic_namespace_override;
    IcebergMode iceberg_mode{};
    MEMBER(leaders_preference);
    bool deprecated_cloud_topic_enabled{false};
    MEMBER(delete_retention_ms);
    std::optional<bool> iceberg_delete;
    std::optional<ss::sstring> iceberg_partition_spec;
    std::optional<int32_t> iceberg_invalid_record_action;
    std::optional<std::chrono::milliseconds> iceberg_target_lag_ms;
    MEMBER(min_cleanable_dirty_ratio);
    std::optional<bool> remote_topic_allow_gaps;
    MEMBER(min_compaction_lag_ms);
    MEMBER(max_compaction_lag_ms);
    MEMBER(message_timestamp_before_max_ms);
    MEMBER(message_timestamp_after_max_ms);
    int32_t storage_mode{255};
    std::optional<ss::sstring> schema_registry_context;
    MEMBER(dedup_window_ms);
    MEMBER(dedup_generation);
    MEMBER(dedup_key_header);

    template<typename T>
    void take_shared_fields_from(const T& live) {
        dedup_window_ms = live.dedup_window_ms;
        dedup_generation = live.dedup_generation;
        dedup_key_header = live.dedup_key_header;
        compression = live.compression;
        cleanup_policy_bitflags = live.cleanup_policy_bitflags;
        compaction_strategy = live.compaction_strategy;
        timestamp_type = live.timestamp_type;
        segment_size = live.segment_size;
        retention_bytes = live.retention_bytes;
        retention_duration = live.retention_duration;
        batch_max_bytes = live.batch_max_bytes;
        retention_local_target_bytes = live.retention_local_target_bytes;
        retention_local_target_ms = live.retention_local_target_ms;
        segment_ms = live.segment_ms;
        initial_retention_local_target_bytes
          = live.initial_retention_local_target_bytes;
        initial_retention_local_target_ms
          = live.initial_retention_local_target_ms;
        mpx_virtual_cluster_id = live.mpx_virtual_cluster_id;
        write_caching = live.write_caching;
        flush_ms = live.flush_ms;
        flush_bytes = live.flush_bytes;
        leaders_preference = live.leaders_preference;
        delete_retention_ms = live.delete_retention_ms;
        min_cleanable_dirty_ratio = live.min_cleanable_dirty_ratio;
        min_compaction_lag_ms = live.min_compaction_lag_ms;
        max_compaction_lag_ms = live.max_compaction_lag_ms;
        message_timestamp_before_max_ms = live.message_timestamp_before_max_ms;
        message_timestamp_after_max_ms = live.message_timestamp_after_max_ms;
    }

    auto stock_fields() {
        return std::tie(
          compression,
          cleanup_policy_bitflags,
          compaction_strategy,
          timestamp_type,
          segment_size,
          retention_bytes,
          retention_duration,
          recovery,
          shadow_indexing,
          read_replica,
          read_replica_bucket,
          remote_topic_properties,
          batch_max_bytes,
          retention_local_target_bytes,
          retention_local_target_ms,
          remote_delete,
          segment_ms,
          record_key_schema_id_validation,
          record_key_schema_id_validation_compat,
          record_key_subject_name_strategy,
          record_key_subject_name_strategy_compat,
          record_value_schema_id_validation,
          record_value_schema_id_validation_compat,
          record_value_subject_name_strategy,
          record_value_subject_name_strategy_compat,
          initial_retention_local_target_bytes,
          initial_retention_local_target_ms,
          mpx_virtual_cluster_id,
          write_caching,
          flush_ms,
          flush_bytes,
          remote_label,
          remote_topic_namespace_override,
          iceberg_mode,
          leaders_preference,
          deprecated_cloud_topic_enabled,
          delete_retention_ms,
          iceberg_delete,
          iceberg_partition_spec,
          iceberg_invalid_record_action,
          iceberg_target_lag_ms,
          min_cleanable_dirty_ratio,
          remote_topic_allow_gaps,
          min_compaction_lag_ms,
          max_compaction_lag_ms,
          message_timestamp_before_max_ms,
          message_timestamp_after_max_ms,
          storage_mode,
          schema_registry_context);
    }

    auto serde_fields() {
        if constexpr (WithDedup) {
            return std::tuple_cat(
              stock_fields(),
              std::tie(dedup_window_ms, dedup_generation, dedup_key_header));
        } else {
            return stock_fields();
        }
    }
};

std::string hex(iobuf b) {
    std::string out;
    for (auto c : iobuf_to_bytes(b)) {
        out += fmt::format("{:02x}", static_cast<uint8_t>(c));
    }
    return out;
}

} // namespace

// The in-place upgrade this branch has to survive: the node being replaced is
// a stock v26.2.x broker already running dedup, so its topic_properties bytes
// must be the ones a slim+dedup node writes -- same slots, same order, same
// envelope version. The dedup fields sit after the legacy slots precisely so
// this stays byte-for-byte true.
TEST(legacy_wire, topic_properties_layout_matches_stock_with_dedup) {
    topic_properties live;
    live.retention_bytes = tristate<size_t>{1234};
    live.write_caching = model::write_caching_mode::default_false;
    live.min_compaction_lag_ms = std::chrono::milliseconds{77};
    live.dedup_window_ms = tristate<std::chrono::milliseconds>{
      std::chrono::milliseconds{300000}};
    live.dedup_generation = 4;
    live.dedup_key_header = "redpanda-dedup-key";

    stock_topic_properties<int32_t, /*WithDedup=*/true> stock;
    stock.take_shared_fields_from(live);

    EXPECT_EQ(hex(serde::to_iobuf(live)), hex(serde::to_iobuf(stock)));
}

// The other direction, and the other predecessor: a plain stock v26.2.x
// broker that never had dedup. Its shorter envelope still parses, leaving the
// dedup properties at their defaults -- serde stops at the envelope's own
// size, and compat_version 0 means the version gap is not an error.
TEST(legacy_wire, topic_properties_reads_stock_without_dedup) {
    stock_topic_properties<> stock;
    stock.retention_bytes = tristate<size_t>{1234};
    stock.min_compaction_lag_ms = std::chrono::milliseconds{77};

    auto parsed = serde::from_iobuf<topic_properties>(serde::to_iobuf(stock));

    EXPECT_EQ(parsed.retention_bytes, stock.retention_bytes);
    EXPECT_EQ(parsed.min_compaction_lag_ms, stock.min_compaction_lag_ms);
    EXPECT_TRUE(parsed.dedup_window_ms.is_empty());
    EXPECT_EQ(parsed.dedup_generation, 0);
    EXPECT_FALSE(parsed.dedup_key_header.has_value());
}

// And a dedup-carrying predecessor's values survive the parse, which is what
// makes the upgrade non-disruptive: the restarted node keeps deduplicating
// the same topics with the same identity source and generation.
TEST(legacy_wire, topic_properties_reads_stock_dedup_topic) {
    stock_topic_properties<int32_t, /*WithDedup=*/true> stock;
    stock.dedup_window_ms = tristate<std::chrono::milliseconds>{
      std::chrono::milliseconds{300000}};
    stock.dedup_generation = 7;
    stock.dedup_key_header = "redpanda-dedup-key";

    auto parsed = serde::from_iobuf<topic_properties>(serde::to_iobuf(stock));

    EXPECT_EQ(parsed.dedup_window_ms, stock.dedup_window_ms);
    EXPECT_EQ(parsed.dedup_generation, 7);
    EXPECT_EQ(parsed.dedup_key_header, stock.dedup_key_header);
}

TEST(legacy_wire, topic_properties_reads_stock_tiered_and_iceberg_topic) {
    stock_topic_properties<iceberg_latest> stock;
    stock.retention_duration = tristate<std::chrono::milliseconds>{
      std::chrono::milliseconds{5000}};
    stock.shadow_indexing = 3;
    stock.remote_delete = false;
    stock.read_replica = true;
    stock.read_replica_bucket = "bucket";
    stock.remote_topic_properties = legacy::remote_topic_properties_wire{};
    stock.remote_label = legacy::remote_label_wire{};
    stock.record_key_subject_name_strategy = 2;
    stock.iceberg_mode = iceberg_latest{"proto.Name", "subject"};
    stock.iceberg_partition_spec = "hour(redpanda.timestamp)";
    stock.storage_mode = 1;
    stock.schema_registry_context = ".ctx";
    stock.message_timestamp_after_max_ms = std::chrono::milliseconds{9};

    auto parsed = serde::from_iobuf<topic_properties>(serde::to_iobuf(stock));

    EXPECT_EQ(parsed.retention_duration, stock.retention_duration);
    EXPECT_EQ(
      parsed.message_timestamp_after_max_ms,
      stock.message_timestamp_after_max_ms);
    EXPECT_EQ(parsed.legacy_storage_mode.value, 1);
    EXPECT_EQ(parsed.legacy_schema_registry_context, ".ctx");

    // What a slim node forwards to a stock peer reads back as the same
    // retention and a locally-stored topic.
    auto resent = serde::from_iobuf<stock_topic_properties<>>(
      serde::to_iobuf(parsed));
    EXPECT_EQ(resent.retention_duration, stock.retention_duration);
    EXPECT_EQ(
      resent.message_timestamp_after_max_ms,
      stock.message_timestamp_after_max_ms);
    EXPECT_EQ(resent.storage_mode, 1);
}

TEST(legacy_wire, topic_properties_round_trip) {
    topic_properties p;
    p.segment_ms = tristate<std::chrono::milliseconds>{
      std::chrono::milliseconds{10}};
    auto copy_p = serde::from_iobuf<topic_properties>(serde::to_iobuf(p));
    EXPECT_EQ(p, copy_p);
}

} // namespace cluster
