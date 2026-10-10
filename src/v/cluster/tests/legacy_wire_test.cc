// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

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

#include <gtest/gtest.h>

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
template<typename IcebergMode = int32_t>
struct stock_topic_properties
  : serde::envelope<
      stock_topic_properties<IcebergMode>,
      serde::version<14>,
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
    MEMBER(deprecated_cloud_topic_enabled);
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

    auto serde_fields() {
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
};

iobuf copy(const iobuf& b) { return b.copy(); }

} // namespace

TEST(legacy_wire, topic_properties_layout_matches_stock) {
    topic_properties live;
    live.retention_bytes = tristate<size_t>{1234};
    live.write_caching = model::write_caching_mode::on;
    live.min_compaction_lag_ms = std::chrono::milliseconds{77};

    stock_topic_properties<> stock;
    stock.retention_bytes = live.retention_bytes;
    stock.write_caching = live.write_caching;
    stock.min_compaction_lag_ms = live.min_compaction_lag_ms;

    EXPECT_EQ(serde::to_iobuf(live), serde::to_iobuf(stock));
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
