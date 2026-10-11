// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "base/format_to.h"
#include "cluster/legacy_wire.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "reflection/adl.h"
#include "serde/rw/chrono.h"
#include "serde/rw/envelope.h"
#include "serde/rw/optional.h"
#include "serde/rw/scalar.h"
#include "serde/rw/tristate_rw.h"
#include "storage/ntp_config.h"
#include "utils/tristate.h"

namespace cluster {

/**
 * Structure holding topic properties overrides, empty values will be replaced
 * with defaults
 */
struct topic_properties
  : serde::
      envelope<topic_properties, serde::version<14>, serde::compat_version<0>> {
    topic_properties() noexcept = default;
    topic_properties(
      std::optional<model::compression> compression,
      std::optional<model::cleanup_policy_bitflags> cleanup_policy_bitflags,
      std::optional<model::compaction_strategy> compaction_strategy,
      std::optional<model::timestamp_type> timestamp_type,
      std::optional<size_t> segment_size,
      tristate<size_t> retention_bytes,
      tristate<std::chrono::milliseconds> retention_duration,
      std::optional<uint32_t> batch_max_bytes,
      tristate<size_t> retention_local_target_bytes,
      tristate<std::chrono::milliseconds> retention_local_target_ms,
      tristate<std::chrono::milliseconds> segment_ms,
      tristate<size_t> initial_retention_local_target_bytes,
      tristate<std::chrono::milliseconds> initial_retention_local_target_ms,
      std::optional<model::vcluster_id> mpx_virtual_cluster_id,
      std::optional<model::write_caching_mode> write_caching,
      std::optional<std::chrono::milliseconds> flush_ms,
      std::optional<size_t> flush_bytes,
      std::optional<config::leaders_preference> leaders_preference,
      tristate<std::chrono::milliseconds> delete_retention_ms,
      tristate<double> min_cleanable_dirty_ratio,
      std::optional<std::chrono::milliseconds> min_compaction_lag_ms,
      std::optional<std::chrono::milliseconds> max_compaction_lag_ms,
      std::optional<std::chrono::milliseconds> message_timestamp_before_max_ms,
      std::optional<std::chrono::milliseconds> message_timestamp_after_max_ms)
      : compression(compression)
      , cleanup_policy_bitflags(cleanup_policy_bitflags)
      , compaction_strategy(compaction_strategy)
      , timestamp_type(timestamp_type)
      , segment_size(segment_size)
      , retention_bytes(retention_bytes)
      , retention_duration(retention_duration)
      , batch_max_bytes(batch_max_bytes)
      , retention_local_target_bytes(retention_local_target_bytes)
      , retention_local_target_ms(retention_local_target_ms)
      , segment_ms(segment_ms)
      , initial_retention_local_target_bytes(
          initial_retention_local_target_bytes)
      , initial_retention_local_target_ms(initial_retention_local_target_ms)
      , mpx_virtual_cluster_id(mpx_virtual_cluster_id)
      , write_caching(write_caching)
      , flush_ms(flush_ms)
      , flush_bytes(flush_bytes)
      , leaders_preference(std::move(leaders_preference))
      , delete_retention_ms(delete_retention_ms)
      , min_cleanable_dirty_ratio(min_cleanable_dirty_ratio)
      , min_compaction_lag_ms(min_compaction_lag_ms)
      , max_compaction_lag_ms(max_compaction_lag_ms)
      , message_timestamp_before_max_ms(message_timestamp_before_max_ms)
      , message_timestamp_after_max_ms(message_timestamp_after_max_ms) {}

    std::optional<model::compression> compression;
    std::optional<model::cleanup_policy_bitflags> cleanup_policy_bitflags;
    std::optional<model::compaction_strategy> compaction_strategy;
    std::optional<model::timestamp_type> timestamp_type;
    std::optional<size_t> segment_size;
    tristate<size_t> retention_bytes{std::nullopt};
    tristate<std::chrono::milliseconds> retention_duration{std::nullopt};

    std::optional<uint32_t> batch_max_bytes;

    tristate<size_t> retention_local_target_bytes{std::nullopt};
    tristate<std::chrono::milliseconds> retention_local_target_ms{std::nullopt};

    tristate<std::chrono::milliseconds> segment_ms{std::nullopt};

    // Local retention to apply while a partition is still catching up: it is
    // relaxed to the values above once the replica is in sync.
    tristate<size_t> initial_retention_local_target_bytes{std::nullopt};
    tristate<std::chrono::milliseconds> initial_retention_local_target_ms{
      std::nullopt};
    std::optional<model::vcluster_id> mpx_virtual_cluster_id;
    std::optional<model::write_caching_mode> write_caching;
    std::optional<std::chrono::milliseconds> flush_ms;
    std::optional<size_t> flush_bytes;

    std::optional<config::leaders_preference> leaders_preference;

    tristate<std::chrono::milliseconds> delete_retention_ms{disable_tristate};

    tristate<double> min_cleanable_dirty_ratio{std::nullopt};
    std::optional<std::chrono::milliseconds> min_compaction_lag_ms{};
    std::optional<std::chrono::milliseconds> max_compaction_lag_ms{};

    std::optional<std::chrono::milliseconds> message_timestamp_before_max_ms{};
    std::optional<std::chrono::milliseconds> message_timestamp_after_max_ms{};

    // Slots of removed features (tiered storage, iceberg, schema id
    // validation). They only exist to keep the serde layout identical to
    // stock v26.2.x, see legacy_wire.h. Nothing reads them.
    std::optional<bool> legacy_recovery;
    std::optional<legacy::enum_wire> legacy_shadow_indexing;
    std::optional<bool> legacy_read_replica;
    std::optional<ss::sstring> legacy_read_replica_bucket;
    std::optional<legacy::remote_topic_properties_wire>
      legacy_remote_topic_properties;
    bool legacy_remote_delete{true};
    std::optional<bool> legacy_record_key_schema_id_validation;
    std::optional<bool> legacy_record_key_schema_id_validation_compat;
    std::optional<legacy::enum_wire> legacy_record_key_subject_name_strategy;
    std::optional<legacy::enum_wire>
      legacy_record_key_subject_name_strategy_compat;
    std::optional<bool> legacy_record_value_schema_id_validation;
    std::optional<bool> legacy_record_value_schema_id_validation_compat;
    std::optional<legacy::enum_wire> legacy_record_value_subject_name_strategy;
    std::optional<legacy::enum_wire>
      legacy_record_value_subject_name_strategy_compat;
    std::optional<legacy::remote_label_wire> legacy_remote_label;
    std::optional<model::topic_namespace>
      legacy_remote_topic_namespace_override;
    legacy::iceberg_mode_wire legacy_iceberg_mode;
    std::optional<bool> legacy_iceberg_delete;
    std::optional<ss::sstring> legacy_iceberg_partition_spec;
    std::optional<legacy::enum_wire> legacy_iceberg_invalid_record_action;
    std::optional<std::chrono::milliseconds> legacy_iceberg_target_lag_ms;
    std::optional<bool> legacy_remote_topic_allow_gaps;
    // redpanda_storage_mode::unset
    legacy::enum_wire legacy_storage_mode{255};
    std::optional<ss::sstring> legacy_schema_registry_context;

    bool is_local_topic() const;

    bool is_compacted() const;
    bool has_overrides() const;
    storage::ntp_config::default_overrides get_ntp_cfg_overrides() const;

    fmt::iterator format_to(fmt::iterator it) const;
    auto serde_fields() {
        return std::tie(
          compression,
          cleanup_policy_bitflags,
          compaction_strategy,
          timestamp_type,
          segment_size,
          retention_bytes,
          retention_duration,
          legacy_recovery,
          legacy_shadow_indexing,
          legacy_read_replica,
          legacy_read_replica_bucket,
          legacy_remote_topic_properties,
          batch_max_bytes,
          retention_local_target_bytes,
          retention_local_target_ms,
          legacy_remote_delete,
          segment_ms,
          legacy_record_key_schema_id_validation,
          legacy_record_key_schema_id_validation_compat,
          legacy_record_key_subject_name_strategy,
          legacy_record_key_subject_name_strategy_compat,
          legacy_record_value_schema_id_validation,
          legacy_record_value_schema_id_validation_compat,
          legacy_record_value_subject_name_strategy,
          legacy_record_value_subject_name_strategy_compat,
          initial_retention_local_target_bytes,
          initial_retention_local_target_ms,
          mpx_virtual_cluster_id,
          write_caching,
          flush_ms,
          flush_bytes,
          legacy_remote_label,
          legacy_remote_topic_namespace_override,
          legacy_iceberg_mode,
          leaders_preference,
          deprecated_cloud_topic_enabled,
          delete_retention_ms,
          legacy_iceberg_delete,
          legacy_iceberg_partition_spec,
          legacy_iceberg_invalid_record_action,
          legacy_iceberg_target_lag_ms,
          min_cleanable_dirty_ratio,
          legacy_remote_topic_allow_gaps,
          min_compaction_lag_ms,
          max_compaction_lag_ms,
          message_timestamp_before_max_ms,
          message_timestamp_after_max_ms,
          legacy_storage_mode,
          legacy_schema_registry_context);
    }

    friend bool
    operator==(const topic_properties&, const topic_properties&) = default;

private:
    // This was deprecated in favour of redpanda.storage.mode, but is kept here
    // for backwards compatible serde purposes.
    bool deprecated_cloud_topic_enabled{false};
};

} // namespace cluster
