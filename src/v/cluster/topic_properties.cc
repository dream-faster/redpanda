// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/topic_properties.h"

#include "base/format_to.h"
#include "model/adl_serde.h"
#include "model/metadata.h"
#include "reflection/adl.h"

namespace cluster {
fmt::iterator topic_properties::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ compression: {}, cleanup_policy_bitflags: {}, compaction_strategy: "
      "{}, retention_bytes: {}, retention_duration_ms: {}, segment_size: {}, "
      "timestamp_type: {}batch_max_bytes: {}, "
      "retention_local_target_bytes: {}, retention_local_target_ms: {}, "
      "segment_ms: {}, initial_retention_local_target_bytes: {}, "
      "initial_retention_local_target_ms: "
      "{}mpx_virtual_cluster_id: {}, write_caching: {}, flush_ms: {}, "
      "flush_bytes: {}leaders_preference: {}, delete_retention_ms: "
      "{}min_cleanable_dirty_ratio: {}, min_compaction_lag_ms: {}, "
      "max_compaction_lag_ms: {}, message_timestamp_before_max_ms: {}, "
      "message_timestamp_after_max_ms: {}}}",
      compression,
      cleanup_policy_bitflags,
      compaction_strategy,
      retention_bytes,
      retention_duration,
      segment_size,
      timestamp_type,
      batch_max_bytes,
      retention_local_target_bytes,
      retention_local_target_ms,
      segment_ms,
      initial_retention_local_target_bytes,
      initial_retention_local_target_ms,
      mpx_virtual_cluster_id,
      write_caching,
      flush_ms,
      flush_bytes,
      leaders_preference,
      delete_retention_ms,
      min_cleanable_dirty_ratio,
      min_compaction_lag_ms,
      max_compaction_lag_ms,
      message_timestamp_before_max_ms,
      message_timestamp_after_max_ms);
}

bool topic_properties::is_local_topic() const { return true; }

bool topic_properties::is_compacted() const {
    if (!cleanup_policy_bitflags) {
        return false;
    }
    return (cleanup_policy_bitflags.value()
            & model::cleanup_policy_bitflags::compaction)
           == model::cleanup_policy_bitflags::compaction;
}

bool topic_properties::has_overrides() const {
    const auto overrides = cleanup_policy_bitflags || compaction_strategy
                           || segment_size || retention_bytes.is_engaged()
                           || retention_duration.is_engaged()
                           || batch_max_bytes.has_value()
                           || retention_local_target_bytes.is_engaged()
                           || retention_local_target_ms.is_engaged()
                           || segment_ms.is_engaged()
                           || initial_retention_local_target_bytes.is_engaged()
                           || initial_retention_local_target_ms.is_engaged()
                           || write_caching.has_value() || flush_ms.has_value()
                           || flush_bytes.has_value()
                           || leaders_preference.has_value()
                           || delete_retention_ms.is_engaged()
                           || min_cleanable_dirty_ratio.is_engaged()
                           || min_compaction_lag_ms.has_value()
                           || max_compaction_lag_ms.has_value()
                           || message_timestamp_before_max_ms.has_value()
                           || message_timestamp_after_max_ms.has_value();

    return overrides;
}

storage::ntp_config::default_overrides
topic_properties::get_ntp_cfg_overrides() const {
    storage::ntp_config::default_overrides ret;
    ret.cleanup_policy_bitflags = cleanup_policy_bitflags;
    ret.compaction_strategy = compaction_strategy;
    ret.retention_bytes = retention_bytes;
    ret.retention_time = retention_duration;
    ret.segment_size = segment_size;
    ret.retention_local_target_bytes = retention_local_target_bytes;
    ret.retention_local_target_ms = retention_local_target_ms;
    ret.segment_ms = segment_ms;
    ret.initial_retention_local_target_bytes
      = initial_retention_local_target_bytes;
    ret.initial_retention_local_target_ms = initial_retention_local_target_ms;
    ret.write_caching = write_caching;
    ret.flush_ms = flush_ms;
    ret.flush_bytes = flush_bytes;
    ret.delete_retention_ms = delete_retention_ms;
    ret.min_cleanable_dirty_ratio = min_cleanable_dirty_ratio;
    ret.min_compaction_lag_ms = min_compaction_lag_ms;
    ret.max_compaction_lag_ms = max_compaction_lag_ms;
    return ret;
}

} // namespace cluster
