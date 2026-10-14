/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "storage/ntp_config.h"

#include "config/configuration.h"

namespace storage {

std::optional<size_t> ntp_config::retention_bytes() const {
    if (_overrides) {
        // Handle the special "-1" case.
        if (_overrides->retention_bytes.is_disabled()) {
            return std::nullopt;
        }
        if (_overrides->retention_bytes.has_optional_value()) {
            return _overrides->retention_bytes.value();
        }
        // If no value set, fall through and use the cluster-wide default.
    }
    return config::shard_local_cfg().retention_bytes();
}

std::optional<std::chrono::milliseconds>
ntp_config::retention_duration() const {
    if (_overrides) {
        // Handle the special "-1" case.
        if (_overrides->retention_time.is_disabled()) {
            return std::nullopt;
        }
        if (_overrides->retention_time.has_optional_value()) {
            return _overrides->retention_time.value();
        }
        // If no value set, fall through and use the cluster-wide default.
    }

    return config::shard_local_cfg().log_retention_ms();
}

auto ntp_config::segment_ms() const
  -> std::optional<std::chrono::milliseconds> {
    if (_overrides) {
        if (_overrides->segment_ms.is_disabled()) {
            return std::nullopt;
        }
        if (_overrides->segment_ms.has_optional_value()) {
            return _overrides->segment_ms.value();
        }
        // fall through to server config
    }

    return config::shard_local_cfg().log_segment_ms;
}

bool ntp_config::write_caching() const {
    if (!model::is_user_topic(_ntp)) {
        return false;
    }
    auto cluster_default = config::shard_local_cfg().write_caching_default();
    if (cluster_default == model::write_caching_mode::disabled) {
        return false;
    }
    auto value = _overrides
                   ? _overrides->write_caching.value_or(cluster_default)
                   : cluster_default;
    return value == model::write_caching_mode::default_true;
}

std::chrono::milliseconds ntp_config::flush_ms() const {
    auto cluster_default
      = config::shard_local_cfg().raft_replica_max_flush_delay_ms();
    return _overrides ? _overrides->flush_ms.value_or(cluster_default)
                      : cluster_default;
}

size_t ntp_config::flush_bytes() const {
    const auto& conf
      = config::shard_local_cfg().raft_replica_max_pending_flush_bytes();
    auto cluster_default = conf.value_or(std::numeric_limits<size_t>::max());
    return _overrides ? _overrides->flush_bytes.value_or(cluster_default)
                      : cluster_default;
}

std::optional<std::chrono::milliseconds>
ntp_config::delete_retention_ms() const {
    if (_overrides) {
        // Tombstone (or tx end marker) deletion should not be enabled at
        // the same time as tiered storage.
        // This is because of a race condition:
        // 1) a prefix of a log is uploaded to TS with a value record (or a
        // tx fence batch or a transactional data record)
        // 2) this prefix is truncated away locally
        // 3) correspondent tombstone (or tx end marker) is compacted away
        // locally.
    }
    auto& cluster_default = config::shard_local_cfg().tombstone_retention_ms();
    if (_overrides) {
        // If the tristate is disabled, return nullopt.
        if (_overrides->delete_retention_ms.is_disabled()) {
            return std::nullopt;
        }
        // If the tristate has a value, use it.
        if (_overrides->delete_retention_ms.has_optional_value()) {
            return _overrides->delete_retention_ms.value();
        }

        // If the tristate holds an empty optional, fall back to cluster
        // default.
        return cluster_default;
    }

    // Fall back to cluster default
    return cluster_default;
}

model::cleanup_policy_bitflags ntp_config::cleanup_policy() const {
    const auto& cluster_default
      = config::shard_local_cfg().log_cleanup_policy();
    return cleanup_policy_override().value_or(cluster_default);
}

std::optional<double> ntp_config::min_cleanable_dirty_ratio() const {
    if (_overrides) {
        if (_overrides->min_cleanable_dirty_ratio.is_disabled()) {
            return std::nullopt;
        }
        if (_overrides->min_cleanable_dirty_ratio.has_optional_value()) {
            return _overrides->min_cleanable_dirty_ratio.value();
        }
    }
    return config::shard_local_cfg().min_cleanable_dirty_ratio();
}

std::chrono::milliseconds ntp_config::min_compaction_lag_ms() const {
    if (_overrides && _overrides->min_compaction_lag_ms.has_value()) {
        return _overrides->min_compaction_lag_ms.value();
    }
    return config::shard_local_cfg().min_compaction_lag_ms();
}

std::chrono::milliseconds ntp_config::max_compaction_lag_ms() const {
    if (_overrides && _overrides->max_compaction_lag_ms.has_value()) {
        return _overrides->max_compaction_lag_ms.value();
    }
    return config::shard_local_cfg().max_compaction_lag_ms();
}

} // namespace storage
