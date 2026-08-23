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

#pragma once
#include "base/format_to.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "ssx/sformat.h"
#include "utils/tristate.h"

#include <seastar/core/sstring.hh>

#include <optional>

namespace storage {
using with_cache = ss::bool_class<struct log_cache_tag>;
class ntp_config {
public:
    // Remote deletes are enabled by default in new tiered storage topics,
    // disabled by default in legacy topics during upgrade (the legacy path
    // is handled during adl/serde decode).

    struct default_overrides {
        // if not set use the log_manager's configuration
        std::optional<model::cleanup_policy_bitflags> cleanup_policy_bitflags;
        // if not set use the log_manager's configuration
        std::optional<model::compaction_strategy> compaction_strategy;
        // if not set, use the log_manager's configuration
        std::optional<size_t> segment_size;

        // partition retention settings. If tristate is disabled the feature
        // will be disabled if there is no value set the default will be used
        tristate<size_t> retention_bytes{std::nullopt};
        tristate<std::chrono::milliseconds> retention_time{std::nullopt};
        // if set, log will not use batch cache
        with_cache cache_enabled = with_cache::yes;
        tristate<size_t> retention_local_target_bytes{std::nullopt};
        tristate<std::chrono::milliseconds> retention_local_target_ms{
          std::nullopt};

        // time before rolling a segment, from first write
        tristate<std::chrono::milliseconds> segment_ms{std::nullopt};

        tristate<size_t> initial_retention_local_target_bytes{std::nullopt};
        tristate<std::chrono::milliseconds> initial_retention_local_target_ms{
          std::nullopt};

        std::optional<model::write_caching_mode> write_caching;

        std::optional<std::chrono::milliseconds> flush_ms;
        std::optional<size_t> flush_bytes;

        tristate<std::chrono::milliseconds> delete_retention_ms;

        // Controls segment compaction eligiblity.
        tristate<double> min_cleanable_dirty_ratio;
        std::optional<std::chrono::milliseconds> min_compaction_lag_ms;
        std::optional<std::chrono::milliseconds> max_compaction_lag_ms;

        // Controls behavior during pause
        // Storage mode for the topic (local, tiered, or cloud)

        fmt::iterator format_to(fmt::iterator it) const;
    };

    ntp_config(model::ntp n, ss::sstring base_dir) noexcept
      : _ntp(std::move(n))
      , _base_dir(std::move(base_dir)) {}

    ntp_config(
      model::ntp n,
      ss::sstring base_dir,
      std::unique_ptr<default_overrides> overrides) noexcept
      : ntp_config(
          std::move(n),
          std::move(base_dir),
          std::move(overrides),
          model::revision_id(0)) {}

    ntp_config(
      model::ntp n,
      ss::sstring base_dir,
      std::unique_ptr<default_overrides> overrides,
      model::revision_id id) noexcept
      : _ntp(std::move(n))
      , _base_dir(std::move(base_dir))
      , _overrides(std::move(overrides))
      , _revision_id(id) {}

    ntp_config(
      model::ntp n,
      ss::sstring base_dir,
      std::unique_ptr<default_overrides> overrides,
      model::revision_id rev,
      model::revision_id topic_rev,
      model::initial_revision_id remote_rev) noexcept
      : _ntp(std::move(n))
      , _base_dir(std::move(base_dir))
      , _overrides(std::move(overrides))
      , _revision_id(rev)
      , _topic_rev(topic_rev)
      , _remote_rev(remote_rev) {}

    const model::ntp& ntp() const { return _ntp; }
    model::ntp& ntp() { return _ntp; }

    model::revision_id get_revision() const { return _revision_id; }

    model::revision_id get_topic_revision() const { return _topic_rev; }

    model::initial_revision_id get_remote_revision() const {
        return _remote_rev;
    }

    const ss::sstring& base_directory() const { return _base_dir; }
    ss::sstring& base_directory() { return _base_dir; }

    const default_overrides& get_overrides() const { return *_overrides; }

    default_overrides& get_overrides() { return *_overrides; }

    bool has_overrides() const { return _overrides != nullptr; }

    // If compaction is enabled for local storage.
    bool is_locally_compacted() const {
        return model::is_compaction_enabled(cleanup_policy());
    }

    // If time/bytes based retention is enabled for local storage.
    bool is_locally_collectable() const {
        return model::is_deletion_enabled(cleanup_policy());
    }

    // If time/bytes based retention is enabled for remote storage.
    bool is_remotely_collectable() const {
        return model::is_deletion_enabled(cleanup_policy());
    }

    ss::sstring work_directory() const {
        return ssx::sformat("{}/{}_{}", _base_dir, _ntp.path(), _revision_id);
    }

    std::filesystem::path topic_directory() const {
        return std::filesystem::path(_base_dir) / _ntp.topic_path();
    }

    with_cache cache_enabled() const {
        return with_cache(!has_overrides() || _overrides->cache_enabled);
    }

    void set_overrides(default_overrides o) {
        _overrides = std::make_unique<default_overrides>(o);
    }

    std::optional<size_t> retention_bytes() const {
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

    std::optional<std::chrono::milliseconds> retention_duration() const {
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

    auto segment_ms() const -> std::optional<std::chrono::milliseconds> {
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

    bool write_caching() const {
        if (!model::is_user_topic(_ntp)) {
            return false;
        }
        auto cluster_default
          = config::shard_local_cfg().write_caching_default();
        if (cluster_default == model::write_caching_mode::disabled) {
            return false;
        }
        auto value = _overrides
                       ? _overrides->write_caching.value_or(cluster_default)
                       : cluster_default;
        return value == model::write_caching_mode::default_true;
    }

    std::chrono::milliseconds flush_ms() const {
        auto cluster_default
          = config::shard_local_cfg().raft_replica_max_flush_delay_ms();
        return _overrides ? _overrides->flush_ms.value_or(cluster_default)
                          : cluster_default;
    }

    size_t flush_bytes() const {
        const auto& conf
          = config::shard_local_cfg().raft_replica_max_pending_flush_bytes();
        auto cluster_default = conf.value_or(
          std::numeric_limits<size_t>::max());
        return _overrides ? _overrides->flush_bytes.value_or(cluster_default)
                          : cluster_default;
    }

    std::optional<std::chrono::milliseconds> delete_retention_ms() const {
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
        auto& cluster_default
          = config::shard_local_cfg().tombstone_retention_ms();
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

    std::optional<model::cleanup_policy_bitflags>
    cleanup_policy_override() const {
        return _overrides ? _overrides->cleanup_policy_bitflags : std::nullopt;
    }

    model::cleanup_policy_bitflags cleanup_policy() const {
        const auto& cluster_default
          = config::shard_local_cfg().log_cleanup_policy();
        return cleanup_policy_override().value_or(cluster_default);
    }

    std::optional<double> min_cleanable_dirty_ratio() const {
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

    std::chrono::milliseconds min_compaction_lag_ms() const {
        if (_overrides && _overrides->min_compaction_lag_ms.has_value()) {
            return _overrides->min_compaction_lag_ms.value();
        }
        return config::shard_local_cfg().min_compaction_lag_ms();
    }

    std::chrono::milliseconds max_compaction_lag_ms() const {
        if (_overrides && _overrides->max_compaction_lag_ms.has_value()) {
            return _overrides->max_compaction_lag_ms.value();
        }
        return config::shard_local_cfg().max_compaction_lag_ms();
    }

    ntp_config copy() const {
        return {
          _ntp,
          _base_dir,
          _overrides ? std::make_unique<default_overrides>(*_overrides)
                     : nullptr,
          _revision_id,
          _topic_rev,
          _remote_rev};
    }

private:
    model::ntp _ntp;
    /// \brief currently this is the basedir. In the future
    /// this will be used to load balance on devices so that there is no
    /// implicit hierarchy, simply directories with data
    ss::sstring _base_dir;

    std::unique_ptr<default_overrides> _overrides;

    /// Revision of the command that resulted in this partition appearing on
    /// this node (i.e. it will changed if the partition replica is moved back
    /// and forth from/to the node, as well as if the topic is re-created). It
    /// is used in constructing the local directory path.
    model::revision_id _revision_id{0};

    /// Revision of the topic creation command.
    model::revision_id _topic_rev;

    /// This revision is used to construct cloud storage paths. It differs from
    /// _topic_revision in case of recovered topics or read replicas.
    model::initial_revision_id _remote_rev{0};

public:
    // in storage/types.cc
    fmt::iterator format_to(fmt::iterator it) const;
};

} // namespace storage
