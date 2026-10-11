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

#include "absl/container/flat_hash_map.h"
#include "base/format_to.h"
#include "cluster/fwd.h"
#include "cluster/ntp_callbacks.h"
#include "cluster/partition.h"
#include "cluster/state_machine_registry.h"
#include "cluster/types.h"
#include "config/property.h"
#include "container/chunked_hash_map.h"
#include "container/intrusive_list_helpers.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/ktp.h"
#include "model/metadata.h"
#include "raft/group_manager.h"
#include "storage/api.h"

#include <chrono>

namespace cluster {
class partition_manager
  : public ss::peering_sharded_service<partition_manager> {
public:
    using ntp_table_container
      = model::ntp_map_type<ss::lw_shared_ptr<partition>>;

    partition_manager(
      ss::sharded<storage::api>&,
      ss::sharded<raft::group_manager>&,
      ss::sharded<features::feature_table>&,
      config::binding<std::chrono::milliseconds>);

    ~partition_manager();

    using manage_cb_t
      = ss::noncopyable_function<void(const ss::lw_shared_ptr<partition>&)>;
    using unmanage_cb_t
      = ss::noncopyable_function<void(model::topic_partition_view)>;

    /// \brief Copies table with ntps matching a given topic namespace
    ntp_table_container
      get_topic_partition_table(model::topic_namespace_view) const;

    inline ss::lw_shared_ptr<partition>
    get(const model::any_ntp auto& ntp) const {
        if (auto it = _ntp_table.find(ntp); it != _ntp_table.end()) {
            return it->second;
        }
        return nullptr;
    }

    /// \brief raw api for raft/service.h
    inline consensus_ptr consensus_for(raft::group_id group) const {
        if (auto it = _raft_table.find(group); it != _raft_table.end()) {
            return it->second->raft();
        }
        return nullptr;
    }

    inline ss::lw_shared_ptr<partition>
    partition_for(raft::group_id group) const {
        if (auto it = _raft_table.find(group); it != _raft_table.end()) {
            return it->second;
        }
        return nullptr;
    }

    ss::future<> start();
    ss::future<> stop_partitions();
    ss::future<consensus_ptr> manage(
      storage::ntp_config,
      raft::group_id,
      std::vector<raft::vnode>,
      raft::with_learner_recovery_throttle,
      raft::keep_snapshotted_log,
      std::optional<xshard_transfer_state>,
      const topic_configuration* = nullptr,
      std::optional<partition_bootstrap_params> = std::nullopt);

    ss::future<xshard_transfer_state> shutdown(const model::ntp& ntp);

    ss::future<> remove(const model::ntp& ntp, partition_removal_mode mode);

    /*
     * register for notification of new partitions within the specific topic
     * being managed. this will invoke the callback for existing partitions
     * synchronously so the caller must be prepared for that.
     *
     * the callback must not block.
     */
    notification_id_type register_manage_notification(
      const model::ns& ns, const model::topic& topic, manage_cb_t cb) {
        /*
         * first create a cb filter and apply all existing partitions. this
         * ensures that the caller initializes its state with existing
         * partitions.
         */
        ntp_callbacks<manage_cb_t> init;
        init.register_notify(
          ns, topic, [&cb](const ss::lw_shared_ptr<partition>& p) { cb(p); });
        for (auto& e : _ntp_table) {
            if (e.second->started()) {
                init.notify(e.first, e.second);
            }
        }

        // now setup the permenant callback for new partitions
        return _manage_watchers.register_notify(ns, topic, std::move(cb));
    }
    notification_id_type
    register_manage_notification(const model::ns& ns, manage_cb_t cb) {
        ntp_callbacks<manage_cb_t> init;
        init.register_notify(
          ns, [&cb](const ss::lw_shared_ptr<partition>& p) { cb(p); });
        for (auto& e : _ntp_table) {
            if (e.second->started()) {
                init.notify(e.first, e.second);
            }
        }
        return _manage_watchers.register_notify(ns, std::move(cb));
    }

    /*
     * register for notification of partitions within the specific topic
     * being removed from manager. the callback must not block.
     */
    notification_id_type register_unmanage_notification(
      const model::ns& ns, const model::topic& topic, unmanage_cb_t cb) {
        return _unmanage_watchers.register_notify(ns, topic, std::move(cb));
    }
    notification_id_type
    register_unmanage_notification(const model::ns& ns, unmanage_cb_t cb) {
        return _unmanage_watchers.register_notify(ns, std::move(cb));
    }

    void unregister_manage_notification(notification_id_type id) {
        _manage_watchers.unregister_notify(id);
    }
    void unregister_unmanage_notification(notification_id_type id) {
        _unmanage_watchers.unregister_notify(id);
    }

    /*
     * read-only interface to partitions.
     *
     * note that users of this interface must take care not to hold iterators
     * across scheduling events as the underlying table may be modified and
     * invalidate iterators.
     */
    const ntp_table_container& partitions() const { return _ntp_table; }

    /*
     * Block/unblock current node from leadership for new and existing raft
     * groups.
     */
    void block_new_leadership() {
        _block_new_leadership = true;
        for (const auto& p : _ntp_table) {
            p.second->block_new_leadership();
        }
    }

    void unblock_new_leadership() {
        _block_new_leadership = false;
        for (const auto& p : _ntp_table) {
            p.second->unblock_new_leadership();
        }
    }

    /// Report the aggregate backlog of all archivers for all managed
    /// partitions

    /*
     * Return disk space usage for for partitions not accounted for by the
     * underlying logs. Examples include raft snapshots, and other snapshots and
     * indicies used by any active state machines.
     *
     * The results are accumulated for all partitions, across all cores.
     */
    ss::future<size_t> non_log_disk_size_bytes() const;

    template<typename T, typename... Args>
    void register_factory(Args&&... args) {
        _stm_registry.register_factory<T>(std::forward<Args>(args)...);
    }

    fmt::iterator format_to(fmt::iterator it) const;

private:
    enum class partition_shutdown_stage {
        shutdown_requested,
        stopping_raft,
        removing_raft,
        stopping_partition,
        removing_persistent_state,
        stopping_storage,
        removing_storage
    };

    struct partition_shutdown_state {
        explicit partition_shutdown_state(ss::lw_shared_ptr<partition>);

        partition_shutdown_state(partition_shutdown_state&&) = delete;
        partition_shutdown_state(const partition_shutdown_state&) = delete;
        partition_shutdown_state&
        operator=(partition_shutdown_state&&) = delete;
        partition_shutdown_state&
        operator=(const partition_shutdown_state&) = delete;
        ~partition_shutdown_state() = default;

        void update(partition_shutdown_stage);
        // it is more convenient to keep the pointer to partition than an ntp
        // copy.
        ss::lw_shared_ptr<partition> partition;
        partition_shutdown_stage stage;
        ss::lowres_clock::time_point last_update_timestamp;
        intrusive_list_hook hook;
    };

    ss::future<xshard_transfer_state> do_shutdown(ss::lw_shared_ptr<partition>);

    void check_partitions_shutdown_state();

    void maybe_arm_shutdown_watchdog();
    storage::api& _storage;
    /// used to wait for concurrent recoveries
    ss::sharded<raft::group_manager>& _raft_manager;

    ntp_callbacks<manage_cb_t> _manage_watchers;
    ntp_callbacks<unmanage_cb_t> _unmanage_watchers;
    // XXX use intrusive containers here
    ntp_table_container _ntp_table;
    chunked_hash_map<raft::group_id, ss::lw_shared_ptr<partition>> _raft_table;

    ss::sharded<features::feature_table>& _feature_table;
    intrusive_list<partition_shutdown_state, &partition_shutdown_state::hook>
      _partitions_shutting_down;
    ss::gate _gate;
    config::binding<std::chrono::milliseconds> _partition_shutdown_timeout;
    ss::timer<> _shutdown_watchdog;

    // In general, all our background work is in partition objects which
    // have their own abort source.  This abort source is only for work that
    // happens after partition stop, during deletion.
    ss::abort_source _as;

    bool _block_new_leadership{false};

    // Our handle from registering for leadership notifications on group_manager
    std::optional<raft::group_manager_notification_id> _leader_notify_handle;

    state_machine_registry _stm_registry;

    static std::string_view
    shutdown_stage_string(partition_shutdown_stage stage);

    friend fmt::iterator format_to(partition_shutdown_stage, fmt::iterator);
};
} // namespace cluster
