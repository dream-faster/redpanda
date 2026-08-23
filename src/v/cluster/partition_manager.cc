// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/partition_manager.h"

#include "base/vlog.h"
#include "cluster/fwd.h"
#include "cluster/logger.h"
#include "cluster/partition.h"
#include "cluster/topic_configuration.h"
#include "cluster/types.h"
#include "model/metadata.h"
#include "raft/consensus.h"
#include "raft/consensus_utils.h"
#include "raft/fundamental.h"
#include "ssx/async-clear.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>

#include <exception>
#include <optional>
#include <utility>

namespace cluster {

partition_manager::partition_manager(
  ss::sharded<storage::api>& storage,
  ss::sharded<raft::group_manager>& raft,
  ss::sharded<features::feature_table>& feature_table,
  config::binding<std::chrono::milliseconds> partition_shutdown_timeout)
  : _storage(storage.local())
  , _raft_manager(raft)
  , _feature_table(feature_table)
  , _partition_shutdown_timeout(std::move(partition_shutdown_timeout)) {
    _shutdown_watchdog.set_callback(
      [this] { check_partitions_shutdown_state(); });
}

partition_manager::~partition_manager() {
    if (_leader_notify_handle) {
        _raft_manager.local().unregister_leadership_notification(
          *_leader_notify_handle);
    }
}

partition_manager::ntp_table_container
partition_manager::get_topic_partition_table(
  model::topic_namespace_view tn) const {
    ntp_table_container rs;
    for (const auto& p : _ntp_table) {
        if (p.second->ntp().ns == tn.ns && p.second->ntp().tp.topic == tn.tp) {
            rs.emplace(p.first, p.second);
        }
    }
    return rs;
}

ss::future<> partition_manager::start() {
    maybe_arm_shutdown_watchdog();
    co_return;
}

ss::future<consensus_ptr> partition_manager::manage(
  storage::ntp_config ntp_cfg,
  raft::group_id group,
  std::vector<raft::vnode> initial_nodes,
  raft::with_learner_recovery_throttle enable_learner_recovery_throttle,
  raft::keep_snapshotted_log keep_snapshotted_log,
  std::optional<xshard_transfer_state> xst_state,
  const topic_configuration* topic_cfg,
  std::optional<partition_bootstrap_params> bootstrap_params) {
    vlog(
      clusterlog.trace,
      "Creating partition with configuration: {}, raft group_id: {}, "
      "initial_nodes: {}",
      ntp_cfg,
      group,
      initial_nodes);

    auto guard = _gate.hold();

    if (bootstrap_params.has_value()) {
        // Programmatic bootstrap with custom offset/term.
        // This is used for creating partitions with arbitrary start
        // offset during cluster recovery.
        vlog(
          clusterlog.info,
          "Bootstrapping partition {} with start offset: {}, term: {}",
          ntp_cfg.ntp(),
          bootstrap_params->start_offset,
          bootstrap_params->initial_term);

        co_await seastar::recursive_touch_directory(ntp_cfg.work_directory());

        co_await raft::details::bootstrap_partition_state(
          _storage,
          ntp_cfg,
          group,
          bootstrap_params->next_offset,
          bootstrap_params->initial_term,
          initial_nodes);
    }

    auto translator_batch_types = raft::offset_translator_batch_types(
      ntp_cfg.ntp());
    auto log = co_await _storage.log_mgr().manage(
      std::move(ntp_cfg), group, std::move(translator_batch_types));
    vlog(
      clusterlog.debug,
      "Log created manage completed, ntp: {}, rev: {}, {} "
      "segments, {} bytes",
      log->config().ntp(),
      log->config().get_revision(),
      log->segment_count(),
      log->size_bytes());

    ss::lw_shared_ptr<raft::consensus> c
      = co_await _raft_manager.local().create_group(
        group,
        initial_nodes,
        log,
        enable_learner_recovery_throttle,
        keep_snapshotted_log);

    auto p = ss::make_lw_shared<partition>(c, _feature_table);

    _ntp_table.emplace(log->config().ntp(), p);
    _raft_table.emplace(group, p);

    /*
     * part of the node leadership draining infrastructure. when a node is
     * in a drianing state new groups might be created since the controller
     * will still be active as a follower. however, if draining is almost
     * complete then new groups may not be noticed. marking as blocked
     * should be done atomically with adding the partition to the ntp_table
     * index above for proper synchronization with the drianing manager.
     */
    if (_block_new_leadership) {
        p->block_new_leadership();
    }

    auto stm_builder = _stm_registry.make_builder_for(c.get(), topic_cfg);

    co_await p->start(std::move(stm_builder), std::move(xst_state));

    // this is not done in partition::start itself because the purpose of this
    // flag is to operate in an uninterruptible context with watcher
    // notification below to avoid registration while this fiber is blocked,
    // leading to double notifications.
    p->mark_started();

    _manage_watchers.notify(p->ntp(), p);

    co_return c;
}

ss::future<> partition_manager::stop_partitions() {
    _as.request_abort();

    _raft_manager.local().unregister_leadership_notification(
      *_leader_notify_handle);
    _leader_notify_handle.reset();

    co_await _gate.close();
    // prevent partitions from being accessed
    auto partitions = std::exchange(_ntp_table, {});

    co_await ssx::async_clear(_raft_table);

    // shutdown all partitions
    co_await ss::max_concurrent_for_each(partitions, 1024, [this](auto& e) {
        return do_shutdown(e.second).discard_result();
    });

    co_await ssx::async_clear(partitions);
}

ss::future<xshard_transfer_state>
partition_manager::do_shutdown(ss::lw_shared_ptr<partition> partition) {
    partition_shutdown_state shutdown_state(partition);
    _partitions_shutting_down.push_back(shutdown_state);

    xshard_transfer_state xst_state;
    try {
        auto ntp = partition->ntp();
        shutdown_state.update(partition_shutdown_stage::stopping_raft);
        vlog(clusterlog.debug, "shutdown partition {} - stopping raft", ntp);
        xst_state.raft = co_await _raft_manager.local().shutdown(
          partition->raft());
        _unmanage_watchers.notify(ntp, model::topic_partition_view(ntp.tp));
        shutdown_state.update(partition_shutdown_stage::stopping_partition);
        vlog(
          clusterlog.debug, "shutdown partition {} - stopping partition", ntp);
        co_await partition->stop();
        shutdown_state.update(partition_shutdown_stage::stopping_storage);
        vlog(clusterlog.debug, "shutdown partition {} - stopping log", ntp);
        co_await _storage.log_mgr().shutdown(partition->ntp());
        vlog(clusterlog.debug, "shutdown partition {} - stopped", ntp);
    } catch (...) {
        vassert(
          false,
          "error shutting down partition {},  "
          "partition manager state: {}, error: {} - terminating redpanda",
          partition->ntp(),
          *this,
          std::current_exception());
    }
    co_return xst_state;
}

ss::future<>
partition_manager::remove(const model::ntp& ntp, partition_removal_mode) {
    auto guard = _gate.hold();

    auto partition = get(ntp);

    if (!partition) {
        throw std::invalid_argument(
          fmt::format(
            "Can not remove partition. NTP {} is not present in partition "
            "manager",
            ntp));
    }
    vlog(clusterlog.debug, "removing partition {}", ntp);
    partition_shutdown_state shutdown_state(partition);
    _partitions_shutting_down.push_back(shutdown_state);
    auto group_id = partition->group();

    // remove partition from ntp & raft tables
    _ntp_table.erase(ntp);
    _raft_table.erase(group_id);
    shutdown_state.update(partition_shutdown_stage::removing_raft);
    co_await _raft_manager.local().remove(partition->raft());
    _unmanage_watchers.notify(
      ntp, model::topic_partition_view(partition->ntp().tp));
    shutdown_state.update(partition_shutdown_stage::stopping_partition);
    co_await partition->stop();
    shutdown_state.update(partition_shutdown_stage::removing_persistent_state);
    co_await partition->remove_persistent_state();
    shutdown_state.update(partition_shutdown_stage::removing_storage);
    co_await _storage.log_mgr().remove(partition->ntp());
}

ss::future<xshard_transfer_state>
partition_manager::shutdown(const model::ntp& ntp) {
    auto guard = _gate.hold();

    auto partition = get(ntp);
    if (!partition) {
        return ss::make_exception_future<xshard_transfer_state>(
          std::invalid_argument(
            fmt::format(
              "Can not shutdown partition. NTP {} is not present in "
              "partition manager",
              ntp)));
    }
    // remove partition from ntp & raft tables
    _ntp_table.erase(ntp);
    _raft_table.erase(partition->group());

    return do_shutdown(partition);
}

partition_manager::partition_shutdown_state::partition_shutdown_state(
  ss::lw_shared_ptr<cluster::partition> p)
  : partition(std::move(p))
  , stage(partition_manager::partition_shutdown_stage::shutdown_requested)
  , last_update_timestamp(ss::lowres_clock::now()) {}

void partition_manager::partition_shutdown_state::update(
  partition_shutdown_stage s) {
    stage = s;
    last_update_timestamp = ss::lowres_clock::now();
}

void partition_manager::check_partitions_shutdown_state() {
    const auto now = ss::lowres_clock::now();
    for (auto& state : _partitions_shutting_down) {
        if (state.last_update_timestamp < now - _partition_shutdown_timeout()) {
            vlog(
              clusterlog.error,
              "partition {} shutdown takes longer than expected, current "
              "shutdown stage: {} time since last update: {} seconds",
              state.partition->ntp(),
              state.stage,
              (now - state.last_update_timestamp) / 1s);
        }
    }
    maybe_arm_shutdown_watchdog();
}

void partition_manager::maybe_arm_shutdown_watchdog() {
    if (!_as.abort_requested()) {
        _shutdown_watchdog.arm(_partition_shutdown_timeout() / 5);
    }
}

fmt::iterator partition_manager::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{shard:{}, mngr:{{{}}}, ntp_table.size:{}, raft_table.size:{}}}",
      ss::this_shard_id(),
      fmt_streamed(_storage.log_mgr()),
      _ntp_table.size(),
      _raft_table.size());
}

ss::future<size_t> partition_manager::non_log_disk_size_bytes() const {
    co_return co_await container().map_reduce0(
      [](const partition_manager& pm) {
          const auto size = std::accumulate(
            pm._raft_table.cbegin(),
            pm._raft_table.cend(),
            size_t{0},
            [](size_t acc, const auto& elem) {
                return acc + elem.second->non_log_disk_size_bytes();
            });
          return size;
      },
      size_t{0},
      [](size_t acc, size_t update) { return acc + update; });
}

std::string_view
partition_manager::shutdown_stage_string(partition_shutdown_stage stage) {
    switch (stage) {
    case partition_shutdown_stage::shutdown_requested:
        return "shutdown_requested";
    case partition_shutdown_stage::stopping_raft:
        return "stopping_raft";
    case partition_shutdown_stage::removing_raft:
        return "removing_raft";
    case partition_shutdown_stage::stopping_partition:
        return "stopping_partition";
    case partition_shutdown_stage::removing_persistent_state:
        return "removing_persistent_state";
    case partition_shutdown_stage::stopping_storage:
        return "stopping_storage";
    case partition_shutdown_stage::removing_storage:
        return "removing_storage";
    }
    return "unknown";
}

fmt::iterator
format_to(partition_manager::partition_shutdown_stage stage, fmt::iterator it) {
    return fmt::format_to(
      it, "{}", partition_manager::shutdown_stage_string(stage));
}

} // namespace cluster
