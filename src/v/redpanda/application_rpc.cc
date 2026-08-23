// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/controller.h"
#include "cluster/ephemeral_credential_service.h"
#include "cluster/id_allocator.h"
#include "cluster/metadata_dissemination_handler.h"
#include "cluster/migrations/tx_manager_migrator_handler.h"
#include "cluster/node_status_rpc_handler.h"
#include "cluster/partition_balancer_rpc_handler.h"
#include "cluster/partition_manager.h"
#include "cluster/self_test_rpc_handler.h"
#include "cluster/service.h"
#include "cluster/tx_gateway.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "kafka/data/rpc/service.h"
#include "kafka/server/rm_group_frontend.h"
#include "raft/service.h"
#include "redpanda/admin/proxy/service.h"
#include "redpanda/admin/server.h"
#include "redpanda/application.h"
#include "resource_mgmt/scheduling_groups_probe.h"

void application::add_runtime_rpc_services(
  rpc::rpc_server& s, bool start_raft_rpc_early) {
    std::vector<std::unique_ptr<rpc::service>> runtime_services;
    runtime_services.push_back(
      std::make_unique<cluster::id_allocator>(
        scheduling_groups::instance().raft_recv_sg(),
        smp_service_groups.raft_smp_sg(),
        std::ref(id_allocator_frontend)));
    // _rm_group_proxy is wrap around a sharded service with only
    // `.local()' access so it's ok to share without foreign_ptr
    runtime_services.push_back(
      std::make_unique<cluster::tx_gateway>(
        scheduling_groups::instance().raft_recv_sg(),
        smp_service_groups.raft_smp_sg(),
        std::ref(tx_gateway_frontend),
        _rm_group_proxy.get(),
        std::ref(rm_partition_frontend)));

    if (config::node().recovery_mode_enabled()) {
        runtime_services.push_back(
          std::make_unique<cluster::tx_manager_migrator_handler>(
            scheduling_groups::instance().cluster_sg(),
            smp_service_groups.cluster_smp_sg(),
            std::ref(controller->get_partition_manager()),
            std::ref(controller->get_shard_table()),
            std::ref(metadata_cache),
            std::ref(_connection_cache),
            std::ref(controller->get_partition_leaders()),
            config::node().node_id().value(),
            _as.local()));
    }
    runtime_services.push_back(
      std::make_unique<admin::proxy::service_impl>(
        scheduling_groups::instance().admin_sg(),
        smp_service_groups.cluster_smp_sg(),
        [this](serde::pb::rpc::context ctx, iobuf buf) {
            if (!_admin.local_is_initialized()) {
                throw std::runtime_error("admin service is not initialized");
            }
            return _admin.local().handle_rpc_request(
              std::move(ctx), std::move(buf));
        }));

    s.add_services(std::move(runtime_services));

    // Done! Disallow unknown method errors.
    s.set_all_services_added();
}
