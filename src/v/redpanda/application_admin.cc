// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/controller.h"
#include "config/node_config.h"
#include "redpanda/admin/server.h"
#include "redpanda/application.h"
#include "resource_mgmt/memory_groups.h"
#include "resource_mgmt/scheduling_groups_probe.h"
#include "syschecks/syschecks.h"

static admin_server_cfg
admin_server_cfg_from_global_cfg(scheduling_groups& sgs) {
    return admin_server_cfg{
      .endpoints = config::node().admin(),
      .endpoints_tls = config::node().admin_api_tls(),
      .admin_api_docs_dir = config::node().admin_api_doc_dir(),
      .sg = sgs.admin_sg(),
      .max_memory_usage_bytes = memory_groups().admin_max_memory(),
    };
}

void application::configure_admin_server() {
    if (config::node().admin().empty()) {
        return;
    }

    syschecks::systemd_message("constructing http server").get();
    construct_service(
      _admin,
      admin_server_cfg_from_global_cfg(scheduling_groups::instance()),
      std::ref(stress_fiber_manager),
      std::ref(partition_manager),
      std::ref(raft_group_manager),
      controller.get(),
      std::ref(shard_table),
      std::ref(metadata_cache),
      std::ref(_connection_cache),
      std::ref(node_status_table),
      std::ref(storage_node),
      std::ref(_memory_sampling),
      std::ref(_cpu_profiler),
      std::ref(audit_mgr),
      std::ref(_tx_manager_migrator),
      std::ref(_kafka_server.ref()),
      std::ref(tx_gateway_frontend))
      .get();
}
