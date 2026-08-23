// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_storage_clients/types.h"
#include "cluster/controller.h"
#include "cluster/utils/partition_change_notifier_impl.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "debug_bundle/debug_bundle_service.h"
#include "kafka/data/rpc/client.h"
#include "kafka/server/usage_manager.h"
#include "redpanda/admin/kafka_connections_service.h"
#include "redpanda/application.h"
#include "resource_mgmt/memory_groups.h"
#include "resource_mgmt/scheduling_groups_probe.h"
#include "syschecks/syschecks.h"

#include <seastar/core/metrics.hh>

void application::wire_up_runtime_services(
  model::node_id node_id, ::stop_signal& app_signal) {
    std::optional<cloud_storage_clients::bucket_name> bucket;
    wire_up_redpanda_services(node_id, app_signal, bucket);
    syschecks::systemd_message("Creating kafka usage manager frontend").get();
    construct_service(
      usage_manager,
      controller.get(),
      std::ref(controller->get_health_monitor()),
      std::ref(storage))
      .get();

    construct_single_service(_monitor_unsafe, std::ref(feature_table));

    construct_service(_debug_bundle_service, &storage.local().kvs()).get();

    auto data_dir = config::node().data_directory().as_sstring();
    auto cache_dir = ss::sstring(
      config::node().cloud_storage_cache_path().string());
    construct_single_service(
      _host_metrics_watcher, std::ref(_log), data_dir, cache_dir);

    // Expose cloud instance info as metrics (detected in the background).
    construct_single_service(_instance_metrics, std::ref(_as.local()));
    _instance_metrics->start();

    construct_service(_kafka_connections_service, std::ref(_kafka_server.ref()))
      .get();

    configure_admin_server(node_id);
}
