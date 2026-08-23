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
#include "cluster_link/service.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "debug_bundle/debug_bundle_service.h"
#include "kafka/data/rpc/client.h"
#include "kafka/server/usage_manager.h"
#include "pandaproxy/rest/api.h"
#include "pandaproxy/schema_registry/api.h"
#include "redpanda/admin/kafka_connections_service.h"
#include "redpanda/application.h"
#include "resource_mgmt/memory_groups.h"
#include "resource_mgmt/scheduling_groups_probe.h"
#include "syschecks/syschecks.h"
#include "transform/api.h"
#include "transform/rpc/client.h"
#include "transform/rpc/service.h"
#include "wasm/cache.h"
#include "wasm/impl.h"

#include <seastar/core/metrics.hh>

void application::wire_up_runtime_services(
  model::node_id node_id, ::stop_signal& app_signal) {
    std::optional<cloud_storage_clients::bucket_name> bucket;
    wire_up_redpanda_services(node_id, app_signal, bucket);
    if (_proxy_config) {
        construct_single_service(
          _proxy,
          smp_service_groups.proxy_smp_sg(),
          // TODO: Improve memory budget for services
          // https://github.com/redpanda-data/redpanda/issues/1392
          memory_groups().kafka_total_memory(),
          *_proxy_client_config,
          *_proxy_config,
          controller.get());
    }
    if (_schema_reg_config) {
        construct_single_service(
          _schema_registry,
          node_id,
          smp_service_groups.proxy_smp_sg(),
          // TODO: Improve memory budget for services
          // https://github.com/redpanda-data/redpanda/issues/1392
          memory_groups().kafka_total_memory(),
          *_schema_reg_client_config,
          *_schema_reg_config,
          &metadata_cache,
          std::reference_wrapper(controller),
          std::ref(audit_mgr),
          &_kafka_data_rpc_client);
    }

    if (wasm_data_transforms_enabled()) {
        syschecks::systemd_message("Starting wasm runtime").get();
        auto base_runtime = wasm::create_default_runtime(
          _schema_registry.get());
        construct_single_service(_wasm_runtime, std::move(base_runtime));

        syschecks::systemd_message("Starting data transforms").get();
        construct_service(
          _transform_rpc_service,
          ss::sharded_parameter([this] {
              return kafka::data::rpc::topic_metadata_cache::make_default(
                &metadata_cache);
          }),
          ss::sharded_parameter([this] {
              return transform::rpc::partition_manager::make_default(
                &shard_table,
                &partition_manager,
                smp_service_groups.transform_smp_sg());
          }),
          ss::sharded_parameter([this] {
              return transform::service::create_reporter(&_transform_service);
          }),
          ss::sharded_parameter([this] {
              return kafka::data::rpc::shadow_link_registry::make_default(
                &controller->get_cluster_link_frontend());
          }))
          .get();
        construct_service(
          _transform_rpc_client,
          node_id,
          ss::sharded_parameter([this] {
              return kafka::data::rpc::partition_leader_cache::make_default(
                &controller->get_partition_leaders());
          }),
          ss::sharded_parameter([this] {
              return kafka::data::rpc::topic_metadata_cache::make_default(
                &metadata_cache);
          }),
          ss::sharded_parameter([this] {
              return transform::rpc::cluster_members_cache::make_default(
                &controller->get_members_table());
          }),
          &_connection_cache,
          &_transform_rpc_service,
          &_kafka_data_rpc_client,
          &controller->get_feature_table(),
          ss::sharded_parameter([] {
              return config::shard_local_cfg()
                .data_transforms_binary_max_size.bind();
          }))
          .get();

        construct_service(
          _transform_service,
          _wasm_runtime.get(),
          node_id,
          &controller->get_plugin_frontend(),
          &controller->get_feature_table(),
          ss::sharded_parameter([this] {
              return cluster::partition_change_notifier_impl::make_default(
                raft_group_manager,
                partition_manager,
                controller->get_topics_state());
          }),
          &controller->get_topics_state(),
          &partition_manager,
          &_transform_rpc_client,
          &metadata_cache,
          scheduling_groups::instance().transforms_sg(),
          memory_groups().data_transforms_max_memory())
          .get();
    }

    construct_service(
      _cluster_link_service,
      node_id,
      ss::sharded_parameter([]() {
          return config::shard_local_cfg().enable_shadow_linking.bind();
      }),
      &controller->get_cluster_link_frontend(),
      ss::sharded_parameter([this] {
          return cluster::partition_change_notifier_impl::make_default(
            raft_group_manager,
            partition_manager,
            controller->get_topics_state());
      }),
      &partition_manager,
      &controller->get_partition_leaders(),
      &controller->get_shard_table(),
      &metadata_cache,
      &_connection_cache,
      controller.get(),
      &group_router,
      &snc_quota_mgr,
      &controller->get_health_monitor(),
      &controller->get_security_frontend(),
      &_kafka_data_rpc_client,
      &id_allocator_frontend,
      _schema_registry.get(),
      smp_service_groups.cluster_link_smp_sg(),
      scheduling_groups::instance().cluster_linking_sg())
      .get();

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
