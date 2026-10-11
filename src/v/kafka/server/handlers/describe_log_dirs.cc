/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/server/handlers/describe_log_dirs.h"

#include "cluster/partition_manager.h"
#include "config/node_config.h"
#include "container/chunked_vector.h"
#include "kafka/data/partition_proxy.h"
#include "kafka/protocol/errors.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "model/fundamental.h"
#include "model/ktp.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>

namespace kafka {

struct log_partition_data {
    describe_log_dirs_partition local;
};

using partition_dir_set
  = chunked_hash_map<model::topic, chunked_vector<log_partition_data>>;

static log_partition_data describe_partition(const kafka::partition_proxy& p) {
    return log_partition_data{
      .local = describe_log_dirs_partition{
        .partition_index = p.ntp().tp.partition(),
        .partition_size = static_cast<int64_t>(p.local_size_bytes()),
        .offset_lag = std::max(p.offset_lag()(), int64_t(0)),
        .is_future_key = false,
      }};
}

static ss::future<partition_dir_set> collect_mapper(
  cluster::partition_manager& pm,
  const std::optional<std::vector<describable_log_dir_topic>>& topics) {
    chunked_vector<model::ktp> ktps;
    if (!topics) {
        // return all partitions
        ktps.reserve(pm.partitions().size());
        for (const auto& partition : pm.partitions()) {
            ktps.emplace_back(
              partition.second->ntp().tp.topic,
              partition.second->ntp().tp.partition);
        }
    } else {
        // return only partition matching request
        for (const auto& topic : *topics) {
            for (auto p_id : topic.partition_index) {
                ktps.emplace_back(topic.topic, p_id);
            }
        }
    }

    partition_dir_set ret;
    for (const auto& ktp : ktps) {
        auto proxy = make_partition_proxy(ktp, pm);
        if (proxy) {
            ret[ktp.get_topic()].push_back(describe_partition(*proxy));
        }
    }
    co_return ret;
}

/*
 * collect log directory information for partitions
 */
static ss::future<partition_dir_set> collect(
  request_context& ctx,
  std::optional<chunked_vector<describable_log_dir_topic>> filter) {
    std::optional<std::vector<describable_log_dir_topic>> filter_v;
    if (filter) {
        filter_v.emplace(
          std::make_move_iterator(filter->begin()),
          std::make_move_iterator(filter->end()));
    }
    return ctx.partition_manager().map_reduce0(
      [filter{std::move(filter_v)}](cluster::partition_manager& pm) {
          return collect_mapper(pm, filter);
      },
      partition_dir_set{},
      [](partition_dir_set acc, const partition_dir_set& update) {
          for (auto& topic : update) {
              for (auto partition : topic.second) {
                  acc[topic.first].push_back(std::move(partition));
              }
          }
          return acc;
      });
}

template<>
ss::future<response_ptr> describe_log_dirs_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group ssg) {
    describe_log_dirs_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    describe_log_dirs_response response;

    // redpanda only supports a single data directory right now
    response.data.results.push_back(
      describe_log_dirs_result{
        .error_code = error_code::none,
        .log_dir = config::node().data_directory().as_sstring(),
      });

    auto authz = ctx.authorized(
      security::acl_operation::describe, security::default_cluster_name);

    if (!ctx.audit()) {
        response.data.error_code = error_code::broker_not_available;
        co_return co_await ctx.respond(std::move(response));
    }

    if (!authz) {
        // in this case kafka returns no authorization error
        co_return co_await ctx.respond(std::move(response));
    }

    auto& local_results = response.data.results.at(0);

    auto partitions = co_await collect(ctx, std::move(request.data.topics));
    while (!partitions.empty()) {
        auto node = partitions.extract(partitions.begin());

        chunked_vector<describe_log_dirs_partition> local_partitions;
        for (const auto& i : node.second) {
            local_partitions.push_back(i.local);
        }

        local_results.topics.push_back(
          describe_log_dirs_topic{
            .name = std::move(node.first),
            .partitions = std::move(local_partitions),
          });
    }

    co_return co_await ctx.respond(std::move(response));
}

} // namespace kafka
