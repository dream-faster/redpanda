/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/cluster_info_initializer.h"

#include "bytes/iobuf.h"
#include "cluster/config_frontend.h"
#include "cluster/controller_stm.h"
#include "cluster/logger.h"
#include "config/configuration.h"
#include "hashing/secure.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "raft/consensus.h"
#include "reflection/adl.h"
#include "ssx/future-util.h"
#include "storage/types.h"

#include <seastar/core/sleep.hh>

#include <boost/random/seed_seq.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/core.h>

using namespace std::chrono_literals;

namespace cluster {

cluster_info_initializer::cluster_info_initializer(
  consensus_ptr raft0,
  ss::sharded<controller_stm>& controller_stm,
  ss::sharded<config_frontend>& config_frontend,
  ss::sharded<ss::abort_source>& as)
  : _raft0(std::move(raft0))
  , _cluster_info(controller_stm.local().get_metrics_reporter_cluster_info())
  , _controller_stm(controller_stm)
  , _config_frontend(config_frontend)
  , _as(as) {}

ss::future<> cluster_info_initializer::start() {
    // Start promptly: the controller cannot generate snapshots until the
    // cluster UUID is initialized.
    ssx::background = ssx::spawn_with_gate_then(_gate, [this] {
                          return run();
                      }).handle_exception([](const std::exception_ptr& e) {
        vlog(clusterlog.warn, "Exception initializing cluster info: {}", e);
    });
    co_return;
}

ss::future<> cluster_info_initializer::stop() { co_await _gate.close(); }

ss::future<> cluster_info_initializer::run() {
    constexpr auto retry_interval = 10s;
    while (!_as.local().abort_requested()) {
        try {
            // Every node does this, so that each can snapshot its controller.
            co_await try_initialize_cluster_info();

            // Wait until we become leader, or another node writes it.
            if (_cluster_info.is_initialized()) {
                while (!_as.local().abort_requested()
                       && !config::shard_local_cfg().cluster_id().has_value()) {
                    if (_raft0->is_elected_leader()) {
                        co_await propagate_cluster_id();
                    } else {
                        co_await ss::sleep_abortable(
                          config::shard_local_cfg()
                            .raft_heartbeat_interval_ms(),
                          _as.local());
                    }
                }
                co_return;
            }
        } catch (const ss::sleep_aborted&) {
            co_return;
        } catch (...) {
            vlog(
              clusterlog.debug,
              "Error initializing cluster info, will retry: {}",
              std::current_exception());
        }

        try {
            co_await ss::sleep_abortable(retry_interval, _as.local());
        } catch (const ss::sleep_aborted&) {
            co_return;
        }
    }
}

ss::future<> cluster_info_initializer::try_initialize_cluster_info() {
    // already initialized, do nothing
    if (_cluster_info.is_initialized()) {
        co_return;
    }

    // Wait until the controller log has seen enough batches to generate
    // a UUID.
    if (_raft0->committed_offset() < model::offset{2}) {
        vlog(clusterlog.info, "Waiting to initialize cluster metrics ID...");
        co_await _controller_stm.local().wait(
          model::offset{2},
          model::timeout_clock::time_point::max(),
          std::ref(_as.local()));
    }

    if (_raft0->start_offset() > model::offset{0}) {
        // Controller log already snapshotted, wait until cluster info gets
        // initialized from the snapshot.
        co_return;
    }

    storage::local_log_reader_config reader_cfg(
      model::offset(0), model::offset(2));
    auto reader = co_await _raft0->make_reader(reader_cfg);

    auto batches = co_await model::consume_reader_to_memory(
      std::move(reader), model::no_timeout);
    if (batches.size() < 2) {
        co_return;
    }

    auto& first_cfg = batches.front();

    _cluster_info.creation_timestamp = first_cfg.header().first_timestamp;

    auto data_bytes = iobuf_to_bytes(first_cfg.data());
    hash_sha256 sha256;
    sha256.update(data_bytes);
    // use timestamps of first two batches in raft-0 log.
    for (int i = 0; i < 2; ++i) {
        sha256.update(iobuf_to_bytes(
          reflection::to_iobuf(batches[i].header().first_timestamp())));
    }
    auto hash = sha256.reset();
    // seed prng with data and timestamps hash
    boost::random::mt19937 mersenne_twister;
    boost::random::seed_seq seed(hash.begin(), hash.end());
    mersenne_twister.seed(seed);

    boost::uuids::random_generator_mt19937 uuid_gen(mersenne_twister);

    _cluster_info.uuid = fmt::format("{}", uuid_gen());
    vlog(
      clusterlog.info, "Generated cluster metrics ID {}", _cluster_info.uuid);
}

/**
 * Having synthesized a unique cluster ID in try_initialize_cluster_info,
 * set it as the global cluster_id in the cluster configuration, if there
 * is not already a cluster_id set there.
 */
ss::future<> cluster_info_initializer::propagate_cluster_id() {
    if (config::shard_local_cfg().cluster_id().has_value()) {
        // Don't override any existing cluster_id
        co_return;
    }

    if (_cluster_info.uuid == "") {
        co_return;
    }

    auto result = co_await _config_frontend.local().patch(
      config_update_request{.upsert = {{"cluster_id", _cluster_info.uuid}}},
      model::timeout_clock::now() + 5s);
    if (result.errc) {
        vlog(
          clusterlog.warn, "Failed to initialize cluster_id: {}", result.errc);
        co_await ss::sleep_abortable(1s, _as.local());
    } else {
        vlog(
          clusterlog.info, "Initialized cluster_id to {}", _cluster_info.uuid);
    }
}

} // namespace cluster
