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

#pragma once

#include "cluster/fwd.h"
#include "cluster/types.h"
#include "raft/fwd.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

namespace cluster {

/// \brief Derives the cluster UUID and copies it into `cluster_id`.
///
/// The UUID is a deterministic function of the first two controller log
/// batches, so every node computes the same value. It is stored in the
/// controller snapshot, and the controller does not snapshot until it is
/// set. Once the UUID is known, the controller leader writes it to the
/// `cluster_id` cluster property if that is still unset.
class cluster_info_initializer {
public:
    cluster_info_initializer(
      consensus_ptr raft0,
      ss::sharded<controller_stm>&,
      ss::sharded<config_frontend>&,
      ss::sharded<ss::abort_source>&);

    ss::future<> start();
    ss::future<> stop();

private:
    ss::future<> run();
    ss::future<> try_initialize_cluster_info();
    ss::future<> propagate_cluster_id();

    consensus_ptr _raft0;
    metrics_reporter_cluster_info& _cluster_info; // owned by controller_stm
    ss::sharded<controller_stm>& _controller_stm;
    ss::sharded<config_frontend>& _config_frontend;
    ss::sharded<ss::abort_source>& _as;
    ss::gate _gate;
};

} // namespace cluster
