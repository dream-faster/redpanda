/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "redpanda/admin/services/utils.h"

#include "cluster/metadata_cache.h"
#include "serde/protobuf/rpc.h"
#include "ssx/sformat.h"

namespace admin::utils {

std::optional<model::node_id> redirect_to_leader(
  const cluster::metadata_cache& md_cache,
  const model::ntp& ntp,
  model::node_id self) {
    auto leader_node = md_cache.get_leader_id(ntp);
    if (!leader_node) {
        throw serde::pb::rpc::unavailable_exception{
          ssx::sformat("Partition {} does not have a leader", ntp)};
    }

    if (*leader_node == self) {
        return std::nullopt;
    }

    return *leader_node;
}

} // namespace admin::utils
