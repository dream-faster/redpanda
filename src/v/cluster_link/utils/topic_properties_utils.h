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

#pragma once

#include "features/fwd.h"
#include "kafka/server/handlers/configs/config_utils.h"

namespace cluster_link::utils {

bool maybe_append_update(
  cluster::topic_properties_update& update,
  const ss::sstring& config_name,
  const ss::sstring& config_value,
  const cluster::topic_configuration& topic_config);

/// Turn the source topic's redpanda.storage.mode into a storage-mode
/// update. Updates that are not permitted storage-mode transitions are
/// rejected with a validation error rather than applied.
bool maybe_append_storage_mode_update(
  cluster::topic_properties_update& update,
  const std::optional<ss::sstring>& mode_value,
  const cluster::topic_configuration& topic_config);

} // namespace cluster_link::utils
