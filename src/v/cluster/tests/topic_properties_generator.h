// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "base/units.h"
#include "cluster/types.h"
#include "model/tests/randoms.h"
#include "random/generators.h"
#include "test_utils/randoms.h"

inline cluster::topic_properties old_random_topic_properties() {
    cluster::topic_properties properties;
    properties.cleanup_policy_bitflags = tests::random_optional(
      [] { return model::random_cleanup_policy(); });
    properties.compaction_strategy = tests::random_optional(
      [] { return model::random_compaction_strategy(); });
    properties.compression = tests::random_optional(
      [] { return model::random_compression(); });
    properties.timestamp_type = tests::random_optional(
      [] { return model::random_timestamp_type(); });
    properties.segment_size = tests::random_optional(
      [] { return random_generators::get_int(100_MiB, 1_GiB); });
    properties.retention_bytes = tests::random_tristate(
      [] { return random_generators::get_int(100_MiB, 1_GiB); });
    properties.retention_duration = tests::random_tristate(
      [] { return tests::random_duration_ms(); });
    return properties;
}

inline cluster::topic_properties random_topic_properties() {
    cluster::topic_properties properties = old_random_topic_properties();

    properties.write_caching = tests::random_optional([] {
        return random_generators::random_choice(
          {model::write_caching_mode::default_true,
           model::write_caching_mode::default_false,
           model::write_caching_mode::disabled});
    });
    properties.flush_ms = tests::random_optional(
      [] { return tests::random_duration_ms(); });
    properties.flush_bytes = tests::random_optional(
      [] { return random_generators::get_int<size_t>(); });
    return properties;
}
