/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "base/seastarx.h"
#include "serde/rw/chrono.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/scalar.h"
#include "serde/rw/sstring.h"
#include "serde/rw/vector.h"

#include <seastar/core/sstring.hh>

#include <chrono>
#include <vector>

namespace security {

enum class license_type : uint8_t { free_trial = 0, enterprise = 1 };

/// \brief A Redpanda Enterprise license as stock Redpanda serializes it.
///
/// This build never parses, verifies or enforces licenses. The type only
/// exists because stock nodes write it into the controller log and the
/// feature table snapshot, and those must stay readable in both directions
/// during a rolling upgrade. Do not change the field list or its order.
struct license
  : serde::envelope<license, serde::version<2>, serde::compat_version<0>> {
    uint8_t format_version;
    license_type _type;
    ss::sstring organization;
    std::chrono::seconds expiry;
    ss::sstring checksum;
    ss::sstring _type_str;
    std::vector<ss::sstring> products;

    auto serde_fields() {
        return std::tie(
          format_version,
          _type,
          organization,
          expiry,
          checksum,
          _type_str,
          products);
    }

    friend bool operator==(const license&, const license&) = default;
};

} // namespace security
