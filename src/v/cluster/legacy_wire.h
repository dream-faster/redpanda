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

#include "base/format_to.h"
#include "bytes/iobuf_parser.h"
#include "model/fundamental.h"
#include "serde/rw/envelope.h"
#include "serde/rw/named_type.h"
#include "serde/rw/rw.h"
#include "serde/rw/scalar.h"
#include "serde/rw/sstring.h"
#include "serde/rw/uuid.h"
#include "serde/serde_exception.h"
#include "serde/serde_is_enum.h"

#include <tuple>

/// Wire-format stand-ins for fields of persisted serde structs whose feature
/// (tiered storage, iceberg, schema registry validation, ...) was removed.
///
/// serde envelopes are positional, so deleting a field changes the bytes on
/// disk and on the wire: a node upgraded in place from stock v26.2.x could no
/// longer read its own controller log, controller snapshot or kvstore, and
/// would not interoperate with stock peers during a rolling upgrade. The
/// removed fields therefore stay in their original slots. They are never
/// consulted and are written with the value a topic that doesn't use the
/// feature carries; whatever a stock peer wrote is parsed and dropped.
namespace cluster::legacy {

/// An `enum class` that serde writes as its int32 discriminant.
struct enum_wire {
    serde::serde_enum_serialized_t value{0};

    enum_wire() = default;
    explicit enum_wire(serde::serde_enum_serialized_t v)
      : value(v) {}

    friend bool operator==(const enum_wire&, const enum_wire&) = default;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{}", value);
    }
};

inline void
tag_invoke(serde::tag_t<serde::write_tag>, iobuf& out, enum_wire e) {
    serde::write(out, e.value);
}

inline void tag_invoke(
  serde::tag_t<serde::read_tag>,
  iobuf_parser& in,
  enum_wire& e,
  const std::size_t bytes_left_limit) {
    e.value = serde::read_nested<serde::serde_enum_serialized_t>(
      in, bytes_left_limit);
}

/// cloud_storage::remote_label.
struct remote_label_wire
  : serde::
      envelope<remote_label_wire, serde::version<0>, serde::compat_version<0>> {
    model::cluster_uuid cluster_uuid{};

    auto serde_fields() { return std::tie(cluster_uuid); }

    friend bool
    operator==(const remote_label_wire&, const remote_label_wire&) = default;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{{cluster_uuid: {}}}", cluster_uuid);
    }
};

/// cluster::remote_topic_properties.
struct remote_topic_properties_wire
  : serde::envelope<
      remote_topic_properties_wire,
      serde::version<0>,
      serde::compat_version<0>> {
    model::initial_revision_id remote_revision;
    int32_t remote_partition_count{0};

    auto serde_fields() {
        return std::tie(remote_revision, remote_partition_count);
    }

    friend bool operator==(
      const remote_topic_properties_wire&,
      const remote_topic_properties_wire&) = default;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it,
          "{{remote_revision: {}, remote_partition_count: {}}}",
          remote_revision,
          remote_partition_count);
    }
};

/// An envelope that is skipped on read and written empty. Peers that still
/// know the real type read an empty envelope as a default-constructed value.
struct empty_envelope
  : serde::
      envelope<empty_envelope, serde::version<0>, serde::compat_version<0>> {
    auto serde_fields() { return std::tie(); }

    friend bool
    operator==(const empty_envelope&, const empty_envelope&) = default;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{{}}");
    }
};

/// model::iceberg_mode. Always written as the "disabled" discriminant; every
/// enabled shape is consumed on read.
struct iceberg_mode_wire {
    friend bool
    operator==(const iceberg_mode_wire&, const iceberg_mode_wire&) = default;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "disabled");
    }
};

inline void
tag_invoke(serde::tag_t<serde::write_tag>, iobuf& out, iceberg_mode_wire) {
    serde::write(out, int32_t{0});
}

inline void tag_invoke(
  serde::tag_t<serde::read_tag>,
  iobuf_parser& in,
  iceberg_mode_wire&,
  const std::size_t bytes_left_limit) {
    switch (serde::read_nested<int32_t>(in, bytes_left_limit)) {
    case 0: // disabled
    case 1: // key_value
    case 2: // value_schema_id_prefix
        return;
    case 3: // value_schema_latest: protobuf name, subject
        std::ignore = serde::read_nested<ss::sstring>(in, bytes_left_limit);
        std::ignore = serde::read_nested<ss::sstring>(in, bytes_left_limit);
        return;
    case 4: // canonical config string in an envelope
        std::ignore = serde::read_nested<empty_envelope>(in, bytes_left_limit);
        return;
    default:
        throw serde::serde_exception("unknown iceberg_mode discriminant");
    }
}

} // namespace cluster::legacy
