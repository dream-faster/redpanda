// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "absl/container/flat_hash_map.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_split.h"
#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/timestamp.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/rw.h"
#include "serde/rw/sstring.h"
#include "serde/serde_exception.h"
#include "strings/string_switch.h"
#include "utils/base64.h"
#include "utils/to_string.h"

#include <fmt/ostream.h>

#include <expected>
#include <iostream>
#include <optional>
#include <type_traits>

namespace model {

fmt::iterator timestamp::format_to(fmt::iterator it) const {
    if (*this != missing()) {
        return fmt::format_to(it, "{{timestamp: {}}}", _v);
    }
    return fmt::format_to(it, "{{timestamp: missing}}");
}

void read_nested(
  iobuf_parser& in, timestamp& ts, const size_t bytes_left_limit) {
    serde::read_nested(in, ts._v, bytes_left_limit);
}

void write_nested(iobuf& out, timestamp ts) { serde::write(out, ts._v); }

fmt::iterator topic_partition_view::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{{}/{}}}", topic(), partition());
}

fmt::iterator topic_partition::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{{}/{}}}", topic(), partition());
}

fmt::iterator topic_id_partition::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{{}/{}}}", topic_id(), partition());
}

fmt::iterator ntp::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{{}/{}/{}}}", ns(), tp.topic(), tp.partition());
}

fmt::iterator topic_namespace::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{{}/{}}}", ns(), tp());
}

fmt::iterator topic_namespace_view::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{{}/{}}}", ns(), tp());
}

fmt::iterator format_to(timestamp_type ts, fmt::iterator out) {
    /**
     * We need to use specific string representations of timestamp_type as this
     * is related with protocol correctness
     */
    switch (ts) {
    case timestamp_type::append_time:
        return fmt::format_to(out, "LogAppendTime");
    case timestamp_type::create_time:
        return fmt::format_to(out, "CreateTime");
    }
    return fmt::format_to(
      out, "{{unknown timestamp:{}}}", static_cast<int>(ts));
}

fmt::iterator record_header::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{key_size={}, key={}, value_size={}, value={}}}",
      _key_size,
      _key,
      _val_size,
      _value);
}

fmt::iterator record_attributes::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{{}}}", _attributes);
}

fmt::iterator record::format_to(fmt::iterator it) const {
    it = fmt::format_to(
      it,
      "{{record: size_bytes={}, attributes={}, timestamp_delta={}, "
      "offset_delta={}, key_size={}, key={}, value_size={}, value={}, "
      "header_size:{}, headers=[",
      _size_bytes,
      _attributes,
      _timestamp_delta,
      _offset_delta,
      _key_size,
      _key,
      _val_size,
      _value,
      _headers.size());
    for (const auto& h : _headers) {
        it = fmt::format_to(it, "{}", h);
    }
    return fmt::format_to(it, "]}}");
}

fmt::iterator producer_identity::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{producer_identity: id={}, epoch={}}}", id, epoch);
}

fmt::iterator record_batch_attributes::format_to(fmt::iterator it) const {
    it = fmt::format_to(it, "{{compression:");
    if (is_valid_compression()) {
        // this method... sadly, just throws
        it = fmt::format_to(it, "{}", compression());
    } else {
        it = fmt::format_to(it, "invalid compression");
    }
    return fmt::format_to(
      it,
      ", type:{}, transactional: {}, control: {}}}",
      timestamp_type(),
      is_transactional(),
      is_control());
}

fmt::iterator record_batch_header::format_to(fmt::iterator it) const {
    it = fmt::format_to(
      it,
      "{{header_crc:{}, size_bytes:{}, base_offset:{}, type:{}, crc:{}, "
      "attrs:{}, last_offset_delta:{}, first_timestamp:{}, "
      "max_timestamp:{}, producer_id:{}, producer_epoch:{}, "
      "base_sequence:{}, record_count:{}, ctx:{{term:{}, owner_shard:",
      header_crc,
      size_bytes,
      base_offset,
      type,
      crc,
      attrs,
      last_offset_delta,
      first_timestamp,
      max_timestamp,
      producer_id,
      producer_epoch,
      base_sequence,
      record_count,
      ctx.term);
    if (ctx.owner_shard) {
        it = fmt::format_to(it, "{}}}}}", *ctx.owner_shard);
    } else {
        it = fmt::format_to(it, "nullopt}}}}");
    }
    return it;
}

fmt::iterator record_batch::format_to(fmt::iterator it) const {
    it = fmt::format_to(it, "{{record_batch={}, records=", _header);
    if (_compressed) {
        it = fmt::format_to(
          it, "{{compressed={} bytes}}", _records.size_bytes());
    } else {
        it = fmt::format_to(it, "{{");
        for_each_record(
          [&it](const model::record& r) { it = fmt::format_to(it, "{}", r); });
        it = fmt::format_to(it, "}}");
    }
    return fmt::format_to(it, "}}");
}

ss::sstring ntp::path() const {
    return ssx::sformat("{}/{}/{}", ns(), tp.topic(), tp.partition());
}

ss::sstring topic_namespace::path() const {
    return ssx::sformat("{}/{}", ns(), tp());
}

std::filesystem::path ntp::topic_path() const {
    return fmt::format("{}/{}", ns(), tp.topic());
}

std::istream& operator>>(std::istream& i, compression& c) {
    ss::sstring s;
    i >> s;
    auto tmp = string_switch<std::optional<compression>>(s)
                 .match_all("none", "uncompressed", compression::none)
                 .match("gzip", compression::gzip)
                 .match("snappy", compression::snappy)
                 .match("lz4", compression::lz4)
                 .match("zstd", compression::zstd)
                 .match("producer", compression::producer)
                 .default_match(std::nullopt);

    if (tmp.has_value()) {
        c = tmp.value();
    } else {
        i.setstate(std::ios_base::failbit);
    }

    return i;
}

fmt::iterator broker_properties::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{cores {}, mem_available {}, disk_available {}, in_fips_mode {}}}",
      cores,
      available_memory_bytes,
      available_disk_gb,
      in_fips_mode);
}

fmt::iterator broker::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{id: {}, kafka_advertised_listeners: {}, rpc_address: {}, rack: {}, "
      "properties: {}}}",
      _id,
      _kafka_advertised_listeners,
      _rpc_address,
      _rack,
      _properties);
}

fmt::iterator topic_metadata::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{topic_namespace: {}, partitons: {}}}", tp_ns, partitions);
}

fmt::iterator partition_metadata::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{id: {}, leader_id: {}, replicas: {}}}", id, leader_node, replicas);
}

fmt::iterator broker_shard::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{node_id: {}, shard: {}}}", node_id, shard);
}

fmt::iterator format_to(compaction_strategy c, fmt::iterator out) {
    switch (c) {
    case compaction_strategy::offset:
        return fmt::format_to(out, "offset");
    case compaction_strategy::timestamp:
        return fmt::format_to(out, "timestamp");
    case compaction_strategy::header:
        return fmt::format_to(out, "header");
    }
    __builtin_unreachable();
}

std::istream& operator>>(std::istream& i, compaction_strategy& cs) {
    ss::sstring s;
    i >> s;
    cs = string_switch<compaction_strategy>(s)
           .match("offset", compaction_strategy::offset)
           .match("header", compaction_strategy::header)
           .match("timestamp", compaction_strategy::timestamp);
    return i;
};

std::istream& operator>>(std::istream& i, timestamp_type& ts_type) {
    ss::sstring s;
    i >> s;
    ts_type = string_switch<timestamp_type>(s)
                .match("LogAppendTime", timestamp_type::append_time)
                .match("CreateTime", timestamp_type::create_time);
    return i;
};

fmt::iterator format_to(cleanup_policy_bitflags c, fmt::iterator out) {
    return fmt::format_to(out, "{}", to_string_view(c));
}

std::istream& operator>>(std::istream& i, cleanup_policy_bitflags& cp) {
    ss::sstring s;
    i >> s;
    cp = string_switch<cleanup_policy_bitflags>(s)
           .match("delete", cleanup_policy_bitflags::deletion)
           .match("compact", cleanup_policy_bitflags::compaction)
           .match_all(
             "compact,delete",
             "delete,compact",
             cleanup_policy_bitflags::deletion
               | cleanup_policy_bitflags::compaction);
    return i;
}

fmt::iterator broker_endpoint::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{{}:{}}}", name, address);
}

fmt::iterator format_to(record_batch_type bt, fmt::iterator out) {
    switch (bt) {
    case record_batch_type::raft_data:
        return fmt::format_to(out, "batch_type::raft_data");
    case record_batch_type::raft_configuration:
        return fmt::format_to(out, "batch_type::raft_configuration");
    case record_batch_type::controller:
        return fmt::format_to(out, "batch_type::controller");
    case record_batch_type::kvstore:
        return fmt::format_to(out, "batch_type::kvstore");
    case record_batch_type::checkpoint:
        return fmt::format_to(out, "batch_type::checkpoint");
    case record_batch_type::topic_management_cmd:
        return fmt::format_to(out, "batch_type::topic_management_cmd");
    case record_batch_type::ghost_batch:
        return fmt::format_to(out, "batch_type::ghost_batch");
    case record_batch_type::id_allocator:
        return fmt::format_to(out, "batch_type::id_allocator");
    case record_batch_type::tx_prepare:
        return fmt::format_to(out, "batch_type::tx_prepare");
    case record_batch_type::tx_fence:
        return fmt::format_to(out, "batch_type::tx_fence");
    case record_batch_type::tm_update:
        return fmt::format_to(out, "batch_type::tm_update");
    case record_batch_type::user_management_cmd:
        return fmt::format_to(out, "batch_type::user_management_cmd");
    case record_batch_type::acl_management_cmd:
        return fmt::format_to(out, "batch_type::acl_management_cmd");
    case record_batch_type::group_prepare_tx:
        return fmt::format_to(out, "batch_type::group_prepare_tx");
    case record_batch_type::group_commit_tx:
        return fmt::format_to(out, "batch_type::group_commit_tx");
    case record_batch_type::group_abort_tx:
        return fmt::format_to(out, "batch_type::group_abort_tx");
    case record_batch_type::node_management_cmd:
        return fmt::format_to(out, "batch_type::node_management_cmd");
    case record_batch_type::data_policy_management_cmd:
        return fmt::format_to(out, "batch_type::data_policy_management_cmd");
    case record_batch_type::archival_metadata:
        return fmt::format_to(out, "batch_type::archival_metadata");
    case record_batch_type::cluster_config_cmd:
        return fmt::format_to(out, "batch_type::cluster_config_cmd");
    case record_batch_type::feature_update:
        return fmt::format_to(out, "batch_type::feature_update");
    case record_batch_type::cluster_bootstrap_cmd:
        return fmt::format_to(out, "batch_type::cluster_bootstrap_cmd");
    case record_batch_type::version_fence:
        return fmt::format_to(out, "batch_type::version_fence");
    case record_batch_type::tx_tm_hosted_trasactions:
        return fmt::format_to(out, "batch_type::tx_tm_hosted_trasactions");
    case record_batch_type::prefix_truncate:
        return fmt::format_to(out, "batch_type::prefix_truncate");
    case record_batch_type::tx_registry:
        return fmt::format_to(out, "batch_type::tx_registry");
    case record_batch_type::cluster_recovery_cmd:
        return fmt::format_to(out, "batch_type::cluster_recovery_cmd");
    case record_batch_type::compaction_placeholder:
        return fmt::format_to(out, "batch_type::compaction_placeholder");
    case record_batch_type::role_management_cmd:
        return fmt::format_to(out, "batch_type::role_management_cmd");
    case record_batch_type::client_quota:
        return fmt::format_to(out, "batch_type::client_quota");
    case record_batch_type::data_migration_cmd:
        return fmt::format_to(out, "batch_type::data_migration_cmd");
    case record_batch_type::group_fence_tx:
        return fmt::format_to(out, "batch_type::group_fence_tx");
    case record_batch_type::partition_properties_update:
        return fmt::format_to(out, "batch_type::partition_properties_update");
    case record_batch_type::group_block:
        return fmt::format_to(out, "group_block");
    }
    return fmt::format_to(
      out, "batch_type::unknown{{{}}}", static_cast<int>(bt));
}

fmt::iterator format_to(membership_state st, fmt::iterator out) {
    switch (st) {
    case membership_state::active:
        return fmt::format_to(out, "active");
    case membership_state::draining:
        return fmt::format_to(out, "draining");
    case membership_state::removed:
        return fmt::format_to(out, "removed");
    }
    return fmt::format_to(
      out, "unknown membership state {{{}}}", static_cast<int>(st));
}

fmt::iterator format_to(maintenance_state st, fmt::iterator out) {
    switch (st) {
    case maintenance_state::active:
        return fmt::format_to(out, "active");
    case maintenance_state::inactive:
        return fmt::format_to(out, "inactive");
    }
    __builtin_unreachable();
}

fmt::iterator format_to(control_record_type crt, fmt::iterator out) {
    switch (crt) {
    case control_record_type::tx_abort:
        return fmt::format_to(out, "tx_abort");
    case control_record_type::tx_commit:
        return fmt::format_to(out, "tx_commit");
    case control_record_type::unknown:
        return fmt::format_to(out, "unknown");
    }
    return fmt::format_to(out, "unknown");
}

fmt::iterator batch_identity::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{pid: {}, first_seq: {}, is_transactional: {}, record_count: {}, "
      "last_seq: {}}}",
      pid,
      first_seq,
      is_transactional,
      record_count,
      last_seq);
}

fmt::iterator format_to(fetch_read_strategy s, fmt::iterator out) {
    return fmt::format_to(out, "{}", fetch_read_strategy_to_string(s));
}

std::istream& operator>>(std::istream& i, fetch_read_strategy& strat) {
    ss::sstring s;
    i >> s;
    strat = string_switch<fetch_read_strategy>(s)
              .match(
                fetch_read_strategy_to_string(fetch_read_strategy::polling),
                fetch_read_strategy::polling)
              .match(
                fetch_read_strategy_to_string(fetch_read_strategy::non_polling),
                fetch_read_strategy::non_polling)
              .match(
                fetch_read_strategy_to_string(
                  fetch_read_strategy::non_polling_with_debounce),
                fetch_read_strategy::non_polling_with_debounce)
              .match(
                fetch_read_strategy_to_string(
                  fetch_read_strategy::non_polling_with_pid),
                fetch_read_strategy::non_polling_with_pid);
    return i;
}

fmt::iterator format_to(write_caching_mode mode, fmt::iterator out) {
    return fmt::format_to(out, "{}", write_caching_mode_to_string(mode));
}

std::istream& operator>>(std::istream& i, write_caching_mode& mode) {
    ss::sstring s;
    i >> s;
    auto value = write_caching_mode_from_string(s);
    if (!value) {
        i.setstate(std::ios::failbit);
        return i;
    }
    mode = *value;
    return i;
}

std::optional<write_caching_mode>
write_caching_mode_from_string(std::string_view s) {
    return string_switch<std::optional<write_caching_mode>>(s)
      .match(
        model::write_caching_mode_to_string(
          model::write_caching_mode::default_true),
        model::write_caching_mode::default_true)
      .match(
        model::write_caching_mode_to_string(
          model::write_caching_mode::default_false),
        model::write_caching_mode::default_false)
      .match(
        model::write_caching_mode_to_string(
          model::write_caching_mode::disabled),
        model::write_caching_mode::disabled)
      .default_match(std::nullopt);
}

fmt::iterator format_to(fips_mode_flag f, fmt::iterator out) {
    return fmt::format_to(out, "{}", to_string_view(f));
}

std::istream& operator>>(std::istream& is, fips_mode_flag& f) {
    ss::sstring s;
    is >> s;
    f = string_switch<fips_mode_flag>(s)
          .match(
            to_string_view(fips_mode_flag::disabled), fips_mode_flag::disabled)
          .match(
            to_string_view(fips_mode_flag::enabled), fips_mode_flag::enabled)
          .match(
            to_string_view(fips_mode_flag::permissive),
            fips_mode_flag::permissive);
    return is;
}

fmt::iterator topic_id::format_to(fmt::iterator it) const {
    const auto& uuid = (*this)().uuid();
    const bytes_view bv{uuid.begin(), uuid.size()};
    return fmt::format_to(it, "{}", bytes_to_base64(bv));
}

topic_id_partition topic_id_partition::from(std::string_view s) {
    std::vector<ss::sstring> ss = absl::StrSplit(s, "/");
    if (ss.size() != 2) {
        throw std::runtime_error(
          fmt::format("Invalid topic_id_partition: {}", s));
    }
    auto tid = uuid_t::from_string(ss[0]);
    int p{0};
    if (!absl::SimpleAtoi(ss[1].data(), &p)) {
        throw std::runtime_error(
          fmt::format("Invalid topic_id_partition: {}", s));
    }
    return model::topic_id_partition(
      model::topic_id(tid), model::partition_id(p));
}

std::optional<kafka_batch_validation_mode>
kafka_batch_validation_mode_from_string(std::string_view s) {
    return string_switch<std::optional<kafka_batch_validation_mode>>(s)
      .match(
        model::kafka_batch_validation_mode_to_string(
          model::kafka_batch_validation_mode::legacy),
        model::kafka_batch_validation_mode::legacy)
      .match(
        model::kafka_batch_validation_mode_to_string(
          model::kafka_batch_validation_mode::relaxed),
        model::kafka_batch_validation_mode::relaxed)
      .match(
        model::kafka_batch_validation_mode_to_string(
          model::kafka_batch_validation_mode::strict),
        model::kafka_batch_validation_mode::strict)
      .default_match(std::nullopt);
}

fmt::iterator format_to(kafka_batch_validation_mode mode, fmt::iterator out) {
    return fmt::format_to(
      out, "{}", kafka_batch_validation_mode_to_string(mode));
}

std::istream& operator>>(std::istream& i, kafka_batch_validation_mode& mode) {
    ss::sstring s;
    i >> s;
    auto value = kafka_batch_validation_mode_from_string(s);
    if (!value) {
        i.setstate(std::ios::failbit);
        return i;
    }
    mode = *value;
    return i;
}

fmt::iterator format_to(isolation_level l, fmt::iterator out) {
    switch (l) {
    case isolation_level::read_uncommitted:
        return fmt::format_to(out, "read_uncommitted");
    case isolation_level::read_committed:
        return fmt::format_to(out, "read_committed");
    }
    return fmt::format_to(out, "unknown");
}

} // namespace model
