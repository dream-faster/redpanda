/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "resource_mgmt/memory_groups.h"

#include "base/seastarx.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "utils/human.h"

#include <seastar/core/memory.hh>

namespace {

bool sliding_window_compaction_enabled() {
    return config::shard_local_cfg().log_compaction_use_sliding_window();
}

struct memory_shares {
    constexpr static size_t chunk_cache = 15;
    constexpr static size_t kafka = 30;
    constexpr static size_t rpc = 20;
    constexpr static size_t recovery = 10;
    constexpr static size_t tiered_storage = 10;
    constexpr static size_t admin = 2;

    static size_t total_shares() {
        return chunk_cache + kafka + rpc + recovery + tiered_storage + admin;
    }
};

} // namespace

size_t
compaction_memory_reservation::reserved_bytes(size_t total_memory) const {
    size_t bytes_limit = total_memory * (max_limit_pct / 100.0);
    return std::min(max_bytes, bytes_limit);
}

size_t
partitions_memory_reservation::reserved_bytes(size_t total_memory) const {
    return total_memory * (max_limit_pct / 100.0);
}

system_memory_groups::system_memory_groups(
  size_t total_available_memory,
  compaction_memory_reservation compaction,
  partitions_memory_reservation partitions)
  : _compaction_reserved_memory(
      compaction.reserved_bytes(total_available_memory))
  , _partitions_reserved_memory(
      partitions.reserved_bytes(total_available_memory))
  , _total_available_memory(total_available_memory) {}

size_t system_memory_groups::chunk_cache_min_memory() const {
    return chunk_cache_max_memory() / 3;
}

size_t system_memory_groups::chunk_cache_max_memory() const {
    return subsystem_memory<memory_shares::chunk_cache>();
}

size_t system_memory_groups::kafka_total_memory() const {
    return subsystem_memory<memory_shares::kafka>();
}

size_t system_memory_groups::rpc_total_memory() const {
    return subsystem_memory<memory_shares::rpc>();
}

size_t system_memory_groups::recovery_max_memory() const {
    return subsystem_memory<memory_shares::recovery>();
}

size_t system_memory_groups::tiered_storage_max_memory() const {
    return subsystem_memory<memory_shares::tiered_storage>();
}

size_t system_memory_groups::admin_max_memory() const {
    return subsystem_memory<memory_shares::admin>();
}

size_t system_memory_groups::partitions_max_memory() const {
    return _partitions_reserved_memory;
}

size_t system_memory_groups::total_reserved_memory() const {
    return _compaction_reserved_memory + _partitions_reserved_memory;
}

double system_memory_groups::partitions_max_memory_share() const {
    return _partitions_reserved_memory
           / static_cast<double>(ss::memory::stats().total_memory());
}

template<size_t shares>
size_t system_memory_groups::subsystem_memory() const {
    size_t per_share_amount = total_memory() / memory_shares::total_shares();
    return per_share_amount * shares;
}

size_t system_memory_groups::total_memory() const {
    size_t reserved_memory = total_reserved_memory();
    size_t available_memory = _total_available_memory;
    if (reserved_memory > available_memory) {
        dassert(
          false,
          "Reserved memory {} is greater than the memory available to the "
          "system {}",
          human::bytes(reserved_memory),
          human::bytes(available_memory));
        return 0;
    }
    return available_memory - reserved_memory;
}

void system_memory_groups::log_memory_group_allocations(seastar::logger& log) {
    vlog(
      log.info,
      "Per shard memory group allocations: total memory: {}, reserved memory: "
      "{}, total memory minus pre-share reservations: {}, chunk cache: {}, "
      "kafka: {}, rpc: {}, recovery: {}, tiered storage: {}, admin: {}, "
      "compaction: {}, partitions: {}",
      human::bytes(ss::memory::stats().total_memory()),
      human::bytes(total_reserved_memory()),
      human::bytes(total_memory()),
      human::bytes(chunk_cache_max_memory()),
      human::bytes(kafka_total_memory()),
      human::bytes(rpc_total_memory()),
      human::bytes(recovery_max_memory()),
      human::bytes(tiered_storage_max_memory()),
      human::bytes(admin_max_memory()),
      human::bytes(compaction_reserved_memory()),
      human::bytes(partitions_max_memory()));
}

std::optional<system_memory_groups>& memory_groups_holder() {
    static thread_local std::optional<system_memory_groups> groups;
    return groups;
}

system_memory_groups& memory_groups() {
    auto& groups = memory_groups_holder();
    if (groups) {
        return *groups;
    }

    auto sliding_window_compaction = sliding_window_compaction_enabled();

    const auto& cfg = config::shard_local_cfg();
    compaction_memory_reservation compaction{
      .max_bytes = sliding_window_compaction
                     ? cfg.storage_compaction_key_map_memory.value()
                     : 0,
      .max_limit_pct
      = cfg.storage_compaction_key_map_memory_limit_percent.value()};
    partitions_memory_reservation partitions{
      .max_limit_pct = cfg.topic_partitions_memory_allocation_percent()};
    groups.emplace(ss::memory::stats().total_memory(), compaction, partitions);
    return *groups;
}
