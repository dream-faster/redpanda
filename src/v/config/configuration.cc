// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/configuration.h"

#include "absl/strings/ascii.h"
#include "base/units.h"
#include "cluster/scheduling/topic_memory_per_partition_default.h"
#include "config/base_property.h"
#include "config/bounded_property.h"
#include "config/sasl_mechanisms.h"
#include "config/types.h"
#include "config/validators.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "net/tls.h"
#include "security/config.h"
#include "security/oidc_url_parser.h"
#include "serde/rw/chrono.h"
#include "ssx/sformat.h"
#include "storage/config.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>

#include <fmt/ranges.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <optional>

namespace config {
using namespace std::chrono_literals;

configuration::configuration()
  : log_segment_size(
      *this,
      "log_segment_size",
      "Default log segment size in bytes for topics which do not set "
      "segment.bytes",
      {.needs_restart = needs_restart::no,
       .example = "2147483648",
       .visibility = visibility::tunable},
      128_MiB,
      {.min = 1_MiB})
  , log_segment_size_min(
      *this,
      "log_segment_size_min",
      "Lower bound on topic `segment.bytes`: lower values will be clamped to "
      "this limit.",
      {.needs_restart = needs_restart::no,
       .example = "16777216",
       .visibility = visibility::tunable},
      1_MiB)
  , log_segment_size_max(
      *this,
      "log_segment_size_max",
      "Upper bound on topic `segment.bytes`: higher values will be clamped to "
      "this limit.",
      {.needs_restart = needs_restart::no,
       .example = "268435456",
       .visibility = visibility::tunable},
      std::nullopt)
  , log_segment_size_jitter_percent(
      *this,
      "log_segment_size_jitter_percent",
      "Random variation to the segment size limit used for each partition.",
      {.needs_restart = needs_restart::yes,
       .example = "2",
       .visibility = visibility::tunable},
      5,
      {.min = 0, .max = 99})
  , compacted_log_segment_size(
      *this,
      "compacted_log_segment_size",
      "Size (in bytes) for each compacted log segment.",
      {.needs_restart = needs_restart::no,
       .example = "268435456",
       .visibility = visibility::tunable},
      256_MiB,
      {.min = 1_MiB})
  , readers_cache_eviction_timeout_ms(
      *this,
      "readers_cache_eviction_timeout_ms",
      "Duration after which inactive readers are evicted from cache.",
      {.visibility = visibility::tunable},
      30s)
  , readers_cache_target_max_size(
      *this,
      "readers_cache_target_max_size",
      "Maximum desired number of readers cached per NTP. This a soft limit, "
      "meaning that a number of readers in cache may temporarily increase as "
      "cleanup is performed in the background.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      200,
      {.min = 0, .max = 10000})
  , log_segment_ms(
      *this,
      "log_segment_ms",
      "Default lifetime of log segments. If `null`, the property is disabled, "
      "and no default lifetime is set. Any value under 60 seconds (60000 ms) "
      "is rejected. This property can also be set in the Kafka API using the "
      "Kafka-compatible alias, `log.roll.ms`. The topic property `segment.ms` "
      "overrides the value of `log_segment_ms` at the topic level.",
      {.needs_restart = needs_restart::no,
       .example = "3600000",
       .visibility = visibility::user},
      std::chrono::weeks{2},
      {.min = 60s})
  , log_segment_ms_min(
      *this,
      "log_segment_ms_min",
      "Lower bound on topic `segment.ms`: lower values will be clamped to this "
      "value.",
      {.needs_restart = needs_restart::no,
       .example = "600000",
       .visibility = visibility::tunable},
      10min,
      {.min = 0ms})
  , log_segment_ms_max(
      *this,
      "log_segment_ms_max",
      "Upper bound on topic `segment.ms`: higher values will be clamped to "
      "this value.",
      {.needs_restart = needs_restart::no,
       .example = "31536000000",
       .visibility = visibility::tunable},
      24h * 365,
      {.min = 0ms})
  , rpc_server_listen_backlog(
      *this,
      "rpc_server_listen_backlog",
      "Maximum TCP connection queue length for Kafka server and internal RPC "
      "server. If `null` (the default value), no queue length is set.",
      {.visibility = visibility::user},
      std::nullopt,
      {.min = 1})
  , rpc_server_tcp_recv_buf(
      *this,
      "rpc_server_tcp_recv_buf",
      "Internal RPC TCP receive buffer size. If `null` (the default value), no "
      "buffer size is set by Redpanda.",
      {.example = "65536"},
      std::nullopt,
      {.min = 32_KiB, .align = 4_KiB})
  , rpc_server_tcp_send_buf(
      *this,
      "rpc_server_tcp_send_buf",
      "Internal RPC TCP send buffer size. If `null` (the default value), then "
      "no buffer size is set by Redpanda.",
      {.example = "65536"},
      std::nullopt,
      {.min = 32_KiB, .align = 4_KiB})
  , rpc_client_connections_per_peer(
      *this,
      "rpc_client_connections_per_peer",
      "The maximum number of connections a broker will open to each of its "
      "peers.",
      {.example = "8"},
      128,
      {.min = 8})
  , rpc_server_compress_replies(
      *this,
      "rpc_server_compress_replies",
      "Enable compression for internal RPC (remote procedure call) server "
      "replies.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      false)
  , topic_memory_per_partition(
      *this,
      "topic_memory_per_partition",
      "Required memory in bytes per partition replica when creating or "
      "altering topics. The total size of the memory pool for partitions is "
      "the total memory available to Redpanda times "
      "topic_partitions_memory_allocation_percent, and then each partition "
      "created requires "
      "topic_memory_per_partition bytes from that pool. If insufficent memory "
      "is available, topic creation or alternation will fail.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      cluster::DEFAULT_TOPIC_MEMORY_PER_PARTITION,
      {
        .min = 1,      // Must be nonzero, it's a divisor
        .max = 100_MiB // Rough 'sanity' limit: a machine with 1GB RAM must be
                       // able to create at least 10 partitions
      })
  , topic_fds_per_partition(
      *this,
      "topic_fds_per_partition",
      "File descriptors required per partition replica: topic creation is "
      "prevented if it would result in the ratio of file descriptor limit to "
      "partition replicas being lower than this value.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      5,
      {
        .min = 1,   // At least one FD per partition, required for appender.
        .max = 1000 // A system with 1M ulimit should be allowed to create at
                    // least 1000 partitions
      })
  , topic_partitions_per_shard(
      *this,
      "topic_partitions_per_shard",
      "Maximum partition replicas per shard: topic creation is prevented if "
      "it would result in the ratio of partition replicas to shards being "
      "higher than this value.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      // default value must be synced with
      // scale_parameters.py::DEFAULT_PARTITIONS_PER_SHARD
      5000,
      {
        .min = 16,    // Forbid absurdly small values that would prevent most
                      // practical workloads from running
        .max = 131072 // An upper bound to prevent pathological values, although
                      // systems will most likely hit issues far before reaching
                      // this.  This property is principally intended to be
                      // tuned downward from the default, not upward.
      },
      legacy_default<uint32_t>(7000, legacy_version{9}))
  , topic_partitions_reserve_shard0(
      *this,
      "topic_partitions_reserve_shard0",
      "Reserved partition slots on shard (CPU core) 0 on each node.  If this "
      "is >= topic_partitions_per_shard, no data partitions will be scheduled "
      "on shard 0",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      0,
      {
        .min = 0,     // It is not mandatory to reserve any capacity
        .max = 131072 // Same max as topic_partitions_per_shard
      })
  , topic_partitions_memory_allocation_percent(
      *this,
      "topic_partitions_memory_allocation_percent",
      "Percentage of total memory to reserve for topic partitions. See "
      "topic_memory_per_partition for details.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      // default value must be synced with
      // scale_parameters.py::DEFAULT_PARTITIONS_MEMORY_ALLOCATION_PERCENT
      10,
      {
        .min = 1,
        .max = 80,
      })
  , partition_manager_shutdown_watchdog_timeout(
      *this,
      "partition_manager_shutdown_watchdog_timeout",
      "A threshold value to detect partitions which might have been stuck "
      "while shutting down. After this threshold, a watchdog in partition "
      "manager will log information about partition shutdown not making "
      "progress.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      30s)
  , topic_label_aggregation_limit(
      *this,
      "topic_label_aggregation_limit",
      "When the number of topics exceeds this limit, the topic label in "
      "generated metrics will be aggregated. If `nullopt`, then there is no "
      "limit.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt)
  , controller_backend_reconciliation_concurrency(
      *this,
      "controller_backend_reconciliation_concurrency",
      "Maximum concurrent reconciliation operations the controller can run. "
      "Higher values can speed up cluster state changes but use more "
      "resources.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1024u,
      {.min = 1u, .max = 2048u})
  , admin_api_require_auth(
      *this,
      "admin_api_require_auth",
      "Whether Admin API clients must provide HTTP basic authentication "
      "headers.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , raft_heartbeat_interval_ms(
      *this,
      "raft_heartbeat_interval_ms",
      "Number of milliseconds for Raft leader heartbeats.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::chrono::milliseconds(150),
      {.min = std::chrono::milliseconds(1)})
  , raft_heartbeat_timeout_ms(
      *this,
      "raft_heartbeat_timeout_ms",
      "Raft heartbeat RPC (remote procedure call) timeout. Raft uses a "
      "heartbeat mechanism to maintain leadership authority and to trigger "
      "leader elections. The `raft_heartbeat_interval_ms` is a periodic "
      "heartbeat sent by the partition leader to all followers to declare its "
      "leadership. If a follower does not receive a heartbeat within the "
      "`raft_heartbeat_timeout_ms`, then it triggers an election to choose a "
      "new partition leader.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      3s,
      {.min = std::chrono::milliseconds(1)})
  , raft_heartbeat_disconnect_failures(
      *this,
      "raft_heartbeat_disconnect_failures",
      "The number of failed heartbeats after which an unresponsive TCP "
      "connection is forcibly closed. To disable forced disconnection, set to "
      "0.",
      {.visibility = visibility::tunable},
      3)

  , raft_max_recovery_memory(
      *this,
      "raft_max_recovery_memory",
      "Maximum memory that can be used for reads in Raft recovery process by "
      "default 15% of total memory.",
      {.needs_restart = needs_restart::no,
       .example = "41943040",
       .visibility = visibility::tunable},
      std::nullopt,
      {.min = 32_MiB})
  , raft_enable_lw_heartbeat(
      *this,
      "raft_enable_lw_heartbeat",
      "Enables Raft optimization of heartbeats.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      true)
  , raft_recovery_concurrency_per_shard(
      *this,
      "raft_recovery_concurrency_per_shard",
      "Number of partitions that may simultaneously recover data to a "
      "particular shard. This number is limited to avoid overwhelming nodes "
      "when they come back online after an outage.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      64,
      {.min = 1, .max = 16384})
  , raft_replica_max_pending_flush_bytes(
      *this,
      "raft_replica_max_pending_flush_bytes",
      "Maximum number of bytes that are not flushed per partition. If the "
      "configured threshold is reached, the log is automatically flushed even "
      "if it has not been explicitly requested.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      256_KiB)
  , raft_flush_timer_interval_ms(
      *this,
      "raft_flush_timer_interval_ms",
      "Interval of checking partition against the "
      "`raft_replica_max_pending_flush_bytes`, deprecated started 24.1, use "
      "raft_replica_max_flush_delay_ms instead ",
      {.visibility = visibility::deprecated},
      100ms)
  , raft_replica_max_flush_delay_ms(
      *this,
      "raft_replica_max_flush_delay_ms",
      "Maximum delay between two subsequent flushes. After this delay, the log "
      "is automatically force flushed.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      100ms,
      [](const auto& v) -> std::optional<ss::sstring> {
          // maximum duration imposed by serde serialization.
          if (v < 1ms || v > serde::max_serializable_ms) {
              return fmt::format(
                "flush delay should be in range: [1, {}]",
                serde::max_serializable_ms);
          }
          return std::nullopt;
      })
  , raft_enable_longest_log_detection(
      *this,
      "raft_enable_longest_log_detection",
      "Enables an additional step in leader election where a candidate is "
      "allowed to wait for all the replies from the broker it requested votes "
      "from. This may introduce a small delay when recovering from failure, "
      "but it prevents truncation if any of the replicas have more data than "
      "the majority.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      true)
  , raft_max_inflight_follower_append_entries_requests_per_shard(
      *this,
      "raft_max_inflight_follower_append_entries_requests_per_shard",
      "The maximum number of append entry requests that may be sent from raft "
      "groups on a Seastar shard to the current node and are awaiting a reply. "
      "This property replaces "
      "raft_max_concurrent_append_requests_per_follower.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1024,
      {.min = 1, .max = 50000})
  , raft_max_buffered_follower_append_entries_bytes_per_shard(
      *this,
      "raft_max_buffered_follower_append_entries_bytes_per_shard",
      "The total size of append entry requests that may be cached per shard, "
      "using Raft buffered protocol. When an entry is cached the leader can "
      "continue serving requests because the ordering of the cached requests "
      "cannot change. When the total size of cached requests reaches the set "
      "limit, back pressure is applied to throttle producers.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      0,
      {.min = 0, .max = 100_MiB})
  , enable_usage(
      *this,
      "enable_usage",
      "Enables the usage tracking mechanism, storing windowed history of "
      "kafka/cloud_storage metrics over time.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , usage_num_windows(
      *this,
      "usage_num_windows",
      "The number of windows to persist in memory and disk.",
      {.needs_restart = needs_restart::no,
       .example = "24",
       .visibility = visibility::tunable},
      24,
      {.min = 2, .max = 86400})
  , usage_window_width_interval_sec(
      *this,
      "usage_window_width_interval_sec",
      "The width of a usage window, tracking cloud and kafka ingress/egress "
      "traffic each interval.",
      {.needs_restart = needs_restart::no,
       .example = "3600",
       .visibility = visibility::tunable},
      std::chrono::seconds(3600),
      {.min = std::chrono::seconds(1)})
  , usage_disk_persistance_interval_sec(
      *this,
      "usage_disk_persistance_interval_sec",
      "The interval in which all usage stats are written to disk.",
      {.needs_restart = needs_restart::no,
       .example = "300",
       .visibility = visibility::tunable},
      std::chrono::seconds(60 * 5),
      {.min = std::chrono::seconds(1)})
  , default_num_windows(
      *this,
      "default_num_windows",
      "Default number of quota tracking windows.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10,
      {.min = 1})
  , default_window_sec(
      *this,
      "default_window_sec",
      "Default quota tracking window size in milliseconds.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::chrono::milliseconds(1000),
      {.min = std::chrono::milliseconds(1)})
  , quota_manager_gc_sec(
      *this,
      "quota_manager_gc_sec",
      "Quota manager GC frequency in milliseconds.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::chrono::milliseconds(30000))
  , cluster_id(
      *this,
      "cluster_id",
      "Cluster identifier.",
      {.needs_restart = needs_restart::no, .gets_restored = gets_restored::no},
      std::nullopt,
      &validate_non_empty_string_opt)
  , disable_metrics(
      *this,
      "disable_metrics",
      "Disable registering the metrics exposed on the internal `/metrics` "
      "endpoint.",
      base_property::metadata{.usable_before_ready = usable_before_ready::yes},
      false)
  , disable_public_metrics(
      *this,
      "disable_public_metrics",
      "Disable registering the metrics exposed on the `/public_metrics` "
      "endpoint.",
      base_property::metadata{.usable_before_ready = usable_before_ready::yes},
      false)
  , enable_development_metrics(
      *this,
      "enable_development_metrics",
      "Enable registering development-oriented metrics on the internal "
      "`/metrics` endpoint, such as the per-method internal RPC latency "
      "histograms.",
      {.needs_restart = needs_restart::no},
      false)
  , aggregate_metrics(
      *this,
      "aggregate_metrics",
      "Enable aggregation of metrics returned by the `/metrics` endpoint. "
      "Aggregation can simplify monitoring by providing summarized data "
      "instead of raw, per-instance metrics. Metric aggregation is performed "
      "by summing the values of samples by labels and is done when it makes "
      "sense by the shard and/or partition labels.",
      {.needs_restart = needs_restart::no,
       .usable_before_ready = usable_before_ready::yes},
      false)
  , enable_consumer_group_metrics(
      *this,
      "enable_consumer_group_metrics",
      "List of enabled consumer group metrics. Accepted "
      "Values: `group`, `partition`, `consumer_lag`",
      {.needs_restart = needs_restart::no},
      std::vector<ss::sstring>{"group", "partition"},
      validate_consumer_group_metrics)
  , consumer_group_lag_collection_interval(
      *this,
      "consumer_group_lag_collection_interval_sec",
      "How often Redpanda runs the collection loop when "
      "`enable_consumer_group_metrics` is set to `consumer_lag`. Updates will "
      "not be more frequent than `health_monitor_max_metadata_age`.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      60s)
  , group_min_session_timeout_ms(
      *this,
      "group_min_session_timeout_ms",
      "The minimum allowed session timeout for registered consumers. Shorter "
      "timeouts result in quicker failure detection at the cost of more "
      "frequent consumer heartbeating, which can overwhelm broker resources.",
      {.needs_restart = needs_restart::no},
      6000ms)
  , group_max_session_timeout_ms(
      *this,
      "group_max_session_timeout_ms",
      "The maximum allowed session timeout for registered consumers. Longer "
      "timeouts give consumers more time to process messages in between "
      "heartbeats at the cost of a longer time to detect failures.",
      {.needs_restart = needs_restart::no},
      300s)
  , group_initial_rebalance_delay(
      *this,
      "group_initial_rebalance_delay",
      "Delay added to the rebalance phase to wait for new members.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      3s)
  , group_new_member_join_timeout(
      *this,
      "group_new_member_join_timeout",
      "Timeout for new member joins.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      30'000ms)
  , group_offset_retention_sec(
      *this,
      "group_offset_retention_sec",
      "Consumer group offset retention seconds. To disable offset retention, "
      "set this to null.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      24h * 7)
  , group_offset_retention_check_ms(
      *this,
      "group_offset_retention_check_ms",
      "Frequency rate at which the system should check for expired group "
      "offsets.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10min)
  , legacy_group_offset_retention_enabled(
      *this,
      "legacy_group_offset_retention_enabled",
      "Group offset retention is enabled by default starting in Redpanda "
      "version 23.1. To enable offset retention after upgrading from an older "
      "version, set this option to true.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      false)
  , metadata_dissemination_interval_ms(
      *this,
      "metadata_dissemination_interval_ms",
      "Interval for metadata dissemination batching.",
      {.example = "5000", .visibility = visibility::tunable},
      3'000ms)
  , metadata_dissemination_retry_delay_ms(
      *this,
      "metadata_dissemination_retry_delay_ms",
      "Delay before retrying a topic lookup in a shard or other meta tables.",
      {.visibility = visibility::tunable},
      0'500ms)
  , metadata_dissemination_retries(
      *this,
      "metadata_dissemination_retries",
      "Number of attempts to look up a topic's metadata-like shard before a "
      "request fails. This configuration controls the number of retries that "
      "request handlers perform when internal topic metadata (for topics like "
      "tx, consumer offsets, etc) is missing. These topics are usually created "
      "on demand when users try to use the cluster for the first time and it "
      "may take some time for the creation to happen and the metadata to "
      "propagate to all the brokers (particularly the broker handling the "
      "request). In the mean time Redpanda waits and retry. This configuration "
      "controls the number retries.",
      {.visibility = visibility::tunable},
      30)
  , tx_timeout_delay_ms(
      *this,
      "tx_timeout_delay_ms",
      "Delay before scheduling the next check for timed out transactions.",
      {.visibility = visibility::user},
      1000ms)
  , fetch_reads_debounce_timeout(
      *this,
      "fetch_reads_debounce_timeout",
      "Time to wait for the next read in fetch requests when the requested "
      "minimum bytes was not reached.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1ms)
  , kafka_fetch_request_timeout_ms(
      *this,
      "kafka_fetch_request_timeout_ms",
      "Broker-side target for the duration of a single fetch request. The "
      "broker will try to complete fetches within the specified duration, even "
      "if it means returning less bytes in the fetch than are available.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      5s)
  , enable_listoffsets_historical_leader_epoch(
      *this,
      "enable_listoffsets_historical_leader_epoch",
      "When enabled, the Kafka ListOffsets API returns the historical "
      "(record-time) leader epoch instead of the current leader epoch. "
      "Intended as a one-way opt-in: disabling after it has been enabled "
      "regresses to the original bug. "
      "Gated as a development feature: not all response paths are fixed "
      "yet (CORE-12505), so enabling this property produces internally "
      "inconsistent epoch values across paths and must not be enabled in "
      "production.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      false)
  , fetch_read_strategy(
      *this,
      "fetch_read_strategy",
      "The strategy used to fulfill fetch requests. * `polling`: If "
      "`fetch_reads_debounce_timeout` is set to its default value, then this "
      "acts exactly like `non_polling`; otherwise, it acts like "
      "`non_polling_with_debounce` (deprecated). * `non_polling`: "
      "The backend is signaled when a partition has new data, so Redpanda does "
      "not need to repeatedly read from every partition in the fetch. Redpanda "
      "Data recommends using this value for most workloads, because it can "
      "improve fetch latency and CPU utilization. * "
      "`non_polling_with_debounce`: This option behaves like `non_polling`, "
      "but it includes a debounce mechanism with a fixed delay specified by "
      "`fetch_reads_debounce_timeout` at the start of each fetch. By "
      "introducing this delay, Redpanda can accumulate more data before "
      "processing, leading to fewer fetch operations and returning larger "
      "amounts of data. Enabling this option reduces reactor utilization, but "
      "it may also increase end-to-end latency.",
      {.needs_restart = needs_restart::no,
       .example = model::fetch_read_strategy_to_string(
         model::fetch_read_strategy::non_polling),
       .visibility = visibility::tunable},
      model::fetch_read_strategy::non_polling,
      {
        model::fetch_read_strategy::polling,
        model::fetch_read_strategy::non_polling,
        model::fetch_read_strategy::non_polling_with_debounce,
        model::fetch_read_strategy::non_polling_with_pid,
      })
  , fetch_max_read_concurrency(
      *this,
      "fetch_max_read_concurrency",
      "The maximum number of concurrent partition reads per fetch request on "
      "each shard. Setting this higher than the default can lead to partition "
      "starvation and unneeded memory usage.",
      {.needs_restart = needs_restart::no,
       .example = "1",
       .visibility = visibility::tunable},
      1,
      {.min = 1, .max = 100})
  , fetch_pid_p_coeff(
      *this,
      "fetch_pid_p_coeff",
      "Proportional coefficient for fetch PID controller.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      100.0,
      {.min = 0.0, .max = std::numeric_limits<double>::max()})
  , fetch_pid_i_coeff(
      *this,
      "fetch_pid_i_coeff",
      "Integral coefficient for fetch PID controller.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      0.01,
      {.min = 0.0, .max = std::numeric_limits<double>::max()})
  , fetch_pid_d_coeff(
      *this,
      "fetch_pid_d_coeff",
      "Derivative coefficient for fetch PID controller.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      0.0,
      {.min = 0.0, .max = std::numeric_limits<double>::max()})
  , fetch_pid_target_utilization_fraction(
      *this,
      "fetch_pid_target_utilization_fraction",
      "A fraction, between 0 and 1, for the target reactor utilization of the "
      "fetch scheduling group.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      0.2,
      {.min = 0.01, .max = 1.0})
  , fetch_pid_max_debounce_ms(
      *this,
      "fetch_pid_max_debounce_ms",
      "The maximum debounce time the fetch PID controller will apply, in "
      "milliseconds.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      100ms)
  , log_cleanup_policy(
      *this,
      "log_cleanup_policy",
      "Default cleanup policy for topic logs. The topic property "
      "`cleanup.policy` overrides the value of `log_cleanup_policy` at the "
      "topic level.",
      {.needs_restart = needs_restart::no,
       .example = "compact,delete",
       .visibility = visibility::user},
      model::cleanup_policy_bitflags::deletion)
  , log_message_timestamp_type(
      *this,
      "log_message_timestamp_type",
      "Default timestamp type for topic messages (CreateTime or "
      "LogAppendTime). The topic property `message.timestamp.type` overrides "
      "the value of `log_message_timestamp_type` at the topic level.",
      {.needs_restart = needs_restart::no,
       .example = "LogAppendTime",
       .visibility = visibility::user},
      model::timestamp_type::create_time,
      {model::timestamp_type::create_time, model::timestamp_type::append_time})
  , log_message_timestamp_before_max_ms(
      *this,
      "log_message_timestamp_before_max_ms",
      "The maximum allowable timestamp difference between the broker's "
      "timestamp and a record's timestamp. For topics with "
      "`message.timestamp.type` set to `CreateTime`, Redpanda rejects records "
      "that have timestamps earlier than the broker timestamp and exceed this "
      "difference. Redpanda ignores this property for topics with "
      "`message.timestamp.type` set to `AppendTime`. The topic property "
      "`message.timestamp.before.max.ms` overrides the value of "
      "`log_message_timestamp_before_max_ms` at the topic level.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      serde::max_serializable_ms,
      {.min = 0ms, .max = serde::max_serializable_ms})
  , log_message_timestamp_after_max_ms(
      *this,
      "log_message_timestamp_after_max_ms",
      "The maximum allowable timestamp difference between the broker's "
      "timestamp and a record's timestamp. For topics with "
      "`message.timestamp.type` set to `CreateTime`, Redpanda rejects records "
      "that have timestamps later than the broker timestamp and exceed this "
      "difference. Redpanda ignores this property for topics with "
      "`message.timestamp.type` set to `AppendTime`. The topic property "
      "`message.timestamp.after.max.ms` overrides the value of "
      "`log_message_timestamp_after_max_ms` at the topic level.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      1h,
      {.min = 0ms, .max = serde::max_serializable_ms})
  , kafka_produce_batch_validation(
      *this,
      "kafka_produce_batch_validation",
      "Controls the level of validation performed on batches produced to "
      "Redpanda. When set to `legacy`, there is minimal validation performed "
      "on the produce path. When set to `relaxed`, full validation is "
      "performed on uncompressed batches and on compressed batches with the "
      "`max_timestamp` value left unset. When set to `strict`, full validation "
      "of uncompressed and compressed batches is performed. This should be the "
      "default in environments where producing clients are not trusted.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      model::kafka_batch_validation_mode::relaxed,
      {model::kafka_batch_validation_mode::legacy,
       model::kafka_batch_validation_mode::relaxed,
       model::kafka_batch_validation_mode::strict})
  , log_compression_type(
      *this,
      "log_compression_type",
      "Default topic compression type. The topic property `compression.type` "
      "overrides the value of `log_compression_type` at the topic level.",
      {.needs_restart = needs_restart::no,
       .example = "snappy",
       .visibility = visibility::user},
      model::compression::producer,
      {model::compression::none,
       model::compression::gzip,
       model::compression::snappy,
       model::compression::lz4,
       model::compression::zstd,
       model::compression::producer})
  , fetch_max_bytes(
      *this,
      "fetch_max_bytes",
      "Maximum number of bytes returned in a fetch request.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      55_MiB)
  , use_fetch_scheduler_group(
      *this,
      "use_fetch_scheduler_group",
      "Use a separate scheduler group for fetch processing.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      true)
  , kafka_fetch_read_coalescing_enabled(
      *this,
      "kafka_fetch_read_coalescing_enabled",
      "Coalesce concurrent fetches of the same partition offset into one read "
      "and serialization shared across the requesting consumers, reducing "
      "duplicate reads and fetch-response memory under high fanout.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      false)
  , use_produce_scheduler_group(
      *this,
      "use_produce_scheduler_group",
      "Use a separate scheduler group for kafka produce requests processing.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      true)
  , use_kafka_handler_scheduler_group(
      *this,
      "use_kafka_handler_scheduler_group",
      "Use separate scheduler group to handle parsing Kafka protocol requests",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      true)
  , kafka_handler_latency_all(
      *this,
      "kafka_handler_latency_all",
      "Enable latency histograms for all Kafka API handlers. When disabled, "
      "only important handlers (produce, fetch, metadata, api_versions, "
      "offset_commit) have latency histograms.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      false)
  , kafka_tcp_keepalive_idle_timeout_seconds(
      *this,
      "kafka_tcp_keepalive_timeout",
      "TCP keepalive idle timeout in seconds for Kafka connections. This "
      "describes the timeout between TCP keepalive probes that the remote site "
      "successfully acknowledged. Refers to the TCP_KEEPIDLE socket option. "
      "When changed, applies to new connections only.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      120s)
  , kafka_tcp_keepalive_probe_interval_seconds(
      *this,
      "kafka_tcp_keepalive_probe_interval_seconds",
      "TCP keepalive probe interval in seconds for Kafka connections. This "
      "describes the timeout between unacknowledged TCP keepalives. Refers to "
      "the TCP_KEEPINTVL socket option. When changed, applies to new "
      "connections only.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      60s)
  , kafka_tcp_keepalive_probes(
      *this,
      "kafka_tcp_keepalive_probes",
      "TCP keepalive unacknowledged probes until the connection is considered "
      "dead for Kafka connections. Refers to the TCP_KEEPCNT socket option. "
      "When changed, applies to new connections only.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      3)
  , kafka_connection_rate_limit(
      *this,
      "kafka_connection_rate_limit",
      "Maximum connections per second for one core. If `null` (the default), "
      "then the number of connections per second is unlimited.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt,
      {.min = 1})
  , kafka_connection_rate_limit_overrides(
      *this,
      "kafka_connection_rate_limit_overrides",
      "Overrides the maximum connections per second for one core for the "
      "specified IP addresses (for example, `['127.0.0.1:90', "
      "'50.20.1.1:40']`)",
      {.needs_restart = needs_restart::no,
       .example = R"(['127.0.0.1:90', '50.20.1.1:40'])",
       .visibility = visibility::user},
      {},
      validate_connection_rate)
  , transactional_id_expiration_ms(
      *this,
      "transactional_id_expiration_ms",
      "Expiration time of producer IDs. Measured starting from the time of the "
      "last write until now for a given ID. Producer IDs are automatically "
      "removed from memory when they expire, which helps manage memory usage. "
      "However, this natural cleanup may not be sufficient for workloads with "
      "high producer churn rates. For applications with long-running "
      "transactions, ensure this value accommodates your typical transaction "
      "lifetime to avoid premature producer ID expiration.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      10080min)
  , max_concurrent_producer_ids(
      *this,
      "max_concurrent_producer_ids",
      "Maximum number of active producer sessions per shard. Each shard "
      "tracks producer IDs using an LRU (Least Recently Used) eviction "
      "policy. When the configured limit is exceeded, the least recently "
      "used producer IDs are evicted from the cache.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      100000,
      {.min = 1})
  , max_transactions_per_coordinator(
      *this,
      "max_transactions_per_coordinator",
      "Specifies the maximum number of active transaction sessions per "
      "coordinator. When the threshold is passed Redpanda terminates old "
      "sessions. When an idle producer corresponding to the terminated session "
      "wakes up and produces, it leads to its batches being rejected with "
      "invalid producer epoch or invalid_producer_id_mapping error (depends on "
      "the transaction execution phase).",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10000,
      {.min = 1})
  , enable_idempotence(
      *this,
      "enable_idempotence",
      "Enable idempotent producers.",
      {.visibility = visibility::user},
      true)
  , enable_transactions(
      *this,
      "enable_transactions",
      "Enable transactions (atomic writes).",
      {.visibility = visibility::user},
      true)
  , abort_index_segment_size(
      *this,
      "abort_index_segment_size",
      "Capacity (in number of txns) of an abort index segment. Each partition "
      "tracks the aborted transaction offset ranges to help service client "
      "requests.If the number transactions increase beyond this threshold, "
      "they are flushed to disk to easy memory pressure.Then they're loaded on "
      "demand. This configuration controls the maximum number of aborted "
      "transactions  before they are flushed to disk.",
      {.visibility = visibility::tunable},
      50000)
  , log_retention_ms(
      *this,
      "log_retention_ms",
      "The amount of time to keep a log file before deleting it (in "
      "milliseconds). If set to `-1`, no time limit is applied. This is a "
      "cluster-wide default when a topic does not set or disable "
      "`retention.ms`.",
      {.needs_restart = needs_restart::no,
       .visibility = visibility::user,
       .aliases = {"delete_retention_ms"}},
      7 * 24h)
  , log_compaction_interval_ms(
      *this,
      "log_compaction_interval_ms",
      "How often to trigger background compaction.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      10s)
  , log_compaction_max_priority_wait_ms(
      *this,
      "log_compaction_max_priority_wait_ms",
      "Maximum time a priority partition (for example, __consumer_offsets) can "
      "wait for compaction before preempting regular compaction.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      60min)
  , tombstone_retention_ms(
      *this,
      "tombstone_retention_ms",
      "The retention time for tombstone records and transaction markers in a "
      "compacted topic.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt,
      validate_tombstone_retention_ms)
  , min_cleanable_dirty_ratio(
      *this,
      "min_cleanable_dirty_ratio",
      "The minimum ratio between the number of bytes in \"dirty\" segments and "
      "the total number of bytes in closed segments that must be reached "
      "before a partition's log is eligible for compaction in a compact topic. "
      "The topic property `min.cleanable.dirty.ratio` overrides the value of "
      "`min_cleanable_dirty_ratio` at the topic level.",
      {.needs_restart = needs_restart::no,
       .example = "0.5",
       .visibility = visibility::user},
      0.5,
      {.min = 0.0, .max = 1.0})
  , min_compaction_lag_ms(
      *this,
      "min_compaction_lag_ms",
      "For a compacted topic, the minimum time a message remains uncompacted "
      "in the log. "
      "The topic property `min.compaction.lag.ms` overrides this property.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      0ms,
      [](const auto& v) -> std::optional<ss::sstring> {
          // Maximum duration imposed by serde serialization.
          if (v < 0ms || v > serde::max_serializable_ms) {
              return fmt::format(
                "min compaction lag should be in range: [0, {}]",
                serde::max_serializable_ms);
          }
          return std::nullopt;
      })
  , max_compaction_lag_ms(
      *this,
      "max_compaction_lag_ms",
      "For a compacted topic, the maximum time a message remains ineligible "
      "for compaction. "
      "The topic property `max.compaction.lag.ms` overrides this property.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      serde::max_serializable_ms,
      [](const auto& v) -> std::optional<ss::sstring> {
          // Maximum duration imposed by serde serialization.
          if (v < 1ms || v > serde::max_serializable_ms) {
              return fmt::format(
                "max compaction lag should be in range: [1, {}]",
                serde::max_serializable_ms);
          }
          return std::nullopt;
      })
  , log_disable_housekeeping_for_tests(
      *this,
      "log_disable_housekeeping_for_tests",
      "Disables the housekeeping loop for local storage. This property is used "
      "to simplify testing, and should not be set in production.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      false)
  , log_compaction_use_sliding_window(
      *this,
      "log_compaction_use_sliding_window",
      "Use sliding window compaction.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      true)
  , log_compaction_pause_use_sliding_window(
      *this,
      "log_compaction_pause_use_sliding_window",
      "Pause use of sliding window compaction. This should only be toggled "
      "to `true` when it is desired to force adjacent segment compaction. The "
      "memory reserved by `storage_compaction_key_map_memory` is not freed "
      "when this is set to `true`.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      false)
  , log_compaction_merge_max_segments_per_range(
      *this,
      "log_compaction_merge_max_segments_per_range",
      "The maximum number of segments that can be combined into a single "
      "segment during an adjacent merge operation. If `null` (the default "
      "value), no maximum is imposed on the number of segments that can be "
      "combined at once. A value below 2 effectively disables adjacent merge "
      "compaction.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt)
  , log_compaction_merge_max_ranges(
      *this,
      "log_compaction_merge_max_ranges",
      "The maximum number of ranges of segments that can be processed in a "
      "single round of adjacent segment compaction. If `null` (the default "
      "value), no maximum is imposed on the number of ranges that can be "
      "processed at once. A value below 1 effectively disables adjacent merge "
      "compaction.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt)
  , log_compaction_tx_batch_removal_enabled(
      *this,
      "log_compaction_tx_batch_removal_enabled",
      "Enables removal of transactional control batches during compaction. "
      "These batches are removed according to a topic's configured "
      "delete.retention.ms, and only if the topic's cleanup.policy "
      "allows compaction.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      true,
      property<bool>::noop_validator)
  , retention_bytes(
      *this,
      "retention_bytes",
      "Default maximum number of bytes per partition on disk before triggering "
      "deletion of the oldest messages. If `null` (the default value), no "
      "limit is applied. The topic property `retention.bytes` overrides the "
      "value of `retention_bytes` at the topic level.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt)
  , group_topic_partitions(
      *this,
      "group_topic_partitions",
      "Number of partitions in the internal group membership topic.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      16)
  , default_topic_replications(
      *this,
      "default_topic_replications",
      "Default replication factor for new topics.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      1,
      {.min = 1, .oddeven = odd_even_constraint::odd})
  , minimum_topic_replication(
      *this,
      "minimum_topic_replications",
      "Minimum allowable replication factor for topics in this cluster. The "
      "set value must be positive, odd, and equal to or less than the number "
      "of available brokers. Changing this parameter only restricts "
      "newly-created topics. Redpanda returns an `INVALID_REPLICATION_FACTOR` "
      "error on any attempt to create a topic with a replication factor less "
      "than this property. If you change the `minimum_topic_replications` "
      "setting, the replication factor of existing topics remains unchanged. "
      "However, Redpanda will log a warning on start-up with a list of any "
      "topics that have fewer replicas than this minimum. For example, you "
      "might see a message such as `Topic X has a replication factor less than "
      "specified minimum: 1 < 3`.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      1,
      {.min = 1, .oddeven = odd_even_constraint::odd})
  , transaction_coordinator_partitions(
      *this,
      "transaction_coordinator_partitions",
      "Number of partitions for transactions coordinator.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      50)
  , transaction_coordinator_cleanup_policy(
      *this,
      "transaction_coordinator_cleanup_policy",
      "Cleanup policy for a transaction coordinator topic. Accepted Values: "
      "`compact`, `delete`, `[\"compact\",\"delete\"]`, `none`",
      {.needs_restart = needs_restart::no,
       .example = "compact,delete",
       .visibility = visibility::user},
      model::cleanup_policy_bitflags::deletion)
  , transaction_coordinator_delete_retention_ms(
      *this,
      "transaction_coordinator_delete_retention_ms",
      "Delete segments older than this age. To ensure transaction state is "
      "retained as long as the longest-running transaction, make sure this is "
      "no less than `transactional_id_expiration_ms`.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      10080min)
  , transaction_coordinator_log_segment_size(
      *this,
      "transaction_coordinator_log_segment_size",
      "The size (in bytes) each log segment should be.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1_GiB)
  , abort_timed_out_transactions_interval_ms(
      *this,
      "abort_timed_out_transactions_interval_ms",
      "Interval, in milliseconds, at which Redpanda looks for inactive "
      "transactions and aborts them.",
      {.visibility = visibility::tunable},
      10s)
  , transaction_max_timeout_ms(
      *this,
      "transaction_max_timeout_ms",
      "The maximum allowed timeout for transactions. If a client-requested "
      "transaction timeout exceeds this configuration, the broker returns an "
      "error during transactional producer initialization. This guardrail "
      "prevents hanging transactions from blocking consumer progress.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      15min)
  , tx_log_stats_interval_s(
      *this,
      "tx_log_stats_interval_s",
      "How often to log per partition tx stats, works only with debug logging "
      "enabled.",
      {.needs_restart = needs_restart::no,
       .visibility = visibility::deprecated},
      10s)
  , default_topic_partitions(
      *this,
      "default_topic_partitions",
      "Default number of partitions per topic.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      1)
  , disable_batch_cache(
      *this,
      "disable_batch_cache",
      "Disable batch cache in log manager.",
      {.visibility = visibility::tunable},
      false)
  , raft_election_timeout_ms(
      *this,
      "election_timeout_ms",
      "Election timeout expressed in milliseconds.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1'500ms)
  , kafka_group_recovery_timeout_ms(
      *this,
      "kafka_group_recovery_timeout_ms",
      "Kafka group recovery timeout.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      30'000ms)
  , replicate_append_timeout_ms(
      *this,
      "replicate_append_timeout_ms",
      "Timeout for append entry requests issued while replicating entries.",
      {.visibility = visibility::tunable},
      3s)
  , raft_replicate_batch_window_size(
      *this,
      "raft_replicate_batch_window_size",
      "Maximum size of requests cached for replication.",
      {.visibility = visibility::tunable},
      1_MiB)
  , raft_learner_recovery_rate(
      *this,
      "raft_learner_recovery_rate",
      "Raft learner recovery rate limit. Throttles the rate of data "
      "communicated to nodes (learners) that need to catch up to leaders. This "
      "rate limit is placed on a node sending data to a recovering node. Each "
      "sending node is limited to this rate. The recovering node accepts data "
      "as fast as possible according to the combined limits of all healthy "
      "nodes in the cluster. For example, if two nodes are sending data to the "
      "recovering node, and `raft_learner_recovery_rate` is 100 MB/sec, then "
      "the recovering node will recover at a rate of 200 MB/sec.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      100_MiB)
  , raft_recovery_throttle_disable_dynamic_mode(
      *this,
      "raft_recovery_throttle_disable_dynamic_mode",
      "Disables cross shard sharing used to throttle recovery traffic. Should "
      "only be used to debug unexpected problems.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      false)
  , controller_log_learner_recovery_rate_enabled(
      *this,
      "controller_log_learner_recovery_rate_enabled",
      "Whether the controller raft group (raft0) honors "
      "`raft_learner_recovery_rate`. When `false` (default) the controller log "
      "replicates to new learners without throttling. When `true`, "
      "controller-log recovery is subject to the same per-node recovery bucket "
      "as user partitions.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      false)
  , raft_smp_max_non_local_requests(
      *this,
      "raft_smp_max_non_local_requests",
      "Maximum number of Cross-core(Inter-shard communication) requests "
      "pending in Raft seastar::smp group. For details, refer to the "
      "`seastar::smp_service_group` documentation).",
      {.visibility = visibility::tunable},
      std::nullopt)
  , write_caching_default(
      *this,
      "write_caching_default",
      "The default write caching mode to apply to user topics. Write caching "
      "acknowledges a message as soon as it is received and acknowledged on a "
      "majority of brokers, without waiting for it to be written to disk. With "
      "`acks=all`, this provides lower latency while still ensuring that a "
      "majority of brokers acknowledge the write. Fsyncs follow "
      "`raft_replica_max_pending_flush_bytes` and "
      "`raft_replica_max_flush_delay_ms`, whichever is reached first. The "
      "`write_caching_default` cluster property can be overridden with the "
      "`write.caching` topic property. Accepted values: * `true` * `false` * "
      "`disabled`: This takes precedence over topic overrides and disables "
      "write caching for the entire cluster.",
      {.needs_restart = needs_restart::no,
       .example = "true",
       .visibility = visibility::user},
      model::write_caching_mode::default_false,
      {model::write_caching_mode::default_true,
       model::write_caching_mode::default_false,
       model::write_caching_mode::disabled})
  , reclaim_min_size(
      *this,
      "reclaim_min_size",
      "Minimum batch cache reclaim size.",
      {.visibility = visibility::tunable},
      128_KiB)
  , reclaim_max_size(
      *this,
      "reclaim_max_size",
      "Maximum batch cache reclaim size.",
      {.visibility = visibility::tunable},
      4_MiB)
  , reclaim_growth_window(
      *this,
      "reclaim_growth_window",
      "Starting from the last point in time when memory was reclaimed from the "
      "batch cache, this is the duration during which the amount of memory to "
      "reclaim grows at a significant rate, based on heuristics about the "
      "amount of available memory.",
      {.visibility = visibility::tunable},
      3'000ms)
  , reclaim_stable_window(
      *this,
      "reclaim_stable_window",
      "If the duration since the last time memory was reclaimed is longer than "
      "the amount of time specified in this property, the memory usage of the "
      "batch cache is considered stable, so only the minimum size "
      "(`reclaim_min_size`) is set to be reclaimed.",
      {.visibility = visibility::tunable},
      10'000ms)
  , reclaim_batch_cache_min_free(
      *this,
      "reclaim_batch_cache_min_free",
      "Minimum amount of free memory maintained by the batch cache background "
      "reclaimer.",
      {.visibility = visibility::tunable},
      64_MiB)
  , auto_create_topics_enabled(
      *this,
      "auto_create_topics_enabled",
      "Allow automatic topic creation. To prevent excess topics, this property "
      "is not supported on Redpanda Cloud BYOC and Dedicated clusters. You "
      "should explicitly manage topic creation for these Redpanda Cloud "
      "clusters. If you produce to a topic that doesn't exist, the topic will "
      "be created with defaults if this property is enabled.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , enable_pid_file(
      *this,
      "enable_pid_file",
      "Enable PID file. You should not need to change.",
      {.visibility = visibility::tunable},
      true)
  , kvstore_flush_interval(
      *this,
      "kvstore_flush_interval",
      "Key-value store flush interval (in milliseconds).",
      {.needs_restart = needs_restart::no,
       .visibility = visibility::tunable,
       .usable_before_ready = usable_before_ready::yes},
      std::chrono::milliseconds(10))
  , kvstore_max_segment_size(
      *this,
      "kvstore_max_segment_size",
      "Key-value maximum segment size (in bytes).",
      {.visibility = visibility::tunable,
       .usable_before_ready = usable_before_ready::yes},
      16_MiB)
  , max_kafka_throttle_delay_ms(
      *this,
      "max_kafka_throttle_delay_ms",
      "Fail-safe maximum throttle delay on Kafka requests.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      30'000ms)
  , kafka_per_entity_quota_metrics(
      *this,
      "kafka_per_entity_quota_metrics",
      "Enable per-entity labels on quota throttle time and throughput "
      "metrics. Metric series are registered only when an entity is actively "
      "being throttled and deregistered after the quota GC window. Opt-in "
      "due to cardinality and performance implications.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , kafka_max_bytes_per_fetch(
      *this,
      "kafka_max_bytes_per_fetch",
      "Limit fetch responses to this many bytes, even if the total of "
      "partition bytes limits is higher.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      64_MiB)
  , raft_io_timeout_ms(
      *this,
      "raft_io_timeout_ms",
      "Raft I/O timeout.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10'000ms)
  , join_retry_timeout_ms(
      *this,
      "join_retry_timeout_ms",
      "Time between cluster join retries in milliseconds.",
      {.visibility = visibility::tunable,
       .usable_before_ready = usable_before_ready::yes},
      5s)
  , raft_timeout_now_timeout_ms(
      *this,
      "raft_timeout_now_timeout_ms",
      "Timeout for Raft's timeout_now RPC. This RPC is used to force a "
      "follower to dispatch a round of votes immediately.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1s)
  , raft_transfer_leader_recovery_timeout_ms(
      *this,
      "raft_transfer_leader_recovery_timeout_ms",
      "Follower recovery timeout waiting period when transferring leadership.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10s)
  , release_cache_on_segment_roll(
      *this,
      "release_cache_on_segment_roll",
      "Flag for specifying whether or not to release cache when a full segment "
      "is rolled.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      false)
  , segment_appender_flush_timeout_ms(
      *this,
      "segment_appender_flush_timeout_ms",
      "Maximum delay until buffered data is written.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::chrono::milliseconds(1s))
  , fetch_session_eviction_timeout_ms(
      *this,
      "fetch_session_eviction_timeout_ms",
      "Time duration after which the inactive fetch session is removed from "
      "the fetch session cache. Fetch sessions are used to implement the "
      "incremental fetch requests where a consumer does not send all requested "
      "partitions to the server but the server tracks them for the consumer.",
      {.visibility = visibility::tunable},
      60s)
  , append_chunk_size(
      *this,
      "append_chunk_size",
      "Size of direct write operations to disk in bytes. A larger chunk size "
      "can improve performance for write-heavy workloads, but increase latency "
      "for these writes as more data is collected before each write operation. "
      "A smaller chunk size can decrease write latency, but potentially "
      "increase the number of disk I/O operations.",
      {.example = "32768",
       .visibility = visibility::tunable,
       .usable_before_ready = usable_before_ready::yes},
      16_KiB,
      {.min = 4096, .max = 32_MiB, .align = 4096})
  , storage_read_buffer_size(
      *this,
      "storage_read_buffer_size",
      "Size of each read buffer (one per in-flight read, per log segment).",
      {.needs_restart = needs_restart::no,
       .example = "31768",
       .visibility = visibility::tunable,
       .usable_before_ready = usable_before_ready::yes},
      128_KiB)
  , storage_read_readahead_count(
      *this,
      "storage_read_readahead_count",
      "How many additional reads to issue ahead of current read location.",
      {.needs_restart = needs_restart::no,
       .example = "1",
       .visibility = visibility::tunable,
       .usable_before_ready = usable_before_ready::yes},
      1)
  , segment_fallocation_step(
      *this,
      "segment_fallocation_step",
      "Size for segments fallocation.",
      {.needs_restart = needs_restart::no,
       .example = "32768",
       .visibility = visibility::tunable},
      32_MiB,
      storage::validate_fallocation_step)
  , storage_target_replay_bytes(
      *this,
      "storage_target_replay_bytes",
      "Target bytes to replay from disk on startup after clean shutdown: "
      "controls frequency of snapshots and checkpoints.",
      {.needs_restart = needs_restart::no,
       .example = "2147483648",
       .visibility = visibility::tunable,
       .usable_before_ready = usable_before_ready::yes},
      10_GiB,
      {.min = 128_MiB, .max = 1_TiB})
  , storage_max_concurrent_replay(
      *this,
      "storage_max_concurrent_replay",
      "Maximum number of partitions' logs that will be replayed concurrently "
      "at startup, or flushed concurrently on shutdown.",
      {.needs_restart = needs_restart::no,
       .example = "2048",
       .visibility = visibility::tunable,
       .usable_before_ready = usable_before_ready::yes},
      1024,
      {.min = 128})
  , storage_compaction_index_memory(
      *this,
      "storage_compaction_index_memory",
      "Maximum number of bytes that may be used on each shard by compaction "
      "index writers.",
      {.needs_restart = needs_restart::no,
       .example = "1073741824",
       .visibility = visibility::tunable,
       .usable_before_ready = usable_before_ready::yes},
      128_MiB,
      {.min = 16_MiB, .max = 100_GiB})
  , storage_compaction_key_map_memory(
      *this,
      "storage_compaction_key_map_memory",
      "Maximum number of bytes that may be used on each shard by compaction "
      "key-offset maps. Only applies when `log_compaction_use_sliding_window` "
      "is set to `true`.",
      {.needs_restart = needs_restart::yes,
       .example = "1073741824",
       .visibility = visibility::tunable},
      128_MiB,
      {.min = 16_MiB, .max = 100_GiB})
  , storage_compaction_key_map_memory_limit_percent(
      *this,
      "storage_compaction_key_map_memory_limit_percent",
      "Limit on `storage_compaction_key_map_memory`, expressed as a percentage "
      "of memory per shard, that bounds the amount of memory used by "
      "compaction key-offset maps. Only applies when "
      "`log_compaction_use_sliding_window` is set to `true`.",
      {.needs_restart = needs_restart::yes,
       .example = "12.0",
       .visibility = visibility::tunable},
      12.0,
      {.min = 1.0, .max = 100.0})
  , max_compacted_log_segment_size(
      *this,
      "max_compacted_log_segment_size",
      "Maximum compacted segment size after consolidation.",
      {.needs_restart = needs_restart::no,
       .example = "10737418240",
       .visibility = visibility::tunable},
      512_MiB)
  , storage_ignore_timestamps_in_future_sec(
      *this,
      "storage_ignore_timestamps_in_future_sec",
      "The maximum number of seconds that a record's timestamp can be ahead of "
      "a Redpanda broker's clock and still be used when deciding whether to "
      "clean up the record for data retention. This property makes possible "
      "the timely cleanup of records from clients with clocks that are "
      "drastically unsynchronized relative to Redpanda. When determining "
      "whether to clean up a record with timestamp more than "
      "`storage_ignore_timestamps_in_future_sec` seconds ahead of the broker, "
      "Redpanda ignores the record's timestamp and instead uses a valid "
      "timestamp of another record in the same segment, or (if another "
      "record's valid timestamp is unavailable) the timestamp of when the "
      "segment file was last modified (mtime). By default, "
      "`storage_ignore_timestamps_in_future_sec` is disabled (null). To figure "
      "out whether to set `storage_ignore_timestamps_in_future_sec` for your "
      "system: . Look for logs with segments that are unexpectedly large and "
      "not being cleaned up. . In the logs, search for records with "
      "unsynchronized timestamps that are further into the future than "
      "tolerable by your data retention and storage settings. For example, "
      "timestamps 60 seconds or more into the future can be considered to be "
      "too unsynchronized. . If you find unsynchronized timestamps throughout "
      "your logs, determine the number of seconds that the timestamps are "
      "ahead of their actual time, and set "
      "`storage_ignore_timestamps_in_future_sec` to that value so data "
      "retention can proceed. . If you only find unsynchronized timestamps "
      "that are the result of transient behavior, you can disable "
      "`storage_ignore_timestamps_in_future_sec`.",
      {.needs_restart = needs_restart::no,
       .example = "3600",
       .visibility = visibility::tunable},
      std::nullopt)
  , storage_ignore_cstore_hints(
      *this,
      "storage_ignore_cstore_hints",
      "When set, cstore hints are ignored and not used for data access (but "
      "are otherwise generated).",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      false)
  , storage_reserve_min_segments(
      *this,
      "storage_reserve_min_segments",
      "The number of segments per partition that the system will attempt to "
      "reserve disk capacity for. For example, if the maximum segment size is "
      "configured to be 100 MB, and the value of this option is 2, then in a "
      "system with 10 partitions Redpanda will attempt to reserve at least 2 "
      "GB of disk space.",
      {.needs_restart = needs_restart::no,
       .example = "4",
       .visibility = visibility::tunable},
      2,
      {.min = 1})
  , debug_load_slice_warning_depth(
      *this,
      "debug_load_slice_warning_depth",
      "The recursion depth after which debug logging is enabled automatically "
      "for the log reader.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt)
  , id_allocator_log_capacity(
      *this,
      "id_allocator_log_capacity",
      "Capacity of the `id_allocator` log in number of batches. After it "
      "reaches `id_allocator_stm`, it truncates the log's prefix.",
      {.visibility = visibility::tunable},
      100)
  , id_allocator_batch_size(
      *this,
      "id_allocator_batch_size",
      "The ID allocator allocates messages in batches (each batch is a one log "
      "record) and then serves requests from memory without touching the log "
      "until the batch is exhausted.",
      {.visibility = visibility::tunable},
      1000)
  , enable_sasl(
      *this,
      "enable_sasl",
      "Enable SASL authentication for Kafka connections. Authorization is "
      "required to modify this property. See also "
      "`kafka_enable_authorization`.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , sasl_mechanisms(
      *this,
      is_enterprise_sasl_mechanism,
      "sasl_mechanisms",
      "A list of supported SASL mechanisms, if no override is defined in "
      "`sasl_mechanisms_overrides` for each Kafka listener. Accepted values: "
      "`SCRAM`, `GSSAPI`, `OAUTHBEARER`, `PLAIN`.  Note that in order to "
      "enable PLAIN, you must also enable SCRAM.",
      meta{
        .needs_restart = needs_restart::no,
        .visibility = visibility::user,
      },
      std::vector<ss::sstring>{ss::sstring{scram}},
      validate_sasl_mechanisms)
  , sasl_mechanisms_overrides(
      *this,
      is_enterprise_sasl_mechanisms_override,
      "sasl_mechanisms_overrides",
      "A list of overrides for SASL mechanisms, defined by listener. SASL "
      "mechanisms defined here will replace the ones set in `sasl_mechanisms`. "
      "The same limitations apply as for `sasl_mechanisms`.",
      meta{
        .needs_restart = needs_restart::no,
        .example
        = "[{'listener':'kafka_listener', 'sasl_mechanisms':['SCRAM']}]",
        .visibility = visibility::user,
      },
      std::vector<sasl_mechanisms_override>{},
      validate_sasl_mechanisms_overrides)
  , sasl_kerberos_config(
      *this,
      "sasl_kerberos_config",
      "The location of the Kerberos `krb5.conf` file for Redpanda.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      "/etc/krb5.conf")
  , sasl_kerberos_keytab(
      *this,
      "sasl_kerberos_keytab",
      "The location of the Kerberos keytab file for Redpanda.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      "/var/lib/redpanda/redpanda.keytab")
  , sasl_kerberos_principal(
      *this,
      "sasl_kerberos_principal",
      "The primary of the Kerberos Service Principal Name (SPN) for Redpanda.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      "redpanda",
      &validate_non_empty_string_opt)
  , sasl_kerberos_principal_mapping(
      *this,
      "sasl_kerberos_principal_mapping",
      "Rules for mapping Kerberos principal names to Redpanda user principals.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      {"DEFAULT"},
      security::validate_kerberos_mapping_rules)
  , kafka_sasl_max_reauth_ms(
      *this,
      "kafka_sasl_max_reauth_ms",
      "The maximum time between Kafka client reauthentications. If a client "
      "has not reauthenticated a connection within this time frame, that "
      "connection is torn down. If this property is not set (or set to "
      "`null`), session expiry is disabled, and a connection could live long "
      "after the client's credentials are expired or revoked.",
      {.needs_restart = needs_restart::no,
       .example = "1000",
       .visibility = visibility::user},
      std::nullopt,
      {.min = 1000ms})
  , kafka_enable_authorization(
      *this,
      "kafka_enable_authorization",
      "Flag to require authorization for Kafka connections. If `null`, the "
      "property is disabled, and authorization is instead enabled by "
      "enable_sasl. * `null`: Ignored. Authorization is enabled with "
      "`enable_sasl`: `true` * `true`: authorization is required. * `false`: "
      "authorization is disabled.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt)
  , tls_certificate_name_format(
      *this,
      "tls_certificate_name_format",
      "The format of the certificates's distinguished name to use for mTLS "
      "principal mapping.  Legacy format would appear as "
      "'C=US,ST=California,L=San Francisco,O=Redpanda,CN=redpanda', while "
      "rfc2253 format would appear as 'CN=redpanda,O=Redpanda,L=San "
      "Francisco,ST=California,C=US'.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      tls_name_format::legacy,
      {tls_name_format::legacy, tls_name_format::rfc2253})
  , kafka_mtls_principal_mapping_rules(
      *this,
      "kafka_mtls_principal_mapping_rules",
      "Principal mapping rules for mTLS authentication on the Kafka API. If "
      "`null`, the property is disabled.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt,
      security::tls::validate_rules)
  , kafka_enable_partition_reassignment(
      *this,
      "kafka_enable_partition_reassignment",
      "Enable the Kafka partition reassignment API.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      true)
  , controller_backend_housekeeping_interval_ms(
      *this,
      "controller_backend_housekeeping_interval_ms",
      "Interval between iterations of controller backend housekeeping loop.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1s)
  , kafka_request_max_bytes(
      *this,
      "kafka_request_max_bytes",
      "Maximum size of a single request processed using the Kafka API.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      100_MiB)
  , kafka_batch_max_bytes(
      *this,
      "kafka_batch_max_bytes",
      "Maximum size of a batch processed by the server. If the batch is "
      "compressed, the limit applies to the compressed batch size.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1_MiB)
  , delete_topic_enable(
      *this,
      false,
      "delete_topic_enable",
      "Enable or disable topic deletion via the Kafka DeleteTopics API. When "
      "set to false, all topic deletion requests are rejected with error code "
      "73 (TOPIC_DELETION_DISABLED). This is a cluster-wide safety setting "
      "that cannot be overridden by superusers. Topics in "
      "kafka_nodelete_topics are always protected regardless of this setting.",
      meta{.needs_restart = needs_restart::no, .visibility = visibility::user},
      true)
  , kafka_nodelete_topics(
      *this,
      "kafka_nodelete_topics",
      "A list of topics that are protected from deletion and configuration "
      "changes by Kafka clients. Set by default to a list of Redpanda internal "
      "topics.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      {model::kafka_audit_logging_topic(), "__consumer_offsets", "_schemas"},
      &validate_non_empty_string_vec)
  , kafka_noproduce_topics(
      *this,
      "kafka_noproduce_topics",
      "A list of topics that are protected from being produced to by Kafka "
      "clients. Set by default to a list of Redpanda internal topics.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      {},
      &validate_non_empty_string_vec)
  , kafka_topics_max(
      *this,
      "kafka_topics_max",
      "Maximum number of Kafka user topics that can be created. If `null`, "
      "then no limit is enforced.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt)
  , kafka_max_message_size_upper_limit_bytes(
      *this,
      "kafka_max_message_size_upper_limit_bytes",
      "Maximum allowed value for the `max.message.size` topic "
      "property. When set to `null`, then no limit is enforced.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      100_MiB,
      {.min = 1})
  , compaction_ctrl_update_interval_ms(
      *this,
      "compaction_ctrl_update_interval_ms",
      "",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      30s)
  , compaction_ctrl_p_coeff(
      *this,
      "compaction_ctrl_p_coeff",
      "Proportional coefficient for compaction PID controller. This must be "
      "negative, because the compaction backlog should decrease when the "
      "number of compaction shares increases.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      -12.5)
  , compaction_ctrl_i_coeff(
      *this,
      "compaction_ctrl_i_coeff",
      "Integral coefficient for compaction PID controller.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      0.0)
  , compaction_ctrl_d_coeff(
      *this,
      "compaction_ctrl_d_coeff",
      "Derivative coefficient for compaction PID controller.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      0.2)
  , compaction_ctrl_min_shares(
      *this,
      "compaction_ctrl_min_shares",
      "Minimum number of I/O and CPU shares that compaction process can use.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10)
  , compaction_ctrl_max_shares(
      *this,
      "compaction_ctrl_max_shares",
      "Maximum number of I/O and CPU shares that compaction process can use.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1000)
  , compaction_ctrl_backlog_size(
      *this,
      "compaction_ctrl_backlog_size",
      "Target backlog size for compaction controller. If not set the max "
      "backlog size is configured to 80% of total disk space available.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt)
  , members_backend_retry_ms(
      *this,
      "members_backend_retry_ms",
      "Time between members backend reconciliation loop retries.",
      {.visibility = visibility::tunable},
      5s)
  , kafka_connections_max(
      *this,
      "kafka_connections_max",
      "Maximum number of Kafka client connections per broker. If `null`, the "
      "property is disabled.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt)
  , kafka_connections_max_per_ip(
      *this,
      "kafka_connections_max_per_ip",
      "Maximum number of Kafka client connections per IP address, per broker. "
      "If `null`, the property is disabled.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt)
  , kafka_connections_max_overrides(
      *this,
      "kafka_connections_max_overrides",
      "A list of IP addresses for which Kafka client connection limits are "
      "overridden and don't apply. For example, `(['127.0.0.1:90', "
      "'50.20.1.1:40']).`",
      {.needs_restart = needs_restart::no,
       .example = R"(['127.0.0.1:90', '50.20.1.1:40'])",
       .visibility = visibility::user},
      {},
      validate_connection_rate)
  , kafka_rpc_server_tcp_recv_buf(
      *this,
      "kafka_rpc_server_tcp_recv_buf",
      "Size of the Kafka server TCP receive buffer. If `null`, the property is "
      "disabled.",
      {.example = "65536"},
      std::nullopt,
      {.min = 32_KiB, .align = 4_KiB})
  , kafka_rpc_server_tcp_send_buf(
      *this,
      "kafka_rpc_server_tcp_send_buf",
      "Size of the Kafka server TCP transmit buffer. If `null`, the property "
      "is disabled.",
      {.example = "65536"},
      std::nullopt,
      {.min = 32_KiB, .align = 4_KiB})
  , kafka_rpc_server_stream_recv_buf(
      *this,
      "kafka_rpc_server_stream_recv_buf",
      "Maximum size of the user-space receive buffer. If `null`, this limit is "
      "not applied.",
      {.example = "65536", .visibility = visibility::tunable},
      std::nullopt,
      // The minimum is set to match seastar's min_buffer_size (i.e. don't
      // permit setting a max below the min).  The maximum is set to forbid
      // contiguous allocations beyond that size.
      {.min = 512, .max = 512_KiB, .align = 4_KiB})
  , kafka_enable_describe_log_dirs_remote_storage(
      *this,
      "kafka_enable_describe_log_dirs_remote_storage",
      "Whether to include Tiered Storage as a special remote:// directory in "
      "`DescribeLogDirs Kafka` API requests.",
      {.needs_restart = needs_restart::no,
       .example = "false",
       .visibility = visibility::user},
      true)
  , audit_enabled(
      *this,
      true, /* restricted values */
      "audit_enabled",
      "Enables or disables audit logging. When you set this to true, Redpanda "
      "checks for an existing topic named `_redpanda.audit_log`. If none is "
      "found, Redpanda automatically creates one for you.",
      meta{
        .needs_restart = needs_restart::no,
        .visibility = visibility::user,
      },
      false)
  , audit_log_num_partitions(
      *this,
      "audit_log_num_partitions",
      "Defines the number of partitions used by a newly-created audit topic. "
      "This configuration applies only to the audit log topic and may be "
      "different from the cluster or other topic configurations. This cannot "
      "be altered for existing audit log topics.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      12)
  , audit_log_replication_factor(
      *this,
      "audit_log_replication_factor",
      "Defines the replication factor for a newly-created audit log topic. "
      "This configuration applies only to the audit log topic and may be "
      "different from the cluster or other topic configurations. This cannot "
      "be altered for existing audit log topics. Setting this value is "
      "optional. If a value is not provided, Redpanda will use the value "
      "specified for `internal_topic_replication_factor`.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt)
  , audit_client_max_buffer_size(
      *this,
      "audit_client_max_buffer_size",
      "Defines the number of bytes allocated by the internal audit client for "
      "audit messages. When changing this, you must disable audit logging and "
      "then re-enable it for the change to take effect. Consider increasing "
      "this if your system generates a very large number of audit records in a "
      "short amount of time.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      16_MiB)
  , audit_queue_drain_interval_ms(
      *this,
      "audit_queue_drain_interval_ms",
      "Interval, in milliseconds, at which Redpanda flushes the queued audit "
      "log messages to the audit log topic. Longer intervals may help prevent "
      "duplicate messages, especially in high throughput scenarios, but they "
      "also increase the risk of data loss during shutdowns where the queue is "
      "lost.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      500ms)
  , audit_queue_max_buffer_size_per_shard(
      *this,
      "audit_queue_max_buffer_size_per_shard",
      "Defines the maximum amount of memory in bytes used by the audit buffer "
      "in each shard. Once this size is reached, requests to log additional "
      "audit messages will return a non-retryable error. Limiting the buffer "
      "size per shard helps prevent any single shard from consuming excessive "
      "memory due to audit log messages.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      1_MiB)
  , audit_enabled_event_types(
      *this,
      "audit_enabled_event_types",
      "List of strings in JSON style identifying the event types to include in "
      "the audit log. This may include any of the following: `management, "
      "produce, consume, describe, heartbeat, authenticate, "
      "admin`.",
      {
        .needs_restart = needs_restart::no,
        .example = R"(["management", "describe"])",
        .visibility = visibility::user,
      },
      {"management", "authenticate", "admin"},
      validate_audit_event_types)
  , audit_excluded_topics(
      *this,
      "audit_excluded_topics",
      "List of topics to exclude from auditing.",
      {
        .needs_restart = needs_restart::no,
        .example = R"(["topic1","topic2"])",
        .visibility = visibility::user,
      },
      {},
      validate_audit_excluded_topics)
  , audit_excluded_principals(
      *this,
      "audit_excluded_principals",
      "List of user principals to exclude from auditing.",
      {
        .needs_restart = needs_restart::no,
        .example = R"(["User:principal1","User:principal2"])",
        .visibility = visibility::user,
      },
      {})
  , audit_failure_policy(
      *this,
      "audit_failure_policy",
      "Defines the policy for rejecting audit log messages when the audit log "
      "queue is full. If set to 'permit', then new audit messages are dropped "
      "and the operation is permitted.  If set to 'reject', then the operation "
      "is rejected.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      audit_failure_policy::reject,
      {audit_failure_policy::reject, audit_failure_policy::permit})
  , audit_use_rpc(
      *this,
      "audit_use_rpc",
      "Produce audit log messages using internal Redpanda RPCs. When disabled, "
      "produce audit log messages using a Kafka client instead.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      true)
  , disable_cluster_recovery_loop_for_tests(
      *this,
      "disable_cluster_recovery_loop_for_tests",
      "Disables the cluster recovery loop. This property is used to simplify "
      "testing and should not be set in production.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      false)
  , retention_local_target_bytes_default(
      *this,
      "retention_local_target_bytes_default",
      "Local retention size target for partitions of topics with object "
      "storage write enabled. If `null`, the property is disabled. This "
      "property can be overridden on a per-topic basis by setting "
      "`retention.local.target.bytes` in each topic enabled for Tiered "
      "Storage. Both `retention_local_target_bytes_default` and "
      "`retention_local_target_ms_default` can be set. The limit that is "
      "reached earlier is applied.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt)
  , retention_local_target_ms_default(
      *this,
      "retention_local_target_ms_default",
      "Local retention time target for partitions of topics with object "
      "storage write enabled. This property can be overridden on a per-topic "
      "basis by setting `retention.local.target.ms` in each topic enabled for "
      "Tiered Storage. Both `retention_local_target_bytes_default` and "
      "`retention_local_target_ms_default` can be set. The limit that is "
      "reached first is applied.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      24h)
  , retention_local_strict(
      *this,
      "retention_local_strict",
      "Flag to allow Tiered Storage topics to expand to consumable retention "
      "policy limits. When this flag is enabled, non-local retention settings "
      "are used, and local retention settings are used to inform data removal "
      "policies in low-disk space scenarios.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false,
      property<bool>::noop_validator,
      legacy_default<bool>(true, legacy_version{9}))
  , retention_local_strict_override(
      *this,
      "retention_local_strict_override",
      "Trim log data when a cloud topic reaches its local retention limit. "
      "When this option is disabled Redpanda will allow partitions to grow "
      "past the local retention limit, and will be trimmed automatically as "
      "storage reaches the configured target size.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      true)
  , retention_local_target_capacity_bytes(
      *this,
      "retention_local_target_capacity_bytes",
      "The target capacity (in bytes) that log storage will try to use before "
      "additional retention rules take over to trim data to meet the target. "
      "When no target is specified, storage usage is unbounded. Redpanda Data "
      "recommends setting only one of `retention_local_target_capacity_bytes` "
      "or `retention_local_target_capacity_percent`. If both are set, the "
      "minimum of the two is used as the effective target capacity.",
      {.needs_restart = needs_restart::no,
       .example = "2147483648000",
       .visibility = visibility::user},
      std::nullopt,
      property<std::optional<size_t>>::noop_validator,
      legacy_default<std::optional<size_t>>(std::nullopt, legacy_version{9}))
  , retention_local_target_capacity_percent(
      *this,
      "retention_local_target_capacity_percent",
      "The target capacity in percent of unreserved space "
      "(`disk_reservation_percent`) that log storage will try to use before "
      "additional retention rules will take over to trim data in order to meet "
      "the target. When no target is specified storage usage is unbounded. "
      "Redpanda Data recommends setting only one of "
      "`retention_local_target_capacity_bytes` or "
      "`retention_local_target_capacity_percent`. If both are set, the minimum "
      "of the two is used as the effective target capacity.",
      {.needs_restart = needs_restart::no,
       .example = "80.0",
       .visibility = visibility::user},
      80.0,
      {.min = 0.0, .max = 100.0},
      legacy_default<std::optional<double>>(std::nullopt, legacy_version{9}))
  , retention_local_trim_interval(
      *this,
      "retention_local_trim_interval",
      "The period during which disk usage is checked for disk pressure, and "
      "data is optionally trimmed to meet the target.",
      {.needs_restart = needs_restart::no,
       .example = "31536000000",
       .visibility = visibility::tunable},
      30s)
  , retention_local_trim_overage_coeff(
      *this,
      "retention_local_trim_overage_coeff",
      "The space management control loop reclaims the overage multiplied by "
      "this this coefficient to compensate for data that is written during the "
      "idle period between control loop invocations.",
      {.needs_restart = needs_restart::no,
       .example = "1.8",
       .visibility = visibility::tunable},
      2.0)
  , space_management_enable(
      *this,
      "space_management_enable",
      "Option to explicitly disable automatic disk space management. If this "
      "property was explicitly disabled while using v23.2, it will remain "
      "disabled following an upgrade.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      true)
  , space_management_enable_override(*this, "space_management_enable_override")
  , disk_reservation_percent(
      *this,
      "disk_reservation_percent",
      "The percentage of total disk capacity that Redpanda will avoid using. "
      "This applies both when cloud cache and log data share a disk, as well "
      "as when cloud cache uses a dedicated disk. It is recommended to not run "
      "disks near capacity to avoid blocking I/O due to low disk space, as "
      "well as avoiding performance issues associated with SSD garbage "
      "collection.",
      {.needs_restart = needs_restart::no,
       .example = "25.0",
       .visibility = visibility::tunable},
      25.0,
      {.min = 0.0, .max = 100.0},
      legacy_default<double>(0.0, legacy_version{9}))
  , space_management_max_log_concurrency(
      *this,
      "space_management_max_log_concurrency",
      "Maximum parallel logs inspected during space management process.",
      {.needs_restart = needs_restart::no,
       .example = "20",
       .visibility = visibility::tunable},
      20,
      {.min = 1})
  , space_management_max_segment_concurrency(
      *this,
      "space_management_max_segment_concurrency",
      "Maximum parallel segments inspected during space management process.",
      {.needs_restart = needs_restart::no,
       .example = "10",
       .visibility = visibility::tunable},
      10,
      {.min = 1})
  , log_eviction_exempt_topics(
      *this,
      "log_eviction_exempt_topics",
      "A list of topics in the kafka namespace whose local log is exempt "
      "from any form of data deletion: retention settings, local retention "
      "for topics with Tiered Storage enabled, and disk space management "
      "never remove their data from local disk, and partition moves always "
      "deliver the full log to the new replica. Does not affect topic "
      "deletion via the Kafka API (see kafka_nodelete_topics).",
      {.needs_restart = needs_restart::yes, .visibility = visibility::user},
      {},
      &validate_non_empty_string_vec)
  , initial_retention_local_target_bytes_default(
      *this,
      "initial_retention_local_target_bytes_default",
      "Initial local retention size target for partitions of topics with "
      "Tiered Storage enabled. If no initial local target retention is "
      "configured all locally retained data will be delivered to learner when "
      "joining partition replica set.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt)
  , initial_retention_local_target_ms_default(
      *this,
      "initial_retention_local_target_ms_default",
      "Initial local retention time target for partitions of topics with "
      "Tiered Storage enabled. If no initial local target retention is "
      "configured all locally retained data will be delivered to learner when "
      "joining partition replica set.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt)
  , superusers(
      *this,
      "superusers",
      "List of superuser usernames.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      {})
  , kafka_qdc_latency_alpha(
      *this,
      "kafka_qdc_latency_alpha",
      "Smoothing parameter for Kafka queue depth control latency tracking.",
      {.visibility = visibility::tunable},
      0.002)
  , kafka_qdc_window_size_ms(
      *this,
      "kafka_qdc_window_size_ms",
      "Window size for Kafka queue depth control latency tracking.",
      {.visibility = visibility::tunable},
      1500ms)
  , kafka_qdc_window_count(
      *this,
      "kafka_qdc_window_count",
      "Number of windows used in kafka queue depth control latency tracking.",
      {.visibility = visibility::tunable},
      12)
  , kafka_qdc_enable(
      *this,
      "kafka_qdc_enable",
      "Enable kafka queue depth control.",
      {.visibility = visibility::user},
      false)
  , kafka_qdc_depth_alpha(
      *this,
      "kafka_qdc_depth_alpha",
      "Smoothing factor for Kafka queue depth control depth tracking.",
      {.visibility = visibility::tunable},
      0.8)
  , kafka_qdc_max_latency_ms(
      *this,
      "kafka_qdc_max_latency_ms",
      "Maximum latency threshold for Kafka queue depth control depth tracking.",
      {.visibility = visibility::user},
      80ms)
  , kafka_qdc_idle_depth(
      *this,
      "kafka_qdc_idle_depth",
      "Queue depth when idleness is detected in Kafka queue depth control.",
      {.visibility = visibility::tunable},
      10)
  , kafka_qdc_min_depth(
      *this,
      "kafka_qdc_min_depth",
      "Minimum queue depth used in Kafka queue depth control.",
      {.visibility = visibility::tunable},
      1)
  , kafka_qdc_max_depth(
      *this,
      "kafka_qdc_max_depth",
      "Maximum queue depth used in kafka queue depth control.",
      {.visibility = visibility::tunable},
      100)
  , kafka_qdc_depth_update_ms(
      *this,
      "kafka_qdc_depth_update_ms",
      "Update frequency for Kafka queue depth control.",
      {.visibility = visibility::tunable},
      7s)
  , zstd_decompress_workspace_bytes(
      *this,
      "zstd_decompress_workspace_bytes",
      "Size of the zstd decompression workspace.",
      {.visibility = visibility::tunable},
      8_MiB)
  , lz4_decompress_reusable_buffers_disabled(
      *this,
      "lz4_decompress_reusable_buffers_disabled",
      "Disable reusable preallocated buffers for LZ4 decompression.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      false)
  , enable_auto_rebalance_on_node_add(
      *this,
      "enable_auto_rebalance_on_node_add",
      "Enable automatic partition rebalancing when new nodes are added",
      {.needs_restart = needs_restart::no,
       .visibility = visibility::deprecated},
      false)

  , partition_autobalancing_mode(
      *this,
      model::partition_autobalancing_mode::continuous,
      model::partition_autobalancing_mode::node_add,
      "partition_autobalancing_mode",
      "Mode of partition balancing for a cluster. * `node_add`: partition "
      "balancing happens when a node is added. * `continuous`: partition "
      "balancing happens automatically to maintain optimal performance and "
      "availability, based on continuous monitoring for node changes (same as "
      "`node_add`) and also high disk usage. This option requires an "
      "Enterprise license, and it is customized by "
      "`partition_autobalancing_node_availability_timeout_sec` and "
      "`partition_autobalancing_max_disk_usage_percent` properties. * `off`: "
      "partition balancing is disabled. This option is not recommended for "
      "production clusters.",
      meta{
        .needs_restart = needs_restart::no,
        .example = "node_add",
        .visibility = visibility::user,
      },
      model::partition_autobalancing_mode::continuous,
      std::vector<model::partition_autobalancing_mode>{
        model::partition_autobalancing_mode::off,
        model::partition_autobalancing_mode::node_add,
        model::partition_autobalancing_mode::continuous,
      },
      legacy_default<model::partition_autobalancing_mode>{
        model::partition_autobalancing_mode::node_add, legacy_version{16}})
  , partition_autobalancing_node_availability_timeout_sec(
      *this,
      "partition_autobalancing_node_availability_timeout_sec",
      "When a node is unavailable for at least this timeout duration, it "
      "triggers Redpanda to move partitions off of the node. This property "
      "applies only when `partition_autobalancing_mode` is set to "
      "`continuous`.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      15min)
  , partition_autobalancing_node_autodecommission_timeout_sec(
      *this,
      "partition_autobalancing_node_autodecommission_timeout_sec",
      "When a node is unavailable for at least this timeout duration, it "
      "triggers Redpanda to decommission the node. This property "
      "applies only when `partition_autobalancing_mode` is set to "
      "`continuous`.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt)
  , partition_autobalancing_max_disk_usage_percent(
      *this,
      "partition_autobalancing_max_disk_usage_percent",
      "When the disk usage of a node exceeds this threshold, it triggers "
      "Redpanda to move partitions off of the node. This property applies only "
      "when partition_autobalancing_mode is set to `continuous`.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      80,
      {.min = 5, .max = 100})
  , partition_autobalancing_tick_interval_ms(
      *this,
      "partition_autobalancing_tick_interval_ms",
      "Partition autobalancer tick interval.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      30s)
  , partition_autobalancing_movement_batch_size_bytes(
      *this,
      "partition_autobalancing_movement_batch_size_bytes",
      "Total size of partitions that autobalancer is going to move in one "
      "batch (deprecated, use partition_autobalancing_concurrent_moves to "
      "limit the autobalancer concurrency)",
      {.needs_restart = needs_restart::no,
       .visibility = visibility::deprecated},
      5_GiB)
  , partition_autobalancing_concurrent_moves(
      *this,
      "partition_autobalancing_concurrent_moves",
      "Number of partitions that can be reassigned at once.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      50)
  , partition_autobalancing_tick_moves_drop_threshold(
      *this,
      "partition_autobalancing_tick_moves_drop_threshold",
      "If the number of scheduled tick moves drops by this ratio, a new tick "
      "is scheduled immediately. Valid values are (0, 1]. For example, with a "
      "value of 0.2 and 100 scheduled moves in a tick, a new tick is scheduled "
      "when the in-progress moves are fewer than 80.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      0.2,
      &validate_0_to_1_ratio)
  , partition_autobalancing_min_size_threshold(
      *this,
      "partition_autobalancing_min_size_threshold",
      "Minimum size of partition that is going to be prioritized when "
      "rebalancing a cluster due to the disk size threshold being breached. "
      "This value is calculated automatically by default.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt)
  , partition_autobalancing_topic_aware(
      *this,
      "partition_autobalancing_topic_aware",
      "If `true`, Redpanda prioritizes balancing a topic’s partition replica "
      "count evenly across all brokers while it’s balancing the cluster’s "
      "overall partition count. Because different topics in a cluster can have "
      "vastly different load profiles, this better distributes the workload of "
      "the most heavily-used topics evenly across brokers.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      true)
  , enable_leader_balancer(
      *this,
      "enable_leader_balancer",
      "Enable automatic leadership rebalancing.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      true)
  , leader_balancer_mode(
      *this,
      "leader_balancer_mode",
      "Mode of the leader balancer optimization strategy. "
      "`calibrated` uses a heuristic that balances leaders based on replica "
      "counts per shard. `random` randomly moves leaders to reduce load on "
      "heavily-loaded shards. Legacy values `greedy_balanced_shards` and "
      "`random_hill_climbing` are treated as `calibrated`.",
      {.needs_restart = needs_restart::no,
       .example = model::leader_balancer_mode_to_string(
         model::leader_balancer_mode::calibrated),
       .visibility = visibility::user},
      model::leader_balancer_mode::calibrated,
      {
        model::leader_balancer_mode::calibrated,
        model::leader_balancer_mode::random,
        model::leader_balancer_mode::greedy,
      })
  , leader_balancer_idle_timeout(
      *this,
      "leader_balancer_idle_timeout",
      "Leadership rebalancing idle timeout.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      2min)
  , leader_balancer_mute_timeout(
      *this,
      "leader_balancer_mute_timeout",
      "Leadership rebalancing mute timeout.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      5min)
  , leader_balancer_node_mute_timeout(
      *this,
      "leader_balancer_node_mute_timeout",
      "Leadership rebalancing node mute timeout.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      20s)
  , leader_balancer_transfer_limit_per_shard(
      *this,
      "leader_balancer_transfer_limit_per_shard",
      "Per shard limit for in-progress leadership transfers.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      512,
      {.min = 1, .max = 2048})
  , default_leaders_preference(
      *this,
      [](const config::leaders_preference& v) {
          return v != config::leaders_preference{};
      },
      "default_leaders_preference",
      "Default settings for preferred location of topic partition leaders. "
      "It can be either \"none\" (no preference), "
      "or \"racks:<rack1>,<rack2>,...\" (prefer brokers with rack id from the "
      "list).",
      meta{
        .needs_restart = needs_restart::no,
        .visibility = visibility::user,
      },
      config::leaders_preference{})
  , core_balancing_on_core_count_change(
      *this,
      "core_balancing_on_core_count_change",
      "If set to `true`, and if after a restart the number of cores changes, "
      "Redpanda will move partitions between cores to maintain balanced "
      "partition distribution.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      true)
  , core_balancing_continuous(
      *this,
      true,
      false,
      "core_balancing_continuous",
      "If set to `true`, move partitions between cores in runtime to maintain "
      "balanced partition distribution.",
      meta{
        .needs_restart = needs_restart::no,
        .visibility = visibility::user,
      },
      true,
      property<bool>::noop_validator,
      legacy_default<bool>{false, legacy_version{16}})
  , core_balancing_debounce_timeout(
      *this,
      "core_balancing_debounce_timeout",
      "Interval, in milliseconds, between trigger and invocation of core "
      "balancing.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10s)
  , internal_topic_replication_factor(
      *this,
      "internal_topic_replication_factor",
      "Target replication factor for internal topics.",
      {.visibility = visibility::user},
      3)
  , health_manager_tick_interval(
      *this,
      "health_manager_tick_interval",
      "How often the health manager runs.",
      {.visibility = visibility::tunable},
      3min)
  , health_monitor_tick_interval(
      *this,
      "health_monitor_tick_interval",
      "How often health monitor refresh cluster state",
      {.needs_restart = needs_restart::no,
       .visibility = visibility::deprecated},
      10s)
  , health_monitor_max_metadata_age(
      *this,
      "health_monitor_max_metadata_age",
      "Maximum age of the metadata cached in the health monitor of a "
      "non-controller broker.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10s)
  , health_monitor_metrics_enabled(
      *this,
      "health_monitor_metrics_enabled",
      "Whether the cluster_health_* metrics are exported. When enabled, each "
      "broker refreshes them on an interval of 10x "
      "`health_monitor_max_metadata_age`, so the two scale together. "
      "Refreshing "
      "increases CPU usage and network traffic. Disabled by default.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      false)
  , storage_space_alert_free_threshold_percent(
      *this,
      "storage_space_alert_free_threshold_percent",
      "Threshold of minimum percent free space before setting storage space "
      "alert.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      5,
      {.min = 0, .max = 50})
  , storage_space_alert_free_threshold_bytes(
      *this,
      "storage_space_alert_free_threshold_bytes",
      "Threshold of minimum bytes free space before setting storage space "
      "alert.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      0,
      {.min = 0})
  , storage_min_free_bytes(
      *this,
      "storage_min_free_bytes",
      "Threshold of minimum bytes free space before rejecting producers.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      5_GiB,
      {.min = 10_MiB})
  , storage_strict_data_init(
      *this,
      "storage_strict_data_init",
      "Requires that an empty file named `.redpanda_data_dir` be present in "
      "the broker configuration `data_directory`. If set to `true`, Redpanda "
      "will refuse to start if the file is not found in the data directory.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , alive_timeout_ms(
      *this,
      "alive_timeout_ms",
      "The amount of time since the last broker status heartbeat. After this "
      "time, a broker is considered offline and not alive.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      5s)
  , memory_abort_on_alloc_failure(
      *this,
      "memory_abort_on_alloc_failure",
      "If `true`, the Redpanda process will terminate immediately when an "
      "allocation cannot be satisfied due to memory exhaustion. If false, an "
      "exception is thrown.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      true)
  , sampled_memory_profile(
      *this,
      "memory_enable_memory_sampling",
      "When `true`, memory allocations are sampled and tracked. A sampled live "
      "set of allocations can then be retrieved from the Admin API. "
      "Additionally, Redpanda will periodically log the top-n allocation "
      "sites.",
      {// Enabling/Disabling this dynamically doesn't make much sense as for the
       // memory profile to be meaningful you'll want to have this on from the
       // beginning. However, we still provide the option to be able to disable
       // it dynamically in case something goes wrong
       .needs_restart = needs_restart::no,
       .visibility = visibility::tunable},
      true)
  , enable_metrics_reporter(
      *this,
      "enable_metrics_reporter",
      "Enable the cluster metrics reporter. If `true`, the metrics reporter "
      "collects and exports to Redpanda Data a set of customer usage metrics "
      "at the interval set by `metrics_reporter_report_interval`. The cluster "
      "metrics of the metrics reporter are different from the monitoring "
      "metrics. * The metrics reporter exports customer usage metrics for "
      "consumption by Redpanda Data.* Monitoring metrics are exported for "
      "consumption by Redpanda users.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      true)
  , metrics_reporter_tick_interval(
      *this,
      "metrics_reporter_tick_interval",
      "Cluster metrics reporter tick interval.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1min)
  , metrics_reporter_report_interval(
      *this,
      "metrics_reporter_report_interval",
      "Cluster metrics reporter report interval.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      24h)
  , metrics_reporter_url(
      *this,
      "metrics_reporter_url",
      "URL of the cluster metrics reporter.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      "https://m.rp.vectorized.io/v2")
  , features_auto_enable(
      *this,
      "features_auto_enable",
      "Whether features whose `available_policy` is `always` or "
      "`new_clusters_only` are auto-activated by the controller after the "
      "cluster active logical version reaches their required version. When "
      "false, the cluster active version still advances normally, but each "
      "such feature must be activated explicitly via the Admin API. Does not "
      "affect features with `available_policy::explicit_only`, which always "
      "require explicit activation.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      true)
  , features_auto_finalization(
      *this,
      false, /* restricted value: license required to disable */
      "features_auto_finalization",
      "Whether the cluster active logical version is advanced automatically "
      "once all nodes have been upgraded (true), or only in response to an "
      "explicit request via the Admin API (false). When false, the cluster "
      "remains able to downgrade to the previous version until finalization "
      "is requested. Setting this to false is an Enterprise feature and "
      "requires a valid license. Note: if upgrade was performed with this "
      "set to false and the cluster is ready to finalize, flipping this to "
      "true does not reliably trigger finalization. Leave this set to false "
      "and use the Admin API to finalize; once the upgrade is complete this "
      "can be set back to true to restore automatic finalization for future "
      "upgrades.",
      meta{.needs_restart = needs_restart::no, .visibility = visibility::user},
      true)
  , enable_rack_awareness(
      *this,
      "enable_rack_awareness",
      "Enable rack-aware replica assignment.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , node_status_interval(
      *this,
      "node_status_interval",
      "Time interval between two node status messages. Node status messages "
      "establish liveness status outside of the Raft protocol.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      100ms)
  , node_status_reconnect_max_backoff_ms(
      *this,
      "node_status_reconnect_max_backoff_ms",
      "Maximum backoff (in milliseconds) to reconnect to an unresponsive peer "
      "during node status liveness checks.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      15s)
  , enable_controller_log_rate_limiting(
      *this,
      "enable_controller_log_rate_limiting",
      "Limits the write rate for the controller log.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , rps_limit_topic_operations(
      *this,
      "rps_limit_topic_operations",
      "Rate limit for controller topic operations.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1000)
  , controller_log_accummulation_rps_capacity_topic_operations(
      *this,
      "controller_log_accummulation_rps_capacity_topic_operations",
      "Maximum capacity of rate limit accumulation"
      "in controller topic operations limit",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt)
  , rps_limit_acls_and_users_operations(
      *this,
      "rps_limit_acls_and_users_operations",
      "Rate limit for controller ACLs and user's operations.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1000)
  , controller_log_accummulation_rps_capacity_acls_and_users_operations(
      *this,
      "controller_log_accummulation_rps_capacity_acls_and_users_operations",
      "Maximum capacity of rate limit accumulation in controller ACLs and "
      "users operations limit.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt)
  , rps_limit_node_management_operations(
      *this,
      "rps_limit_node_management_operations",
      "Rate limit for controller node management operations.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1000)
  , controller_log_accummulation_rps_capacity_node_management_operations(
      *this,
      "controller_log_accummulation_rps_capacity_node_management_operations",
      "Maximum capacity of rate limit accumulation in controller node "
      "management operations limit.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt)
  , rps_limit_move_operations(
      *this,
      "rps_limit_move_operations",
      "Rate limit for controller move operations.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1000)
  , controller_log_accummulation_rps_capacity_move_operations(
      *this,
      "controller_log_accummulation_rps_capacity_move_operations",
      "Maximum capacity of rate limit accumulation in controller move "
      "operations limit.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt)
  , rps_limit_configuration_operations(
      *this,
      "rps_limit_configuration_operations",
      "Rate limit for controller configuration operations.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1000)
  , controller_log_accummulation_rps_capacity_configuration_operations(
      *this,
      "controller_log_accummulation_rps_capacity_configuration_operations",
      "Maximum capacity of rate limit accumulation in controller configuration "
      "operations limit.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt)
  , kafka_throughput_limit_node_in_bps(
      *this,
      "kafka_throughput_limit_node_in_bps",
      "The maximum rate of all ingress Kafka API traffic for a node. Includes "
      "all Kafka API traffic (requests, responses, headers, fetched data, "
      "produced data, etc.). If `null`, the property is disabled, and traffic "
      "is not limited.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt,
      {.min = 1})
  , kafka_throughput_limit_node_out_bps(
      *this,
      "kafka_throughput_limit_node_out_bps",
      "The maximum rate of all egress Kafka traffic for a node. Includes all "
      "Kafka API traffic (requests, responses, headers, fetched data, produced "
      "data, etc.). If `null`, the property is disabled, and traffic is not "
      "limited.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt,
      {.min = 1})
  , kafka_throughput_replenish_threshold(
      *this,
      "kafka_throughput_replenish_threshold",
      "Threshold for refilling the token bucket as part of enforcing "
      "throughput limits. This threshold is evaluated "
      "with each request for data. When the number of tokens to replenish "
      "exceeds this threshold, then tokens are added to the token bucket. This "
      "ensures that the atomic is not being updated for the token count with "
      "each request. The range for this threshold is automatically clamped to "
      "the corresponding throughput limit for ingress and egress.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt,
      {.min = 1})
  , kafka_throughput_controlled_api_keys(
      *this,
      "kafka_throughput_controlled_api_keys",
      "List of Kafka API keys that are subject to cluster-wide and node-wide "
      "throughput limit control.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      {"produce", "fetch"})
  , kafka_throughput_control(
      *this,
      "kafka_throughput_control",
      "List of throughput control groups that define exclusions from node-wide "
      "throughput limits. Clients excluded from node-wide throughput limits "
      "are still potentially subject to client-specific throughput limits. For "
      "more information see "
      "https://docs.redpanda.com/current/reference/properties/"
      "cluster-properties/#kafka_throughput_control.",
      {
        .needs_restart = needs_restart::no,
        .example
        = R"([{'name': 'first_group','client_id': 'client1'}, {'client_id': 'consumer-\d+'}, {'name': 'catch all'}])",
        .visibility = visibility::user,
      },
      {},
      [](auto& v) {
          return validate_throughput_control_groups(v.cbegin(), v.cend());
      })
  , node_isolation_heartbeat_timeout(
      *this,
      "node_isolation_heartbeat_timeout",
      "How long after the last heartbeat request a node will wait before "
      "considering itself to be isolated.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      3000,
      {.min = 100, .max = 10000})
  , controller_snapshot_max_age_sec(
      *this,
      "controller_snapshot_max_age_sec",
      "Maximum amount of time before Redpanda attempts to create a controller "
      "snapshot after a new controller command appears.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      60s)
  , legacy_permit_unsafe_log_operation(
      *this,
      "legacy_permit_unsafe_log_operation",
      "Flag to enable a Redpanda cluster operator to use unsafe control "
      "characters within strings, such as consumer group names or user names. "
      "This flag applies only for Redpanda clusters that were originally on "
      "version 23.1 or earlier and have been upgraded to version 23.2 or "
      "later. Starting in version 23.2, newly-created Redpanda clusters ignore "
      "this property.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      true)
  , legacy_unsafe_log_warning_interval_sec(
      *this,
      "legacy_unsafe_log_warning_interval_sec",
      "Period at which to log a warning about using unsafe strings containing "
      "control characters. If unsafe strings are permitted by "
      "`legacy_permit_unsafe_log_operation`, a warning will be logged at an "
      "interval specified by this property.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      300s)
  , pp_sr_smp_max_non_local_requests(
      *this,
      "pp_sr_smp_max_non_local_requests",
      "Maximum number of Cross-core(Inter-shard communication) requests "
      "pending in HTTP Proxy and Schema Registry seastar::smp group. (For more "
      "details, see the `seastar::smp_service_group` documentation).",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      std::nullopt)
  , kafka_memory_share_for_fetch(
      *this,
      "kafka_memory_share_for_fetch",
      "The share of Kafka subsystem memory that can be used for fetch read "
      "buffers, as a fraction of the Kafka subsystem memory amount.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::user},
      0.5,
      {.min = 0.0, .max = 1.0})
  , cpu_profiler_enabled(
      *this,
      "cpu_profiler_enabled",
      "Enables CPU profiling for Redpanda.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , cpu_profiler_sample_period_ms(
      *this,
      "cpu_profiler_sample_period_ms",
      "The sample period for the CPU profiler.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      100ms,
      {.min = 1ms})
  , rpk_path(
      *this,
      "rpk_path",
      "Path to RPK binary",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      "/usr/bin/rpk")
  , debug_bundle_storage_dir(
      *this,
      "debug_bundle_storage_dir",
      "Path to the debug bundle storage directory. Note: Changing this path "
      "does not clean up existing debug bundles. If not set, the debug bundle "
      "is stored in the Redpanda data directory specified in the redpanda.yaml "
      "broker configuration file.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt)
  , debug_bundle_auto_removal_seconds(
      *this,
      "debug_bundle_auto_removal_seconds",
      "If set, how long debug bundles are kept in the debug bundle storage "
      "directory after they are created. If not set, debug bundles are kept "
      "indefinitely.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt)
  , oidc_discovery_url(
      *this,
      "oidc_discovery_url",
      "The URL pointing to the well-known discovery endpoint for the OIDC "
      "provider.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      "https://auth.prd.cloud.redpanda.com/.well-known/openid-configuration",
      [](const auto& v) -> std::optional<ss::sstring> {
          auto res = security::oidc::parse_url(v);
          if (res.has_error()) {
              return res.error().message();
          }
          return std::nullopt;
      })
  , oidc_http_proxy_url(
      *this,
      "oidc_http_proxy_url",
      "URL of the HTTP forward proxy used for OIDC discovery and JWKS "
      "fetches. Accepts http://host:port or https://host:port. When "
      "set, oidc_discovery_url must use https:// — plaintext OIDC "
      "origins cannot be routed through a forward proxy.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt,
      [](const auto& v) -> std::optional<ss::sstring> {
          if (!v.has_value()) {
              return std::nullopt;
          }
          auto res = security::oidc::parse_url(*v);
          if (res.has_error()) {
              return res.error().message();
          }
          return std::nullopt;
      })
  , oidc_http_proxy_username(
      *this,
      "oidc_http_proxy_username",
      "Username for HTTP Basic authentication to the OIDC forward proxy "
      "(oidc_http_proxy_url). Leave unset for an unauthenticated proxy. "
      "Both username and password must be set for credentials to be sent.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt,
      [](const auto& v) -> std::optional<ss::sstring> {
          if (!v.has_value()) {
              return std::nullopt;
          }
          if (v->find(':') != ss::sstring::npos) {
              return "must not contain ':' (RFC 7617)";
          }
          if (std::ranges::any_of(*v, absl::ascii_iscntrl)) {
              return "must not contain control characters";
          }
          return std::nullopt;
      })
  , oidc_http_proxy_password(
      *this,
      "oidc_http_proxy_password",
      "Password for HTTP Basic authentication to the OIDC forward proxy "
      "(oidc_http_proxy_url). Leave unset for an unauthenticated proxy. "
      "Both username and password must be set for credentials to be sent.",
      {.needs_restart = needs_restart::no,
       .visibility = visibility::user,
       .secret = is_secret::yes},
      std::nullopt,
      [](const auto& v) -> std::optional<ss::sstring> {
          if (!v.has_value()) {
              return std::nullopt;
          }
          if (std::ranges::any_of(*v, absl::ascii_iscntrl)) {
              return "must not contain control characters";
          }
          return std::nullopt;
      })
  , oidc_token_audience(
      *this,
      "oidc_token_audience",
      "A string representing the intended recipient of the token.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      "redpanda")
  , oidc_clock_skew_tolerance(
      *this,
      "oidc_clock_skew_tolerance",
      "The amount of time (in seconds) to allow for when validating the expiry "
      "claim in the token.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::chrono::seconds{} * 30)
  , oidc_principal_mapping(
      *this,
      "oidc_principal_mapping",
      "Rule for mapping JWT payload claim to a Redpanda user principal.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      "$.sub",
      security::oidc::validate_principal_mapping_rule)
  , oidc_keys_refresh_interval(
      *this,
      "oidc_keys_refresh_interval",
      "The frequency of refreshing the JSON Web Keys (JWKS) used to validate "
      "access tokens.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      1h)
  , oidc_group_claim_path(
      *this,
      "oidc_group_claim_path",
      "JSON path to extract groups from the JWT payload.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      "$.groups",
      security::oidc::validate_group_claim_path)
  , nested_group_behavior(
      *this,
      "nested_group_behavior",
      "Behavior for handling nested groups when extracting groups from "
      "authentication tokens.  Two options are available - none and suffix.  "
      "With none, the group is left alone (e.g. '/group/child/grandchild').  "
      "Suffix will extract the final component from the nested group (e.g. "
      "'/group' -> 'group' and '/group/child/grandchild' -> 'grandchild').",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      security::oidc::nested_group_behavior::none,
      {security::oidc::nested_group_behavior::none,
       security::oidc::nested_group_behavior::suffix})
  , http_authentication(
      *this,
      "OIDC",
      "http_authentication",
      "A list of supported HTTP authentication mechanisms. Accepted Values: "
      "`BASIC`, `OIDC`",
      meta{
        .needs_restart = needs_restart::no,
        .visibility = visibility::user,
      },
      std::vector<ss::sstring>{"BASIC"},
      validate_http_authn_mechanisms)
  , enable_mpx_extensions(
      *this,
      "enable_mpx_extensions",
      "Enable Redpanda extensions for MPX.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      false)
  , virtual_cluster_min_producer_ids(
      *this,
      "virtual_cluster_min_producer_ids",
      "Minimum number of active producers per virtual cluster.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::numeric_limits<uint64_t>::max(),
      {.min = 1})
  , unsafe_enable_consumer_offsets_delete_retention(
      *this,
      "unsafe_enable_consumer_offsets_delete_retention",
      "Enables delete retention of consumer offsets topic. This is an "
      "internal-only configuration and should be enabled only after consulting "
      "with Redpanda support.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::user},
      false)
  , tls_min_version(
      *this,
      "tls_min_version",
      "The minimum TLS version that Redpanda clusters support. This property "
      "prevents client applications from negotiating a downgrade to the TLS "
      "version when they make a connection to a Redpanda cluster.",
      {.needs_restart = needs_restart::yes,
       .visibility = visibility::user,
       .usable_before_ready = usable_before_ready::yes},
      tls_version::v1_2,
      {tls_version::v1_0,
       tls_version::v1_1,
       tls_version::v1_2,
       tls_version::v1_3})
  , tls_enable_renegotiation(
      *this,
      "tls_enable_renegotiation",
      "TLS client-initiated renegotiation is considered unsafe and is by "
      "default disabled.  Only re-enable it if you are experiencing issues "
      "with your TLS-enabled client.  This option has no effect on TLSv1.3 "
      "connections as client-initiated renegotiation was removed.",
      {.needs_restart = needs_restart::yes,
       .visibility = visibility::tunable,
       .usable_before_ready = usable_before_ready::yes},
      false)
  , tls_v1_2_cipher_suites(
      *this,
      "tls_v1_2_cipher_suites",
      "Specifies the TLS 1.2 cipher suites available for external client "
      "connections as a colon-separated OpenSSL-compatible list. Configure "
      "this property to support legacy clients.",
      {.needs_restart = needs_restart::yes,
       .visibility = visibility::user,
       .usable_before_ready = usable_before_ready::yes},
      ss::sstring{net::tls_v1_2_cipher_suites},
      [](ss::sstring s) -> std::optional<ss::sstring> {
          if (!validate_tls_v1_2_cipher_suites(s)) {
              return ssx::sformat("Invalid cipher suites: {}", s);
          }
          return std::nullopt;
      })
  , tls_v1_3_cipher_suites(
      *this,
      "tls_v1_3_cipher_suites",
      "Specifies the TLS 1.3 cipher suites available for external client "
      "connections as a colon-separated OpenSSL-compatible list. Most "
      "deployments don't need to modify this setting. Configure this property "
      "only for specific organizational security policies.",
      {.needs_restart = needs_restart::yes,
       .visibility = visibility::user,
       .usable_before_ready = usable_before_ready::yes},
      ss::sstring{net::tls_v1_3_cipher_suites},
      [](ss::sstring s) -> std::optional<ss::sstring> {
          if (!validate_tls_v1_3_cipher_suites(s)) {
              return ssx::sformat("Invalid cipher suites: {}", s);
          }
          return std::nullopt;
      })

  , enable_host_metrics(
      *this,
      "enable_host_metrics",
      "Enable exporting of some host metrics like /proc/diskstats, /proc/snmp "
      "and /proc/net/netstat",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      true)
  , consumer_offsets_topic_batch_cache_enabled(
      *this,
      "consumer_offsets_topic_batch_cache_enabled",
      "This property lets you enable the batch cache for the consumer offsets "
      "topic. By default, the cache for consumer offsets topic is disabled. "
      "Changing this property is not recommended in production systems, as it "
      "may affect performance. The change is applied only after the restart.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      false)
  , internal_rpc_request_timeout_ms(
      *this,
      "internal_rpc_request_timeout_ms",
      "Default timeout for RPC requests between Redpanda nodes.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10s)
  , code_hugepages_enabled(
      *this,
      "code_hugepages_enabled",
      "Map the binary into hugepages",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      false)
  , development_feature_property_testing_only(
      *this,
      "development_feature_property_testing_only",
      "Development feature property for testing only.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , enable_developmental_unrecoverable_data_corrupting_features(
      *this,
      "enable_developmental_unrecoverable_data_corrupting_features",
      "Development features should never be enabled in a production cluster, "
      "or any cluster where stability, data loss, or the ability to upgrade "
      "are a concern. To enable experimental features, set the value of this "
      "configuration option to the current unix epoch expressed in seconds. "
      "The value must be within one hour of the current time on the broker. "
      "Once experimental features are enabled they cannot be disabled.",
      {.needs_restart = needs_restart::no,
       .visibility = visibility::tunable,
       .usable_before_ready = usable_before_ready::yes},
      "",
      [this](const ss::sstring& v) -> std::optional<ss::sstring> {
          if (development_features_enabled()) {
              return fmt::format(
                "Development feature flag cannot be changed once enabled.");
          }

          const auto time_since_epoch
            = std::chrono::system_clock::now().time_since_epoch();

          try {
              const auto key = std::chrono::seconds(
                boost::lexical_cast<int64_t>(v));

              const auto dur = std::chrono::abs(time_since_epoch - key);
              if (dur > std::chrono::hours(1)) {
                  return fmt::format(
                    "Invalid key '{}'. Must be within 1 hour of the current "
                    "unix epoch in seconds.",
                    key.count());
              }
          } catch (const boost::bad_lexical_cast&) {
              return fmt::format("Could not convert '{}' to integer", v);
          }

          return std::nullopt;
      }) {}

configuration::error_map_t configuration::load(const YAML::Node& root_node) {
    if (!root_node["redpanda"]) {
        throw std::invalid_argument("'redpanda' root is required");
    }

    return config_store::read_yaml(root_node["redpanda"]);
}

std::unique_ptr<configuration> make_config() {
    // Constructing `configuration` requires about 90KB of stack space in debug.
    // In some tests this makes us run out of stack space/into stackoverflows as
    // the default stack for ss::thread is 128KiB.
    //
    // Hence we construct the configuration object in a separate thread with
    // increased stack size.
    //
    // Also we want to keep the configuration object of the stack itself as it's
    // even larger (>90KB). Hence, we take the unique_ptr indirection and store
    // it on the heap. This also avoids having to rely on RVO and friends.
    //
    // Note this is above our usual max allocation limit of 128KiB but this
    // isn't much of an issue here as this happens on startup (and also before
    // the warning threshold is set up)
    //
    // Note all of this only happens when running with the reactor active and on
    // a ss::thread.  ss::thread requires the reactor to be inited. This isn't
    // the case in all tests (BOOST_AUTO_TEST_CASE). Further, otherwise we are
    // running on a native posix thread with large stack anyway so this isn't an
    // issue.
    auto make_cfg = []() { return std::make_unique<configuration>(); };
    if (seastar::engine_is_ready() && ss::thread::running_in_thread()) {
        ss::thread_attributes attrs;
        attrs.stack_size = 512_KiB;
        return ss::async(attrs, make_cfg).get();
    } else {
        return make_cfg();
    }
}

configuration& shard_local_cfg() {
    static thread_local std::unique_ptr<configuration> cfg = make_config();
    return *cfg;
}
} // namespace config
