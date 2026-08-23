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

#include "config/validators.h"

#include "absl/container/flat_hash_set.h"
#include "absl/container/node_hash_set.h"
#include "config/configuration.h"
#include "config/sasl_mechanisms.h"
#include "config/types.h"
#include "model/namespace.h"
#include "model/validation.h"
#include "security/oidc_url_parser.h"
#include "serde/rw/chrono.h"
#include "ssx/sformat.h"
#include "utils/inet_address_wrapper.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <optional>

namespace config {

std::optional<std::pair<ss::sstring, int64_t>>
split_string_int_by_colon(const ss::sstring& raw_option) {
    auto del_pos = raw_option.find(":");
    if (del_pos == ss::sstring::npos || del_pos == raw_option.size() - 1) {
        return std::nullopt;
    }

    auto str_val = raw_option.substr(0, del_pos);
    auto int_val = raw_option.substr(del_pos + 1);

    std::pair<ss::sstring, int64_t> ans;
    ans.first = str_val;
    try {
        ans.second = std::stoll(int_val);
    } catch (...) {
        return std::nullopt;
    }
    return ans;
}

std::optional<std::pair<ss::sstring, int64_t>>
parse_connection_rate_override(const ss::sstring& raw_option) {
    return split_string_int_by_colon(raw_option);
}

std::optional<ss::sstring>
validate_connection_rate(const std::vector<ss::sstring>& ips_with_limit) {
    absl::node_hash_set<net::inet_address_wrapper> ip_set;
    for (const auto& ip_and_limit : ips_with_limit) {
        auto parsing_setting = parse_connection_rate_override(ip_and_limit);
        if (!parsing_setting) {
            return fmt::format(
              "Can not parse connection_rate override {}", ip_and_limit);
        }

        ss::net::inet_address addr;
        try {
            addr = ss::net::inet_address(parsing_setting->first);
        } catch (...) {
            return fmt::format(
              "Looks like {} is not ip", parsing_setting->first);
        }

        if (!ip_set.insert(addr).second) {
            return fmt::format(
              "Duplicate setting for ip: {}", parsing_setting->first);
        }
    }

    return std::nullopt;
}

std::optional<ss::sstring>
validate_sasl_mechanisms(const std::vector<ss::sstring>& mechanisms) {
    // Validate results
    for (const auto& m : mechanisms) {
        if (!std::ranges::contains(supported_sasl_mechanisms, m)) {
            return ssx::sformat("'{}' is not a supported SASL mechanism", m);
        }
    }

    const auto contains = [&mechanisms](const std::string_view& s) {
        return std::ranges::contains(mechanisms, s);
    };

    if (contains(plain) && !contains(scram)) {
        return ssx::sformat(
          "{} mechanism must be enabled if {} is enabled", scram, plain);
    }
    return std::nullopt;
}

std::optional<ss::sstring> validate_sasl_mechanisms_overrides(
  const std::vector<config::sasl_mechanisms_override>& overrides) {
    for (const auto& overide : overrides) {
        const auto error = validate_sasl_mechanisms(overide.sasl_mechanisms);
        if (error.has_value()) {
            return ssx::sformat(
              "Invalid sasl mechanisms override for listener '{}'. Error: {}",
              overide.listener,
              error.value());
        }
    }
    return std::nullopt;
}

std::optional<ss::sstring>
validate_http_authn_mechanisms(const std::vector<ss::sstring>& mechanisms) {
    constexpr auto supported = std::to_array<std::string_view>(
      {"BASIC", "OIDC"});

    // Validate results
    for (const auto& m : mechanisms) {
        if (std::ranges::none_of(supported, [&m](const auto& s) {
                return s == m;
            })) {
            return ssx::sformat(
              "'{}' is not a supported HTTP authentication mechanism", m);
        }
    }
    return std::nullopt;
}

bool oidc_is_enabled_http() {
    return std::ranges::any_of(
      config::shard_local_cfg().http_authentication(),
      [](const auto& m) { return m == "OIDC"; });
}

bool oidc_is_enabled_kafka() { return has_sasl_mechanism(oauthbearer); }

std::optional<ss::sstring> validate_0_to_1_ratio(const double d) {
    if (d < 0 || d > 1) {
        return fmt::format("Ratio must be in the [0,1] range, got: {}", d);
    }
    return std::nullopt;
}

std::optional<ss::sstring>
validate_non_empty_string_vec(const std::vector<ss::sstring>& vs) {
    for (const auto& s : vs) {
        if (s.empty()) {
            return "Empty strings are not valid in this collection";
        }
    }
    return std::nullopt;
}

std::optional<ss::sstring>
validate_non_empty_string_opt(const std::optional<ss::sstring>& os) {
    if (os.has_value() && os.value().empty()) {
        return "Empty string is not valid";
    } else {
        return std::nullopt;
    }
}

std::optional<ss::sstring>
validate_audit_event_types(const std::vector<ss::sstring>& vs) {
    /// TODO: Should match stringified enums in kafka/types.h
    static const absl::flat_hash_set<ss::sstring> audit_event_types{
      "management",
      "produce",
      "consume",
      "describe",
      "heartbeat",
      "authenticate",
      "admin"};

    for (const auto& e : vs) {
        if (!audit_event_types.contains(e)) {
            return ss::format("Unsupported audit event type passed: {}", e);
        }
    }
    return std::nullopt;
}

std::optional<ss::sstring>
validate_audit_excluded_topics(const std::vector<ss::sstring>& vs) {
    bool is_kafka_audit_topic = false;
    std::optional<ss::sstring> is_invalid_topic_name = std::nullopt;
    if (
      std::any_of(
        vs.begin(),
        vs.end(),
        [&is_kafka_audit_topic,
         &is_invalid_topic_name](const ss::sstring& topic_name) {
            auto t = model::topic{topic_name};
            if (t == model::kafka_audit_logging_topic) {
                is_kafka_audit_topic = true;
            } else if (model::validate_kafka_topic_name(t)) {
                is_invalid_topic_name = topic_name;
            }
            return is_kafka_audit_topic || is_invalid_topic_name.has_value();
        })) {
        if (is_kafka_audit_topic) {
            return ss::format(
              "Unable to exclude audit log '{}' from auditing",
              model::kafka_audit_logging_topic);
        } else if (is_invalid_topic_name.has_value()) {
            return ss::format(
              "{} is an invalid topic name", *is_invalid_topic_name);
        }
    }

    return std::nullopt;
}

std::optional<ss::sstring>
validate_api_endpoint(const std::optional<ss::sstring>& os) {
    if (
      auto non_empty_string_opt = validate_non_empty_string_opt(os);
      non_empty_string_opt.has_value()) {
        return non_empty_string_opt;
    }

    if (
      os.has_value()
      && (os.value().starts_with("http://") || os.value().starts_with("https://"))) {
        return "String starting with URL protocol is not valid";
    }

    return std::nullopt;
}

std::optional<ss::sstring> validate_tombstone_retention_ms(
  const std::optional<std::chrono::milliseconds>& ms) {
    if (ms.has_value()) {
        if (ms.value() < 1ms || ms.value() > serde::max_serializable_ms) {
            return fmt::format(
              "tombstone_retention_ms should be in range: [1, {}]",
              serde::max_serializable_ms);
        }
    }

    return std::nullopt;
}

std::optional<ss::sstring>
validate_consumer_group_metrics(const std::vector<ss::sstring>& metrics) {
    constexpr auto supported = std::to_array<std::string_view>(
      {"group", "partition", "consumer_lag"});

    // Validate results
    for (const auto& m : metrics) {
        if (std::ranges::none_of(supported, [&m](const auto& s) {
                return s == m;
            })) {
            return ssx::sformat("'{}' is not a valid consumer group metric", m);
        }
    }

    return std::nullopt;
}

std::optional<ss::sstring>
validate_sane_partition_balancer_timeouts(const configuration& config) {
    // how often node status sends an rpc
    auto node_status = config.node_status_interval();
    // in pbp, if 7 consecutive node statuses are missed, the node is considered
    // down for the purposes of determining alive / dead quorums
    auto node_unresponsiveness = 7 * node_status;
    // how often the partition balancer runs
    auto pbp_tick_interval = config.partition_autobalancing_tick_interval_ms();
    // timeout after which the partition balancer will start draining partitions
    // from a node
    auto node_availability
      = config.partition_autobalancing_node_availability_timeout_sec();
    // timeout or nullopt. If timeout, its the timeout after which the partition
    // balancer may consider a node for automatic decommissioning
    auto maybe_auto_decom_timeout
      = config.partition_autobalancing_node_autodecommission_timeout_sec();
    // how often the health report refreshes of its own accord
    auto health_report_tick_time = config.health_monitor_tick_interval();

    // pbp is written under the assumption that node_status << pbp_tick_interval
    if (node_status > pbp_tick_interval) {
        return fmt::format(
          "node_status_interval ({}) must be less than or equal to "
          "partition_autobalancing_tick_interval_ms ({})",
          node_status,
          pbp_tick_interval);
    }

    // node_unresponsiveness is when a node is considered down from the
    // perspective of quorum liveness, aka pbp will consider partitions
    // immutable if their source quorum is down.
    // node_availability is the time at which a node is preemptively drained of
    // replicas. The sane assumption is that node_unresponsiveness <
    // node_availability
    if (node_unresponsiveness > node_availability) {
        return fmt::format(
          "node_status_interval * 7 ({}) should be less than "
          "partition_autobalancing_node_availability_timeout_sec ({})",
          node_unresponsiveness,
          node_availability);
    }

    // pbp relies on health reports for feedback on how its last round of
    // balancing actions worked out. If the health report interval is greater
    // than the pbp_tick interval, pbp may exhibit oscillating behavior where it
    // overshoots balance. It is best to keep the health report interval lower
    // than the pbp tick
    if (pbp_tick_interval < health_report_tick_time) {
        return fmt::format(
          "health_monitor_tick_interval ({}) must be less than "
          "partition_autobalancing_tick_interval_ms ({})",
          health_report_tick_time,
          pbp_tick_interval);
    }

    // sanity check, we should drain but not decommission a node before we
    // attempt to eject it from the cluster
    if (
      maybe_auto_decom_timeout
      && std::chrono::duration_cast<std::chrono::seconds>(node_availability)
           > maybe_auto_decom_timeout) {
        return fmt::format(
          "partition_autobalancing_node_availability_timeout_sec ({}) must be "
          "less than partition_autobalancing_node_autodecommission_timeout_sec "
          "({})",
          node_availability,
          *maybe_auto_decom_timeout);
    }
    return std::nullopt;
}

std::optional<ss::sstring>
validate_oidc_http_proxy_url(const config::configuration& config) {
    // Only https oidc discovery URLs are supported when an HTTP proxy is
    // configured
    if (!config.oidc_http_proxy_url().has_value()) {
        return std::nullopt;
    }
    auto discovery = security::oidc::parse_url(config.oidc_discovery_url());
    if (discovery.has_error()) {
        // The per-property oidc_discovery_url validator will flag
        // unparseable URLs; avoid double-reporting.
        return std::nullopt;
    }
    if (discovery.assume_value().scheme != "https") {
        return fmt::format(
          "oidc_http_proxy_url requires oidc_discovery_url to use https:// "
          "(got {})",
          discovery.assume_value().scheme);
    }
    return std::nullopt;
}

}; // namespace config
