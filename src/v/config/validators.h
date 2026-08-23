#pragma once

#include "base/seastarx.h"
#include "config/configuration.h"
#include "config/types.h"

#include <seastar/core/sstring.hh>

#include <optional>
#include <vector>

namespace config {

std::optional<std::pair<ss::sstring, int64_t>>
parse_connection_rate_override(const ss::sstring& raw_option);

std::optional<ss::sstring>
validate_connection_rate(const std::vector<ss::sstring>& ips_with_limit);

std::optional<ss::sstring>
validate_sasl_mechanisms(const std::vector<ss::sstring>& mechanisms);

std::optional<ss::sstring> validate_sasl_mechanisms_overrides(
  const std::vector<config::sasl_mechanisms_override>& overrides);

std::optional<ss::sstring>
validate_http_authn_mechanisms(const std::vector<ss::sstring>& mechanisms);

bool oidc_is_enabled_http();
bool oidc_is_enabled_kafka();

std::optional<ss::sstring> validate_0_to_1_ratio(const double d);

std::optional<ss::sstring>
validate_non_empty_string_vec(const std::vector<ss::sstring>&);

std::optional<ss::sstring>
validate_non_empty_string_opt(const std::optional<ss::sstring>&);

std::optional<ss::sstring>
validate_audit_event_types(const std::vector<ss::sstring>& vs);

std::optional<ss::sstring>
validate_audit_excluded_topics(const std::vector<ss::sstring>&);

std::optional<ss::sstring>
validate_api_endpoint(const std::optional<ss::sstring>& os);

std::optional<ss::sstring> validate_tombstone_retention_ms(
  const std::optional<std::chrono::milliseconds>& ms);

std::optional<ss::sstring>
validate_consumer_group_metrics(const std::vector<ss::sstring>& metrics);

std::optional<ss::sstring>
validate_cloud_storage_cluster_name(const std::optional<ss::sstring>&);

std::optional<ss::sstring>
validate_sane_partition_balancer_timeouts(const configuration& config);

std::optional<ss::sstring>
validate_default_redpanda_storage_mode(const configuration& config);

std::optional<ss::sstring>
validate_oidc_http_proxy_url(const configuration& config);

}; // namespace config
