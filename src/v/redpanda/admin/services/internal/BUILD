load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    "//src/v/redpanda:__subpackages__",
])

redpanda_cc_library(
    name = "debug",
    srcs = ["debug.cc"],
    hdrs = ["debug.h"],
    deps = [
        "//proto/redpanda/core/admin/internal/v1:debug_redpanda_proto",
        "//src/v/base",
        "//src/v/finjector",
        "//src/v/redpanda/admin/proxy:client",
        "//src/v/serde/protobuf:rpc",
        "//src/v/utils:to_string",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "breakglass",
    srcs = ["breakglass.cc"],
    hdrs = ["breakglass.h"],
    deps = [
        "//proto/redpanda/core/admin/internal/v1:breakglass_redpanda_proto",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/cluster:client_quota_backend",
        "//src/v/cluster:cloud_metadata_types",
        "//src/v/cluster:cluster_link_table",
        "//src/v/cluster:cluster_recovery_table",
        "//src/v/cluster:commands",
        "//src/v/cluster:controller_log_limiter",
        "//src/v/cluster:data_migration_group_proxy",
        "//src/v/cluster:data_migration_table",
        "//src/v/cluster:feature_backend",
        "//src/v/cluster:fwd",
        "//src/v/cluster:members_table",
        "//src/v/cluster:notification",
        "//src/v/cluster:rpc_utils",
        "//src/v/finjector",
        "//src/v/model",
        "//src/v/redpanda/admin/proxy:client",
        "//src/v/serde/protobuf:rpc",
        "//src/v/utils:to_string",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "metastore",
    srcs = ["metastore.cc"],
    hdrs = ["metastore.h"],
    implementation_deps = [
        "//src/v/redpanda/admin/services:utils",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:btree",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cluster:fwd",
        "//src/v/cluster:shard_table",
        "//src/v/cluster:topic_table",
        "//src/v/model",
        "//src/v/redpanda/admin/proxy:client",
        "//src/v/serde/protobuf:rpc",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "level_zero",
    srcs = ["level_zero.cc"],
    hdrs = ["level_zero.h"],
    deps = [
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/cluster:fwd",
        "//src/v/cluster:members_table",
        "//src/v/cluster:partition_leaders_table",
        "//src/v/cluster:partition_properties_stm",
        "//src/v/cluster:shard_table",
        "//src/v/cluster:state_machine_registry",
        "//src/v/model",
        "//src/v/redpanda/admin/proxy:client",
        "//src/v/redpanda/admin/proxy:context",
        "//src/v/serde/protobuf:rpc",
        "//src/v/ssx:sformat",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "shadow_link_internal",
    srcs = ["shadow_link_internal.cc"],
    hdrs = ["shadow_link_internal.h"],
    implementation_deps = [
        "//src/v/cluster_link",
        "//src/v/redpanda/admin/services:utils",
        "//src/v/redpanda/admin/services/shadow_link:err",
    ],
    deps = [
        "//proto/redpanda/core/admin/internal/shadow_link_internal/v1:shadow_link_internal_redpanda_proto",
        "//src/v/cluster:fwd",
        "//src/v/cluster_link:fwd",
        "//src/v/cluster_link/model",
        "//src/v/model",
        "//src/v/redpanda/admin/proxy:client",
        "//src/v/serde/protobuf:rpc",
        "@seastar",
    ],
)
