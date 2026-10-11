load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "migrations",
    srcs = [
        "feature_migrator.cc",
        "rbac_migrator.cc",
        "shard_placement_migrator.cc",
        "topic_id_migrator.cc",
    ],
    hdrs = [
        "feature_migrator.h",
        "migrators.h",
        "rbac_migrator.h",
        "shard_placement_migrator.h",
        "topic_id_migrator.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/cluster:client_quota_backend",
        "//src/v/cluster:commands",
        "//src/v/cluster:controller_log_limiter",
        "//src/v/cluster:feature_backend",
        "//src/v/cluster:fwd",
        "//src/v/cluster:members_table",
        "//src/v/cluster:notification",
        "//src/v/cluster:rpc_utils",
        "//src/v/cluster:types",
        "//src/v/features",
        "//src/v/model",
        "//src/v/security",
        "//src/v/ssx:future_util",
        "//src/v/utils:uuid",
        "@seastar",
    ],
)
