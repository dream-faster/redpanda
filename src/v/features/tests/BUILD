load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "feature_table_test",
    timeout = "short",
    srcs = [
        "feature_table_test.cc",
    ],
    deps = [
        "//src/v/cluster:features",
        "//src/v/features",
        "//src/v/model",
        "//src/v/security:license",
        "//src/v/serde",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
