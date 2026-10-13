"""
This module contains functions for building Redpanda libraries and executables.
Prefer using the methods in this module (e.g. redpanda_cc_library) over native
Bazel functions (e.g. cc_library) because it provides a centralized place for
making behavior changes across the entire build.
"""

load("@rules_cc//cc:cc_binary.bzl", "cc_binary")
load("@rules_cc//cc:cc_library.bzl", "cc_library")
load(":internal.bzl", "antithesis_deps", "redpanda_copts", "unity_source")

# Sources per unity translation unit. Small enough that the big libraries
# (cluster, kafka/server, storage, raft) still compile in parallel, large
# enough to amortize the headers every file in a library shares.
_UNITY_CHUNK_SIZE = 6

def _unity_srcs(name, srcs, unity, unity_exclude):
    """Returns (srcs, textual_hdrs) for a library, honoring --//bazel:unity_build.

    The library's own .cc files become textual headers so that each unity
    file may include them (and they may include their neighbours' private
    headers) without tripping layering_check.
    """
    cc_srcs = [
        s
        for s in srcs
        if s.endswith(".cc") and ":" not in s and s not in unity_exclude
    ]
    if not unity or len(cc_srcs) < 2:
        return srcs, []
    other_srcs = [s for s in srcs if s not in cc_srcs]
    chunks = [
        cc_srcs[i:i + _UNITY_CHUNK_SIZE]
        for i in range(0, len(cc_srcs), _UNITY_CHUNK_SIZE)
    ]
    unity_files = []
    for i, chunk in enumerate(chunks):
        unity_name = "{}_unity_{}".format(name, i)
        unity_source(
            name = unity_name,
            srcs = chunk,
            out = unity_name + ".cc",
        )
        unity_files.append(":" + unity_name)
    return (
        select({
            "//bazel:unity_build_enabled": other_srcs + unity_files,
            "//conditions:default": srcs,
        }),
        select({
            "//bazel:unity_build_enabled": cc_srcs,
            "//conditions:default": [],
        }),
    )

# buildifier: disable=function-docstring-args
def redpanda_cc_library(
        name,
        srcs = [],
        hdrs = [],
        defines = [],
        local_defines = [],
        implementation_deps = [],
        include_prefix = None,
        visibility = None,
        copts = [],
        deps = [],
        tags = [],
        unity = True,
        unity_exclude = []):
    """
    Define a Redpanda C++ library.

    Set `unity = False` for a library whose sources cannot share a
    translation unit, or list the offending files in `unity_exclude` to
    compile them on their own (e.g. a file with a namespace-scope
    using-directive, which would leak into the files after it).
    """
    if include_prefix == None:
        include_prefix = native.package_name().removeprefix("src/v/")
    srcs, textual_hdrs = _unity_srcs(name, srcs, unity, unity_exclude)
    cc_library(
        name = name,
        srcs = srcs,
        hdrs = hdrs,
        textual_hdrs = textual_hdrs,
        defines = defines,
        local_defines = local_defines,
        visibility = visibility,
        include_prefix = include_prefix,
        implementation_deps = implementation_deps,
        deps = deps,
        copts = redpanda_copts() + copts,
        tags = tags,
        features = [
            "layering_check",
        ],
    )

# buildifier: disable=function-docstring-args
def redpanda_cc_binary(
        name,
        srcs = [],
        defines = [],
        local_defines = [],
        visibility = None,
        testonly = False,
        copts = [],
        linkopts = [],
        deps = []):
    """
    Define a Redpanda C++ binary.
    """
    cc_binary(
        name = name,
        srcs = srcs,
        defines = defines,
        local_defines = local_defines,
        visibility = visibility,
        deps = deps + antithesis_deps(),
        testonly = testonly,
        copts = redpanda_copts() + copts,
        linkopts = linkopts,
        features = [
            "layering_check",
        ],
    )
