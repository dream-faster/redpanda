"""
This module contains functions for working with Seastar.
"""

load("//bazel:build.bzl", "redpanda_cc_library")

def _swagger_genrule(name, src, definitions):
    """Runs seastar-json2code on src; returns (hh_out, cc_out)."""
    hh_out = src + ".hh"
    cc_out = src + ".cc"
    src_abs = "$(location " + src + ")"
    hh_out_abs = "$(location " + hh_out + ")"

    native.genrule(
        name = name + "_genrule",
        srcs = [src] + definitions,
        outs = [hh_out, cc_out],
        cmd = "$(location @seastar//:seastar-json2code) --create-cc -f " + src_abs + " -o " + hh_out_abs,
        tools = ["@seastar//:seastar-json2code"],
        toolchains = ["@rules_python//python:current_py_toolchain"],
    )
    return hh_out, cc_out

def seastar_cc_swagger_library(name, src, definitions = [], visibility = None):
    """
    Generate Seastar swagger library.

    Args:
      name: name of the library
      src: the swagger json file
      definitions: additional json definition files
      visibility: library visibility setting
    """
    hh_out, cc_out = _swagger_genrule(name, src, definitions)
    redpanda_cc_library(
        name = name,
        srcs = [
            cc_out,
        ],
        visibility = visibility,
        hdrs = [
            hh_out,
        ],
        deps = [
            "@seastar",
            "@boost//:range",
        ],
    )

def seastar_cc_swagger_libraries(name, apis, visibility = None):
    """
    Generate several Seastar swagger APIs as one library.

    Compiling them together lets unity builds batch the generated sources.
    Each API keeps its own target name as an alias of the combined library.

    Args:
      name: name of the combined library
      apis: dict of API target name to {"src": ..., "definitions": [...]}
      visibility: visibility of the library and the aliases
    """
    srcs = []
    hdrs = []
    for api, spec in apis.items():
        hh_out, cc_out = _swagger_genrule(
            api,
            spec["src"],
            spec.get("definitions", []),
        )
        srcs.append(cc_out)
        hdrs.append(hh_out)
        native.alias(
            name = api,
            actual = ":" + name,
            visibility = visibility,
        )

    redpanda_cc_library(
        name = name,
        srcs = srcs,
        visibility = visibility,
        hdrs = hdrs,
        deps = [
            "@seastar",
            "@boost//:range",
        ],
    )
