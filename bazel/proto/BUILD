load("@protobuf//bazel/toolchains:proto_lang_toolchain.bzl", "proto_lang_toolchain")

# C++ code generation with the C++ generator built into protoc.
#
# protobuf's own C++ toolchain always runs the protoc-gen-cpp plugin, which
# means compiling the protobuf compiler libraries (and their protobuf and
# abseil dependencies) for the exec configuration, even when protoc itself
# is the prebuilt release binary (see prefer_prebuilt_protoc in .bazelrc).
# The release protoc has the same generator built in, so use it directly.
# Toolchains registered by the root module take precedence over protobuf's.
proto_lang_toolchain(
    name = "cc",
    blacklisted_protos = [
        "@protobuf//:compiler_plugin_proto",
        "@protobuf//:cpp_features_proto",
        "@protobuf//:descriptor_proto",
    ],
    command_line = "--cpp_out=$(OUT)",
    # The runtime without the well-known types: those are cc_proto_library
    # targets themselves, so depending on them here would be a cycle. This
    # is what protobuf's own toolchain uses.
    runtime = "@protobuf//:protobuf_nowkt",
    toolchain_type = "@protobuf//bazel/private:cc_toolchain_type",
)
