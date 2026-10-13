#!/usr/bin/env python3
"""Attribute the compile time in a Bazel profile to product features.

Reads a --profile=...json.gz written by a non-unity build (unity chunks
mix several features into one action, so they are only attributed to a
library) and assigns every "Compiling <file>" action to the first
feature whose glob matches. Prints one table per group and the files
nobody claimed, so the mapping can be kept complete as the tree changes.

    python3 tools/build-timing/feature_breakdown.py profile.json.gz
    python3 tools/build-timing/feature_breakdown.py profile.json.gz --files
    python3 tools/build-timing/feature_breakdown.py profile.json.gz \
        --time-trace time_trace.json

With --time-trace (the --json output of summarize_time_trace.py) it also
prints each feature's header tax: the parse time its headers cost
translation units of other features. That time is counted at first
inclusion and is inclusive, so it is an upper bound on what removing the
feature saves in other files.

Times are the summed duration of the compile actions: CPU-seconds on the
runner, not wall time. Removing a feature saves at least its row, plus
whatever its headers cost the translation units that stop including them.
"""

import argparse
import collections
import fnmatch
import gzip
import json
import re

# (group, feature, globs). Globs match the path in the action name: src/v
# files relative to the repository root, third-party files relative to
# their own repository, as Bazel prints them. First match wins, so put
# narrow globs before broad ones.
FEATURES = [
    # --- cluster -----------------------------------------------------------
    (
        "cluster",
        "Transactions + idempotent producers (rm_stm, tm_stm, tx gateway)",
        [
            "src/v/cluster/rm_*",
            "src/v/cluster/tm_*",
            "src/v/cluster/tx_*",
            "src/v/cluster/producer_state*",
            "src/v/cluster/id_allocator*",
            "src/v/cluster/migrations/*",
        ],
    ),
    (
        "cluster",
        "Produce-time dedup (dedup_stm, window filter)",
        ["src/v/cluster/dedup_*"],
    ),
    (
        "cluster",
        "Partition + leader balancers, allocator",
        [
            "src/v/cluster/partition_balancer_*",
            "src/v/cluster/scheduling/*",
            "src/v/cluster/shard_balancer*",
        ],
    ),
    (
        "cluster",
        "Partition runtime (partition, manager, local STMs, shard placement)",
        [
            "src/v/cluster/partition.cc",
            "src/v/cluster/partition_manager*",
            "src/v/cluster/partition_probe*",
            "src/v/cluster/partition_kafka_offsets*",
            "src/v/cluster/partition_properties_stm*",
            "src/v/cluster/log_eviction_stm*",
            "src/v/cluster/shard_placement_table*",
            "src/v/cluster/utils/*",
        ],
    ),
    (
        "cluster",
        "Health monitor, node status, metadata dissemination",
        [
            "src/v/cluster/health_*",
            "src/v/cluster/node_status*",
            "src/v/cluster/node/*",
            "src/v/cluster/metadata_dissemination*",
            "src/v/cluster/node_isolation_watcher*",
        ],
    ),
    (
        "cluster",
        "Self-test (disk/network benchmarks via admin API)",
        ["src/v/cluster/self_test*"],
    ),
    (
        "cluster",
        "Client quotas store/frontend",
        ["src/v/cluster/client_quota_*"],
    ),
    (
        "cluster",
        "Users/ACLs/roles store (security_frontend/manager)",
        ["src/v/cluster/security_*", "src/v/cluster/ephemeral_credential*"],
    ),
    (
        "cluster",
        "Maintenance mode / drain",
        ["src/v/cluster/drain_*"],
    ),
    (
        "cluster",
        "Phone-home metrics + crash reporter",
        ["src/v/cluster/metrics_reporter*", "src/v/cluster/crash_reporter*"],
    ),
    (
        "cluster",
        "Topic metrics watcher",
        ["src/v/cluster/topic_metrics_watcher*"],
    ),
    (
        "cluster",
        "Controller core (controller STM, topic/member tables, config, features)",
        ["src/v/cluster/*"],
    ),
    # --- raft / storage ----------------------------------------------------
    ("raft", "Raft", ["src/v/raft/*"]),
    (
        "storage",
        "Storage: compaction",
        [
            "src/v/compaction/*",
            "src/v/storage/compact*",
            "src/v/storage/spill_key_index*",
            "src/v/storage/segment_deduplication*",
        ],
    ),
    ("storage", "Storage: log, segments, kvstore", ["src/v/storage/*"]),
    # --- kafka -------------------------------------------------------------
    (
        "kafka",
        "Kafka codecs, generated: auth (SASL, SCRAM, ACLs, roles)",
        [
            "src/v/kafka/protocol/schemata/sasl_*",
            "src/v/kafka/protocol/schemata/*_acls_*",
            "src/v/kafka/protocol/schemata/*scram*",
            "src/v/kafka/protocol/schemata/describe_redpanda_roles*",
        ],
    ),
    (
        "kafka",
        "Kafka codecs, generated: transactions",
        [
            "src/v/kafka/protocol/schemata/*txn*",
            "src/v/kafka/protocol/schemata/*transactions*",
            "src/v/kafka/protocol/schemata/init_producer_id*",
            "src/v/kafka/protocol/schemata/describe_producers*",
        ],
    ),
    (
        "kafka",
        "Kafka codecs, generated: consumer groups",
        [
            "src/v/kafka/protocol/schemata/*group*",
            "src/v/kafka/protocol/schemata/heartbeat*",
            "src/v/kafka/protocol/schemata/offset_commit*",
            "src/v/kafka/protocol/schemata/offset_fetch*",
            "src/v/kafka/protocol/schemata/offset_delete*",
            "src/v/kafka/protocol/schemata/find_coordinator*",
        ],
    ),
    (
        "kafka",
        "Kafka codecs, generated: quotas",
        ["src/v/kafka/protocol/schemata/*client_quotas*"],
    ),
    (
        "kafka",
        "Kafka codecs, generated: data, metadata, topic/config admin",
        ["src/v/kafka/protocol/schemata/*"],
    ),
    ("kafka", "Kafka codecs, hand-written", ["src/v/kafka/protocol/*"]),
    (
        "kafka",
        "Kafka handlers: auth (SASL, SCRAM, ACLs, roles)",
        [
            "src/v/kafka/server/handlers/*acls*",
            "src/v/kafka/server/handlers/*scram*",
            "src/v/kafka/server/handlers/describe_redpanda_roles*",
            "src/v/kafka/server/handlers/details/security*",
        ],
    ),
    (
        "kafka",
        "Kafka handlers: transactions",
        [
            "src/v/kafka/server/handlers/*txn*",
            "src/v/kafka/server/handlers/*transactions*",
            "src/v/kafka/server/handlers/describe_producers*",
            "src/v/kafka/server/rm_group_frontend*",
        ],
    ),
    (
        "kafka",
        "Kafka consumer groups (group manager, group STMs)",
        [
            "src/v/kafka/server/group*",
            "src/v/kafka/server/member*",
            "src/v/kafka/server/handlers/find_coordinator*",
        ],
    ),
    (
        "kafka",
        "Kafka client quotas + throughput control",
        [
            "src/v/kafka/server/*quota*",
            "src/v/kafka/server/handlers/client_quotas*",
        ],
    ),
    ("kafka", "Kafka usage accounting", ["src/v/kafka/server/usage*"]),
    (
        "kafka",
        "Kafka produce + fetch",
        [
            "src/v/kafka/server/handlers/produce*",
            "src/v/kafka/server/handlers/fetch*",
            "src/v/kafka/server/fetch_*",
        ],
    ),
    (
        "kafka",
        "Kafka topic + config admin handlers",
        [
            "src/v/kafka/server/handlers/*topics*",
            "src/v/kafka/server/handlers/topics/*",
            "src/v/kafka/server/handlers/create_partitions*",
            "src/v/kafka/server/handlers/*configs*",
            "src/v/kafka/server/handlers/configs/*",
            "src/v/kafka/server/handlers/delete_records*",
            "src/v/kafka/server/handlers/*reassignments*",
            "src/v/kafka/server/handlers/describe_log_dirs*",
        ],
    ),
    (
        "kafka",
        "Kafka metadata, list offsets, leader epoch",
        [
            "src/v/kafka/server/handlers/metadata*",
            "src/v/kafka/server/handlers/api_versions*",
            "src/v/kafka/server/handlers/list_offsets*",
            "src/v/kafka/server/handlers/offset_for_leader_epoch*",
        ],
    ),
    (
        "kafka",
        "Kafka server core (connections, dispatch)",
        ["src/v/kafka/server/*"],
    ),
    (
        "kafka",
        "Kafka partition proxy + kafka_data RPC",
        ["src/v/kafka/data/*", "src/v/kafka/utils/*"],
    ),
    (
        "kafka",
        "Kafka client library (used by audit logging only)",
        ["src/v/kafka/client/*"],
    ),
    # --- security ----------------------------------------------------------
    ("security", "Audit logging", ["src/v/security/audit/*"]),
    (
        "security",
        "Enterprise license checks",
        [
            "src/v/security/license*",
            "src/v/security/config_bsl*",
            "src/v/features/enterprise_feature*",
        ],
    ),
    (
        "security",
        "Authentication: SASL PLAIN/SCRAM, mTLS",
        [
            "src/v/security/scram_*",
            "src/v/security/plain_*",
            "src/v/security/mtls*",
            "src/v/security/request_auth*",
            "src/v/config/broker_authn_endpoint*",
            "src/v/config/rest_authn_endpoint*",
            "src/v/config/sasl_mechanisms*",
        ],
    ),
    ("security", "Authorization: ACLs, RBAC", ["src/v/security/*"]),
    (
        "security",
        "TLS + crypto wrappers (OpenSSL)",
        ["src/v/net/tls*", "src/v/config/tls_config*", "src/v/crypto/*"],
    ),
    # --- admin + application ------------------------------------------------
    (
        "admin",
        "Admin API v2 (ConnectRPC services, protobuf glue, generated protos)",
        [
            "src/v/redpanda/admin/services/*",
            "src/v/redpanda/admin/proxy/*",
            "src/v/redpanda/admin/kafka_connections_service*",
            "src/v/redpanda/admin/aip_*",
            "src/v/serde/protobuf/*",
            "proto/redpanda/*",
        ],
    ),
    (
        "admin",
        "Admin API v1 (REST handlers, generated Swagger routes)",
        ["src/v/redpanda/admin/*"],
    ),
    ("admin", "Debug bundle (runs rpk)", ["src/v/debug_bundle/*"]),
    (
        "app",
        "Application wiring (application*.cc, main)",
        ["src/v/redpanda/*"],
    ),
    # --- leftovers from removed features --------------------------------------
    (
        "leftover",
        "Parquet/Thrift/Avro codecs",
        ["src/v/serde/parquet/*", "src/v/serde/thrift/*", "src/v/serde/avro/*"],
    ),
    (
        "leftover",
        "Cloud instance metadata (AWS/GCP/Azure IMDS)",
        ["src/v/cloud_instance_metadata/*"],
    ),
    ("leftover", "HTTP client", ["src/v/http/*"]),
    # --- shared libraries ---------------------------------------------------
    ("base", "Config (properties, node config)", ["src/v/config/*"]),
    ("base", "Feature table + migrators", ["src/v/features/*", "src/v/migrations/*"]),
    ("base", "Internal RPC + networking", ["src/v/rpc/*", "src/v/net/*"]),
    (
        "base",
        "Model + serde",
        ["src/v/model/*", "src/v/serde/*", "src/v/reflection/*"],
    ),
    ("base", "Compression codecs", ["src/v/compression/*"]),
    (
        "base",
        "Crash tracker, syschecks, resource mgmt, metrics",
        [
            "src/v/crash_tracker/*",
            "src/v/syschecks/*",
            "src/v/resource_mgmt/*",
            "src/v/metrics/*",
        ],
    ),
    ("base", "Other src/v utilities", ["src/v/*"]),
    # --- third-party ---------------------------------------------------------
    (
        "third-party",
        "Seastar (incl. Prometheus protobuf)",
        [
            "src/core/*",
            "src/net/*",
            "src/http/*",
            "src/util/*",
            "src/rpc/*",
            "src/websocket/*",
            "src/json/*",
            "external/+non_module_dependencies+seastar/*",
        ],
    ),
    (
        "third-party",
        "Protobuf runtime",
        ["src/google/*", "external/protobuf+/*", "util/strutil.cc"],
    ),
    ("third-party", "Abseil", ["absl/*"]),
    ("third-party", "Boost", ["libs/*", "src/build_beast.cpp", "src/build_asio.cpp"]),
    (
        "third-party",
        "xz (liblzma, for Boost.Iostreams)",
        ["src/liblzma/*", "src/common/*"],
    ),
    (
        "third-party",
        "zstd",
        ["lib/common/*", "lib/compress/*", "lib/decompress/*", "lib/dictBuilder/*"],
    ),
    ("third-party", "lz4", ["lib/lz4*"]),
    ("third-party", "re2", ["re2/*"]),
    ("third-party", "roaring", ["src/containers/*", "src/roaring*"]),
    ("third-party", "yaml-cpp", ["src/contrib/*", "src/*.cpp"]),
    ("third-party", "ada (URL parser)", ["ada.cpp"]),
    ("third-party", "snappy", ["snappy*"]),
    ("third-party", "Other C libraries (zlib, hwloc, pciaccess, ...)", ["*"]),
]

TOOL_FEATURES = [
    (
        "Seastar's ragel (HTTP parser generator)",
        ["src/libfsm/*", "src/ragel/*", "external/ragel+/*", "src/*"],
    ),
    ("Other host tools", ["*"]),
]


def load(path):
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt") as f:
        data = json.load(f)
    return data["traceEvents"] if isinstance(data, dict) else data


def classify(path, table):
    for entry in table:
        group, feature, globs = entry if len(entry) == 3 else ("tool", *entry)
        if any(fnmatch.fnmatch(path, g) for g in globs):
            return group, feature
    return None, None


def norm_header(path):
    """src/v/x/_virtual_includes/<target>/x/y.h -> src/v/x/y.h"""
    m = re.match(r"src/v/.*?/_virtual_includes/[^/]+/(.*)", path)
    if m:
        return "src/v/" + m.group(1)
    m = re.match(r"bazel-out/[^/]+/bin/(.*)", path)
    if m:
        return norm_header(m.group(1))
    return path


def header_tax(trace_json, top=3):
    with open(trace_json) as f:
        edges = json.load(f)["edges"]

    def feature(path):
        path = norm_header(path)
        if not path.startswith(("src/v/", "proto/")):
            return None
        return classify(path, FEATURES)[1]

    tax = collections.Counter()
    examples = collections.defaultdict(collections.Counter)
    for includer, header, dur, count in edges:
        fh = feature(header)
        if fh is None or includer == "<tu>" or feature(includer) == fh:
            continue
        tax[fh] += dur
        examples[fh][(norm_header(includer), norm_header(header), count)] += dur

    us = 1e6
    print("\n== header tax: parse time of a feature's headers in other features ==")
    for f, v in tax.most_common():
        print(f"{v / us:7.0f}s  {f}")
        for (a, b, n), w in examples[f].most_common(top):
            print(f"            {w / us:5.0f}s {n:4d} TUs  {a} -> {b}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("profile")
    p.add_argument(
        "--files", action="store_true", help="list each file under its feature"
    )
    p.add_argument("--time-trace", help="summarize_time_trace.py --json output")
    args = p.parse_args()

    us = 1e6
    total = 0
    by_feature = collections.Counter()
    n_feature = collections.Counter()
    group_of = {}
    files = collections.defaultdict(list)
    other = collections.Counter()
    for e in load(args.profile):
        if e.get("ph") != "X" or e.get("cat") != "action processing":
            continue
        dur, name = e.get("dur", 0), e.get("name", "")
        total += dur
        m = re.match(r"Compiling (\S+)", name)
        if not m:
            mn = (e.get("args") or {}).get("mnemonic", "?")
            other[mn + (" (tool)" if "[for tool]" in name else "")] += dur
            continue
        path = m.group(1)
        if "[for tool]" in name:
            group, feature = classify(path, TOOL_FEATURES)
        else:
            group, feature = classify(path, FEATURES)
        group_of[feature] = group
        by_feature[feature] += dur
        n_feature[feature] += 1
        files[feature].append((dur, path))

    print(f"summed action time: {total / us:.0f}s\n")
    print("| group | feature | CPU-s | % | TUs |")
    print("|---|---|---:|---:|---:|")
    order = sorted(
        by_feature,
        key=lambda f: (
            -sum(v for k, v in by_feature.items() if group_of[k] == group_of[f]),
            group_of[f],
            -by_feature[f],
        ),
    )
    for f in order:
        print(
            f"| {group_of[f]} | {f} | {by_feature[f] / us:.0f} | {100 * by_feature[f] / total:.1f} | {n_feature[f]} |"
        )
    for k, v in other.most_common():
        print(f"| non-compile | {k} | {v / us:.0f} | {100 * v / total:.1f} | |")

    print("\n| group | CPU-s | % |")
    print("|---|---:|---:|")
    groups = collections.Counter()
    for f, v in by_feature.items():
        groups[group_of[f]] += v
    groups["non-compile"] = sum(other.values())
    for g, v in groups.most_common():
        print(f"| {g} | {v / us:.0f} | {100 * v / total:.1f} |")

    if args.files:
        for f in order:
            print(f"\n## {f}")
            for dur, path in sorted(files[f], reverse=True):
                print(f"{dur / us:8.1f}s  {path}")

    if args.time_trace:
        header_tax(args.time_trace)


if __name__ == "__main__":
    main()
