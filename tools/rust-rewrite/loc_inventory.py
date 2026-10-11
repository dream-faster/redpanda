#!/usr/bin/env python3
"""Count non-blank C++ lines under src/v per feature, for the Rust rewrite plan.

Each .h/.cc file is assigned to the first feature whose glob matches it.
Files under a tests/ or test/ directory, or named *_test.cc / *_bench.cc,
are counted as test code. Unassigned files are printed so the mapping
stays complete as the tree changes.

    python3 tools/rust-rewrite/loc_inventory.py [--markdown]
"""

import collections
import fnmatch
import os
import sys

ROOT = os.path.join(os.path.dirname(__file__), "..", "..", "src", "v")

# (feature, disposition, globs). Disposition: keep, drop (authn/TLS scope
# cut), dead (no production caller in this tree).
FEATURES = [
    # --- security: dropped from scope -----------------------------------
    ("Audit logging (OCSF events to a topic)", "drop", ["security/audit/*"]),
    ("Authentication: SASL PLAIN/SCRAM, mTLS principal mapping",
     "drop", [
         "security/scram_*", "security/plain_*", "security/sasl_*",
         "security/mtls*", "security/credential_store*",
         "security/ephemeral_credential*", "security/request_auth*",
         "kafka/server/handlers/sasl_*", "kafka/server/sasl_probe*",
         "kafka/server/handlers/*scram*",
         "cluster/ephemeral_credential*",
         "config/broker_authn_endpoint*", "config/rest_authn_endpoint*",
         "config/sasl_mechanisms*",
         "redpanda/admin/services/internal/breakglass*",
     ]),
    ("Enterprise license checks", "drop", [
        "security/license*", "security/config_bsl*",
        "features/enterprise_feature*",
    ]),
    ("Authorization: ACLs, RBAC roles, user/role management",
     "drop", [
         "security/acl*", "security/authorizer*", "security/role*",
         "kafka/server/handlers/*acls*",
         "kafka/server/handlers/describe_redpanda_roles*",
         "kafka/server/handlers/details/security*",
         "kafka/server/handlers/details/roles*",
         "cluster/security_*",
         "migrations/rbac_migrator*",
         "redpanda/admin/security*", "redpanda/admin/services/security*",
         "security/*",
     ]),
    ("TLS (OpenSSL contexts, cert reload, crypto wrappers)",
     "drop", [
         "net/tls*", "config/tls_config*", "config/endpoint_tls_config*",
         "crypto/*",
     ]),
    # --- dead in this tree --------------------------------------------------
    ("Parquet/Thrift/Avro codecs (Iceberg leftovers)", "dead",
     ["serde/parquet/*", "serde/thrift/*", "serde/avro/*"]),
    ("Kafka client library (only caller was audit)", "dead",
     ["kafka/client/*"]),
    # --- kafka --------------------------------------------------------------
    ("Kafka protocol codecs + schema generator", "keep",
     ["kafka/protocol/*"]),
    ("Kafka fetch path (fetch, sessions, memory units, coalescing)", "keep", [
        "kafka/server/handlers/fetch*", "kafka/server/fetch_*",
        "kafka/server/handlers/fetch/*", "kafka/server/read_distribution*",
    ]),
    ("Kafka produce path", "keep", [
        "kafka/server/handlers/produce*",
    ]),
    ("Kafka consumer groups + offsets", "keep", [
        "kafka/server/group*", "kafka/server/member*",
        "kafka/server/coordinator_ntp_mapper*",
        "kafka/server/handlers/*group*", "kafka/server/handlers/heartbeat*",
        "kafka/server/handlers/offset_commit*",
        "kafka/server/handlers/offset_fetch*",
        "kafka/server/handlers/offset_delete*",
        "kafka/server/handlers/find_coordinator*",
    ]),
    ("Kafka transactions + idempotence (broker-side handlers)", "keep", [
        "kafka/server/rm_group_frontend*",
        "kafka/server/handlers/*txn*", "kafka/server/handlers/end_txn*",
        "kafka/server/handlers/init_producer_id*",
        "kafka/server/handlers/*transactions*",
        "kafka/server/handlers/describe_producers*",
    ]),
    ("Kafka topic + config admin APIs", "keep", [
        "kafka/server/handlers/*topics*", "kafka/server/handlers/topics/*",
        "kafka/server/handlers/create_partitions*",
        "kafka/server/handlers/*configs*", "kafka/server/handlers/configs/*",
        "kafka/server/handlers/details/alter_config*",
        "kafka/server/handlers/delete_records*",
        "kafka/server/handlers/*reassignments*",
        "kafka/server/handlers/describe_log_dirs*",
        "kafka/server/handlers/list_offsets*",
        "kafka/server/handlers/offset_for_leader_epoch*",
        "kafka/server/handlers/details/leader_epoch*",
    ]),
    ("Kafka metadata / api_versions / describe_cluster", "keep", [
        "kafka/server/handlers/metadata*",
        "kafka/server/handlers/api_versions*",
        "kafka/server/handlers/describe_cluster*",
    ]),
    ("Kafka client quotas + throughput control", "keep", [
        "kafka/server/*quota*", "kafka/server/atomic_token_bucket*",
        "kafka/server/handlers/*client_quotas*",
        "cluster/client_quota*", "config/throughput_control_group*",
    ]),
    ("Kafka usage accounting", "keep", ["kafka/server/usage*"]),
    ("Kafka server core (listener, connections, dispatch)", "keep", [
        "kafka/server/*", "kafka/server/handlers/*",
    ]),
    ("Kafka partition proxy + kafka_data RPC", "keep", ["kafka/data/*"]),
    ("Kafka misc utils", "keep", ["kafka/utils/*"]),
    # --- raft ---------------------------------------------------------------
    ("Raft: consensus core (election, replication, heartbeats)", "keep", [
        "raft/consensus*", "raft/vote_stm*", "raft/replicate*",
        "raft/heartbeat*", "raft/follower_states*", "raft/append_entries*",
        "raft/buffered_protocol*", "raft/mutex_buffer*", "raft/types*",
        "raft/transfer_leadership*", "raft/voter_priority*",
        "raft/timeout_jitter*", "raft/event_manager*", "raft/fundamental*",
        "raft/offset_monitor*", "raft/replication_monitor*",
    ]),
    ("Raft: recovery + throttling", "keep", [
        "raft/recovery*", "raft/coordinated_recovery*",
    ]),
    ("Raft: membership / joint-consensus configuration", "keep", [
        "raft/group_configuration*", "raft/configuration*",
        "raft/broker_compat*",
    ]),
    ("Raft: state machine framework (persisted_stm, snapshots)", "keep", [
        "raft/state_machine*", "raft/persisted_stm*", "raft/mux_state*",
        "raft/compaction_coordinator*",
    ]),
    ("Raft: group manager, RPC service, probes", "keep", ["raft/*"]),
    # --- storage ------------------------------------------------------------
    ("Storage: compaction (sliding window, key maps, indexes)", "keep", [
        "storage/compact*", "storage/spill_key_index*",
        "storage/segment_deduplication*", "storage/backlog_controller*",
        "compaction/*",
    ]),
    ("Storage: KV store", "keep", ["storage/kvstore*"]),
    ("Storage: offset translator", "keep", [
        "storage/offset_translator*", "storage/translating_reader*",
    ]),
    ("Storage: caches (batch cache, readers cache, chunk cache)", "keep", [
        "storage/batch_cache*", "storage/readers_cache*",
        "storage/chunk_cache*",
    ]),
    ("Storage: fault-injection file wrappers (test support)", "keep", [
        "storage/file_sanitizer*", "storage/failure_probes*",
        "storage/opfuzz/*",
    ]),
    ("Storage: segments, index, appender, readers, log manager", "keep", [
        "storage/*",
    ]),
    # --- cluster ------------------------------------------------------------
    ("Transactions: coordinator (tm_stm, tx gateway) + rm_stm", "keep", [
        "cluster/tx_*", "cluster/tm_stm*", "cluster/rm_*",
        "cluster/producer_state*", "cluster/rm_stm*",
        "cluster/migrations/*",
    ]),
    ("Partition placement: allocator + leader balancer", "keep", [
        "cluster/scheduling/*",
    ]),
    ("Partition auto-balancer (planner/backend)", "keep", [
        "cluster/partition_balancer*",
    ]),
    ("Shard placement + core balancing", "keep", [
        "cluster/shard_*",
    ]),
    ("Health monitor + node status", "keep", [
        "cluster/health_*", "cluster/node_status*", "cluster/node/*",
        "cluster/node_isolation*",
    ]),
    ("Membership, discovery, cluster bootstrap", "keep", [
        "cluster/members_*", "cluster/cluster_discovery*",
        "cluster/bootstrap_*", "cluster/cluster_uuid*",
    ]),
    ("Cluster config replication (config_manager)", "keep", [
        "cluster/config_*",
    ]),
    ("Feature flags + upgrade gating", "keep", [
        "cluster/feature_*", "features/*", "migrations/*",
    ]),
    ("Topic metadata (topic_table, frontend, dispatcher)", "keep", [
        "cluster/topic*", "cluster/topics_frontend*",
    ]),
    ("Controller core (controller stm, backend, snapshot, commands)", "keep",
     ["cluster/controller*", "cluster/commands*", "cluster/raft0*"]),
    ("Metadata dissemination + leaders table + caches", "keep", [
        "cluster/metadata_*", "cluster/partition_leaders*",
        "cluster/leader_router*", "cluster/namespaced_cache*",
    ]),
    ("Partition runtime (partition, manager, eviction/properties stms)",
     "keep", [
         "cluster/partition*", "cluster/log_eviction*",
         "cluster/prefix_truncate*", "cluster/ntp_callbacks*",
         "cluster/state_machine_registry*",
     ]),
    ("ID allocator + distributed KV stm", "keep", [
        "cluster/id_allocator*", "cluster/distributed_kv*",
        "cluster/offsets_snapshot*",
    ]),
    ("Maintenance mode / drain", "keep", ["cluster/drain*"]),
    ("Self-test (disk/network benchmarks)", "keep", [
        "cluster/self_test*",
    ]),
    ("Phone-home metrics reporter + crash reporter", "keep", [
        "cluster/metrics_reporter*", "cluster/crash_reporter*",
    ]),
    ("Cluster shared types, errors, legacy wire shims, RPC service", "keep",
     ["cluster/*"]),
    # --- admin --------------------------------------------------------------
    ("Admin API v2 (ConnectRPC) + proto/JSON codecs", "keep", [
        "redpanda/admin/services/*", "redpanda/admin/proxy/*",
        "redpanda/admin/kafka_connections*", "redpanda/admin/aip_*",
        "serde/protobuf/*", "serde/json/*",
    ]),
    ("Admin API v1 (REST) + debug bundle trigger", "keep",
     ["redpanda/admin/*"]),
    # --- config, rpc, net, model -------------------------------------------
    ("Configuration framework + ~350 properties", "keep", ["config/*"]),
    ("Internal RPC framework (serde over TCP)", "keep", ["rpc/*"]),
    ("Networking: server, connection limits/rates, transports", "keep",
     ["net/*"]),
    ("Data model (record batches, ntp, offsets, ADL)", "keep", ["model/*"]),
    ("serde: positional binary envelope format", "keep", ["serde/*"]),
    ("Compression (gzip, snappy, lz4, zstd)", "keep", ["compression/*"]),
    ("HTTP client", "keep", ["http/*"]),
    # --- observability / ops -----------------------------------------------
    ("Metrics (Prometheus endpoints, host metrics)", "keep", [
        "metrics/*", "cloud_instance_metadata/*",
    ]),
    ("Debug bundle service", "keep", ["debug_bundle/*"]),
    ("Crash tracker (signal-safe crash records)", "keep",
     ["crash_tracker/*"]),
    ("Resource mgmt (memory groups, CPU sched groups, profilers)", "keep",
     ["resource_mgmt/*"]),
    ("Startup system checks", "keep", ["syschecks/*"]),
    ("Application wiring (main, service startup/shutdown)", "keep",
     ["redpanda/*"]),
    # --- foundations --------------------------------------------------------
    ("iobuf / zero-copy byte buffers", "keep", ["bytes/*"]),
    ("Async primitives on Seastar (ssx)", "keep", ["ssx/*"]),
    ("Containers (chunked vectors, radix tree, interval maps)", "keep",
     ["container/*"]),
    ("Utilities (hist, retry chain, caches, encodings, misc)", "keep", [
        "utils/*", "base/*", "strings/*", "random/*", "hashing/*",
        "reflection/*", "json/*", "finjector/*", "version/*", "rp_util/*",
        "thirdparty/*",
    ]),
    ("Shared test fixtures", "keep", ["test_utils/*"]),
]


def is_test(path):
    parts = path.split("/")
    return (any(p in ("tests", "test", "test_utils") for p in parts)
            or path.endswith(("_test.cc", "_bench.cc", "_rpbench.cc")))


def strip_test_dirs(path):
    return "/".join(p for p in path.split("/") if p not in ("tests", "test"))


def main():
    markdown = "--markdown" in sys.argv
    counts = collections.OrderedDict(
        (name, collections.Counter()) for name, _, _ in FEATURES)
    disposition = {name: d for name, d, _ in FEATURES}
    unassigned = []
    for dp, dn, fn in os.walk(ROOT):
        dn[:] = [d for d in dn if not d.startswith(".")]
        for f in fn:
            if not f.endswith((".h", ".cc")):
                continue
            rel = os.path.relpath(os.path.join(dp, f), ROOT)
            with open(os.path.join(dp, f), errors="ignore") as fh:
                n = sum(1 for line in fh if line.strip())
            key = strip_test_dirs(rel)
            for name, _, globs in FEATURES:
                if any(fnmatch.fnmatch(key, g) for g in globs):
                    counts[name]["test" if is_test(rel) else "prod"] += n
                    break
            else:
                unassigned.append(rel)

    totals = collections.Counter()
    if markdown:
        print("| Feature | Scope | Prod LOC | Test LOC |")
        print("|---|---|---:|---:|")
    for name, c in counts.items():
        d = disposition[name]
        totals[d + "_prod"] += c["prod"]
        totals[d + "_test"] += c["test"]
        if markdown:
            print(f"| {name} | {d} | {c['prod']:,} | {c['test']:,} |")
        else:
            print(f"{d:5s} {c['prod']:7d} {c['test']:7d}  {name}")
    print()
    for d in ("keep", "drop", "dead"):
        print(f"{d}: prod={totals[d + '_prod']:,} test={totals[d + '_test']:,}")
    if unassigned:
        print("\nunassigned:", *unassigned, sep="\n  ")


if __name__ == "__main__":
    main()
