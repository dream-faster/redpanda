# Slim Redpanda — handoff

Built on branch `claude/redpanda-slim` from `upstream/v26.2.x` (`fd30e45b31`)
and squash-merged into this fork's `v26.2.x` as `2d4e264da7` (#7), followed by
`926f87a647` (#10, Helm chart compatibility). Commit hashes in the tables below
are from the unsquashed branch.

Goal: a Redpanda that is only a distributed Kafka broker on local storage,
with everything else removed.

**Status: it builds, boots and serves.** CI on `1aed61e873` is fully green (27
checks). That includes a smoke test (`tools/slim-checks/smoke.sh`) that starts
the broker, creates plain/compacted/retention topics, produces, consumes with a
consumer group, restarts on the same data directory and checks the log, topic
configs and group survived. A wire-layout test
(`//src/v/cluster/tests:legacy_wire_test`) pins `topic_properties` to the stock
v26.2.x byte layout.

**Not yet proven:** an in-place or rolling upgrade from a real v26.2.x node.
`tools/slim-checks/upgrade_test.sh` does that (three stock nodes replaced one by
one with the slim image on the same volumes); it is wired as the `upgrade` job
of `.github/workflows/docker-build.yml`. Both workflows are manual
(`workflow_dispatch`); run docker-build once before rolling out.

---

## What was removed

Twenty commits, `2244 files changed, 1229 insertions(+), 511960 deletions(-)`.
Whole trees deleted: `cloud_io`, `cloud_roles`, `cloud_storage`,
`cloud_storage_clients`, `cloud_topics`, `cluster_link`, `datalake`, `iceberg`,
`lsm`, `pandaproxy`, `schema`, `transform`, `wasm`, plus the data-migration and
cluster-recovery files under `cluster/`.

| Subsystem | Commits |
|---|---|
| Iceberg and datalake | `e7c4948`, `a2fbb4f` |
| Cloud topics (L0/L1, metastore) | `d9e35f3` |
| Data transforms (wasm, plugin registry) | `cd504ed` |
| Cluster linking and shadow links | `6540818` |
| Schema registry and HTTP proxy | `d5dfdbf` |
| Tiered storage, cluster recovery, data migrations | `f5bc745` |
| GSSAPI/Kerberos and OIDC authentication | `27a742d` |
| Ducktape tests and harness (see [Ducktape tests](#ducktape-tests)) | `6a81637`, `3b35d1c`, `ffa33a8`, `976bc99`, `30d304d`, `8fa43e1`, `ff5a497`, then the follow-up branch `claude/practical-darwin-ffahhu` |

The last of these is the largest. It removes the archival upload path
(`ntp_archiver`, `archiver_manager`, `upload_housekeeping`, `purger`,
`archival_metadata_stm`), the cloud read path (`remote_partition`,
`async_manifest_view`, read replicas), recovery from a bucket (partition,
topic and cluster recovery, the cloud metadata uploader, consumer-offsets
snapshotting, the inventory scrubber), and the whole data-migration subsystem —
mount/unmount and topic-manifest download are all object-storage mediated, so
nothing of it survives the loss of cloud storage.

`cluster::partition` shrank from 471 to 286 lines of header and 1870 to 677
lines of implementation; its constructor is now `(consensus_ptr,
sharded<feature_table>&)`.

### Authentication: GSSAPI and OIDC

`27a742d` removes both, leaving SCRAM, PLAIN and mTLS for Kafka and HTTP Basic
for the admin API:

- **Sources and build.** `security/gssapi_*`, `krb5*`, `jwt.*`, `oidc_*`, the
  vendored `thirdparty/krb5` headers and `bazel/thirdparty/krb5.BUILD`.
- **Config.** `sasl_kerberos_config`, `sasl_kerberos_keytab`,
  `sasl_kerberos_principal`, `sasl_kerberos_principal_mapping`, every `oidc_*`
  property and `nested_group_behavior` are gone. `sasl_mechanisms` (and the
  per-listener overrides) now accept only `SCRAM` and `PLAIN`;
  `http_authentication` accepts only `BASIC`. With no restricted value left,
  neither is enterprise-gated any more, and `gssapi`/`oidc` are no longer
  reported as license-required features.
- **Kafka.** The `GSSAPI` and `OAUTHBEARER` SASL mechanisms are not offered; a
  handshake asking for either gets `unsupported_sasl_mechanism`. The internal
  Kafka client lost OAUTHBEARER too.
- **Admin API.** The v1 OIDC routes and the HTTP bearer-token path in
  `request_auth` are gone. The admin v2 `SecurityService` keeps its three OIDC
  RPCs (`ResolveOidcIdentity`, `RefreshOidcKeys`, `RevokeOidcSessions`) so the
  proto and the generated ducktape bindings don't churn; they answer
  `unimplemented`.

### Deliberate behaviour changes

- **Wire and disk formats stay compatible with stock v26.2.x** for anything that
  does not use a removed feature. serde envelopes are positional, so removed
  mid-struct fields keep their slot as unused `legacy_*` members
  (`src/v/cluster/legacy_wire.h`): `topic_properties`,
  `incremental_topic_updates`, `bootstrap_cluster_cmd_data`, `partition_state`,
  `partition_status` and the `cluster_health_report` tail. Removed controller
  snapshot parts (plugins, cluster recovery, data migrations, cluster links, the
  iceberg/cloud-topic tombstone maps) are written empty and skipped on read.
  Trailing-only removals (`usage`, `start_test_request`) need nothing.
  Anything a stock peer wrote for a removed feature is parsed and dropped, so a
  tiered or Iceberg topic would silently become a plain local one: do not roll
  this onto a cluster that uses them. Legacy ADL encodings (pre-v23 controller
  logs) were not restored.
- `retention.local.target.*` are accepted but inert.
  `maybe_apply_local_storage_overrides` returns early: those knobs bounded the
  on-disk slice of a *tiered* partition, and with no cloud tier the whole log is
  local, so ordinary `retention.bytes`/`retention.ms` govern.
- Lifecycle markers are never written. `topic_lifecycle_transition_mode::pending_gc`
  falls straight through to local deletion; the marker only ever existed to
  drive remote erase.
- `describe_log_dirs` no longer reports a `remote://` log dir.
- Learner initial offset is always `nullopt` — there is no cloud-resident
  prefix for a learner to skip.
- **Clusters using GSSAPI or OIDC are not supported.** A stored
  `sasl_mechanisms` or `http_authentication` value naming a removed mechanism
  is reported as an invalid property on load (`config_manager` still applies
  it, as it does for any value written by an older version), and clients using
  that mechanism can no longer authenticate. Move those clients to SCRAM or
  mTLS before rolling this onto such a cluster.

---

## Where it stands

CI is `.github/workflows/slim-build.yml`: a `toolchain` job that builds and
caches a bazel toolchain image in GHCR, then a `build` job on a
GitHub-hosted `ubuntu-24.04` runner inside that container, with the bazel
repo cache, disk cache and bazelisk cache all restored from `/mnt`. It does
`build --nobuild //...` then `build --config=release //src/v/redpanda:redpanda`.

It took eleven rounds to go green. Almost every failure was the same shape:
**transitive-include collapse**. A file used `result<>`, `ss::async`,
`kafka::group_initializer` or `rpc::connection_cache` and reached the header
through `pandaproxy/schema_registry/types.h` or
`kafka/server/data_migration_group_proxy_impl.h`. Delete those and the file
stops compiling for reasons that have nothing to do with the removal.
`layering_check` is on, so the providing target is already a direct dep and
adding the `#include` is enough.

Two things made this take longer than it should have:

- `gh run view --log-failed` prints only the **last** failing action, so a
  round with twenty errors looked like one broken file. Use
  `tools/slim-checks/ci-errors.sh <run-id>`, which reads the raw log archive.
- Several rounds were spent on breakage I had introduced myself while fixing
  the previous round — a bazel label deduped by line number out of the wrong
  rule, a private field left without a reader, a declaration deleted by name
  leaving its return type behind.

## Ducktape tests

`tests/rptest` went from 669 Python modules (379 `*_test.py`) upstream to 438
(246). CI still does not run ducktape, so none of the survivors have been run
against the slim broker; what has been checked is that the whole tree passes
the repo's pyright gate (`tools/type-checking`) and CI's pinned ruff.

**Done in #7** (`6a81637` … `ff5a497`): deleted the test suites for cloud
topics, datalake/Iceberg, cluster linking, recovery from a bucket, data
migrations, transforms, read replicas, the schema registry and HTTP proxy,
Kerberos, OIDC, tiered storage and the Redpanda Cloud suites, plus the services
that only those used. Surviving tests lost their tiered-storage, Iceberg and
OAuth cases.

**Done on `claude/practical-darwin-ffahhu`:**

- **Keycloak / OAuth.** `KeycloakService`, its image stage and download script,
  `python-keycloak`, the OAUTHBEARER paths in the librdkafka and Kafka CLI
  clients (and the strimzi-kafka-oauth build they needed), and the mitmproxy
  service that only proxied OIDC discovery.
- **Tiered-storage harness.** `SISettings` and the `si_settings` plumbing
  through `RedpandaService`/`RedpandaTest`/`EndToEndTest`, bucket
  create/scrub/cleanup, cloud diagnostics, tiered-storage cache stats,
  `rptest.archival` (S3/ABS clients), `rptest.utils.si_utils`,
  `rptest.context`, the `rp-storage-tool` client, the admin helpers for the
  `/v1/cloud_storage/*` routes, and the remote/storage-mode/Iceberg `TopicSpec`
  fields. MinIO, Azurite and its MySQL are gone from `docker-compose.yml`.
- **Redpanda Cloud harness.** `RedpandaServiceCloud`, the cloud
  broker/cluster/provider clients, kubectl, install packs, the cloud failure
  injector and log search, and the kubectl/helm and teleport image stages. The
  "mixed" local-or-cloud test base is gone; its users run on `RedpandaTest`.
- **Per-test cleanup.** About 35 surviving test files lost tiered-storage,
  cloud-topic, Iceberg, OIDC and Kerberos parameters and cases. Tests that only
  used a removed property as an *example* were switched to a surviving one
  (aliases now use `log_retention_ms`/`delete_retention_ms`, the only alias
  left). Tests deleted outright: `log_storage_target_size_test.py` (the space
  manager that enforced the target is gone), `test_secret_redaction` (no
  secret cluster properties remain), `test_validate_cloud_storage_api_endpoint`,
  `test_cluster_recovery_needs_restart_property`,
  `test_upgrade_with_topic_configs`, `test_recovery_local_data_missing`,
  `test_block_s3_on_all_nodes`, and the boolean topic-property tests (no
  boolean topic properties remain).
- **Deps.** Dropped the Azure Blob and Google Cloud SDK pins and `adlfs`.

**Still left in `tests/`:**

- Schema registry and HTTP proxy leftovers: `PandaproxyConfig`,
  `SchemaRegistryConfig` and `schema_reg()` in `services/redpanda.py`,
  schema-registry flags in the rpk client, and `test_many_subjects` /
  `test_many_schemas` in `scale_tests/many_topics_test.py`, which cannot pass.
- Unused Iceberg/query-engine Python deps in `tests/setup.py` (`pyiceberg`,
  `redpanda-polaris-catalog-python`, `databricks-*`, `duckdb`, `pyhive`,
  `thrift*`, `pyarrow`).
- A third Kafka listener named `kerberoslistener` in
  `services/templates/redpanda.yaml`. It is an ordinary listener now; it was
  kept because `security_report_test.py` and listener counts depend on it.
- The test image still builds librdkafka/kcat with GSSAPI support and installs
  `confluent-kafka` with `--no-binary` for it. That is client-side only and
  harmless.
- The `tools/antithesis` compose template and `tools/pgo_bolt` still reference
  MinIO.

To type-check without docker (the `type-check.sh` wrapper needs a daemon):
create a venv, `pip install 'pyright[nodejs]==1.1.406' -e tests/`, put its
`bin` first on `PATH`, and run
`tools/type-checking/type-check.py --no-venv --tests-root tests check [files]`.

## What is left

1. **Run the upgrade test** (dispatch `docker-build`). It is the only thing
   standing between this and a safe rollout.
2. **Run the ducktape tests.** The survivors are cleaned of tiered storage,
   cloud and OIDC/Kerberos (see [Ducktape tests](#ducktape-tests)) but none
   have been run against the slim broker, and CI does not run them.
3. **Vestigial config remains.** `space_management_enable`,
   `retention_local_trim_*` and `disk_reservation_percent` survive with nothing
   reading them (the space manager was deleted). Kept so stock cluster config
   still loads; remove if you prefer.
4. `rpk`: the registry, shadow, transform, `cluster storage` and
   `topic describe-storage` commands are gone. `topic produce/consume` still
   carry schema-registry flags.
5. MinIO is no longer fetched by the build (the pinned archive 410s
   upstream). `tools/dev_cluster.py` is still there, with `--use-minio` and
   `--use-iceberg-catalog` defaulting off; `tools/pgo_bolt` and the
   `tools/antithesis` compose template still expect MinIO.

---

## How to work on this safely

Read `tools/slim-checks/README.md` first. Analysis being green means very
little; each of those scripts exists because a specific class of breakage got
past analysis and cost a build round-trip.

The failure modes that actually bit, in order of how much time they cost:

- **Transitive-include collapse.** A file used `result<>` or `tristate<>` and
  got the header through `pandaproxy/schema_registry/types.h`. Delete that and
  the file stops compiling for reasons unrelated to the removal. This produced
  the 415-error round. `layering_check` is on, so the providing target is
  already a direct dep; adding the `#include` is enough.
- **Block cuts running past their end.** Removing the transform RPC service
  took the rest of `add_runtime_rpc_services` with it — the raft service,
  `cluster::service`, metadata dissemination, node status, self-test, partition
  balancer, ephemeral credentials. A broker built from that tree would have had
  no controller or raft RPC at all, and nothing flagged it. `collateral.py`
  looks for exactly this.
- **Deleting a declaration by name.** Leaves the return type behind, which
  fuses into the next declaration. `check_merged_decls.py`.
- **Over-broad config removal.** The whole `retention_local_*` /
  `space_management_*` family went with the tiered knobs it sat next to, though
  it governs local disk. Restored in `3bc0559`.
- **BUILD rules outliving their sources.** Analysis resolves labels without
  stat'ing them. `check_files.py`.

A note on method: the surgery was done with regex-driven Python scripts, and
several of the worst regressions came from those scripts over-reaching. Prefer
explicit anchored replacements with a miss-count assertion over "cut from here
to the next brace", and re-run the checks after every pass.
