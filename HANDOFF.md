# Slim Redpanda — handoff

Branch `claude/redpanda-slim`, based on `upstream/v26.2.x` (`fd30e45b31`).
Head at time of writing: `c10e7333fc`.

Goal: a Redpanda that is only a distributed Kafka broker on local storage,
with everything else removed.

**Status: not finished.** The tree compiles — the last CI round got 5806 of
5935 build actions done (98%) — but no build has completed end to end yet, so
the binary has never been produced or run. See *Where it stands* below.

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

### Deliberate behaviour changes

- **Wire formats break.** `topic_properties` and `topic_configuration` drop
  tiered fields from their serde and adl encodings; `incremental_topic_updates`
  went from 45 to 24 serde fields; `cluster_health_report` drops
  `bytes_in_cloud_storage`; the controller snapshot drops its
  `cluster_recovery` and `data_migrations` parts. There is no upgrade path from
  stock v26.2.x.
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

---

## Where it stands

CI is `.github/workflows/slim-build.yml`: a `toolchain` job that builds and
caches a bazel toolchain image in GHCR, then a `build` job on
`ubicloud-standard-8` running inside that container, with the bazel repo cache,
disk cache and bazelisk cache all restored from `/mnt`. It does
`build --nobuild //...` then `build --config=release //src/v/redpanda:redpanda`.

Compile errors per round: **415 → 4 → 2 → 2 → 1**. The trend is the point —
each round is a smaller, more local failure.

The last round (`32651449395`) failed on a single translation unit,
`src/v/security/audit/client.cc`, with `Exit 1` and **no compiler diagnostic**
despite `--verbose_failures`. That file is byte-identical to upstream and
references nothing that was removed, so the reading is that clang was
OOM-killed: it is a heavy TU (`cluster/controller.h` plus the kafka client)
compiled 8-wide on a 32 GB runner. A re-run with the disk cache warm was in
flight when this document was written.

**If it fails again at the same TU**, cap concurrency in the build step rather
than changing code:

```yaml
bazel ... build --config=release --jobs=6 ... //src/v/redpanda:redpanda
```

**If it succeeds**, the binary has still never been executed. Nothing here has
been run — not a unit test, not a broker. That is the whole of the remaining
risk and it is not small.

---

## What is left

1. **Get one green build**, then actually start the broker: create a topic,
   produce, consume, restart, confirm the log survives.
2. **Tests do not build.** Analysis covers them, but they were never compiled.
   `bazel build //src/v/...` will surface a fresh crop of errors in test files,
   which have had far less attention than production code.
3. **Vestigial config remains.** `cloud_storage_*` cluster properties are gone,
   but `space_management_enable`, `retention_local_trim_*` and
   `disk_reservation_percent` survive with nothing reading them — the space
   manager (`resource_mgmt/storage.cc`) was deleted. Either restore a local
   space manager or remove the knobs.
4. **`ducktape` tests under `tests/` were not touched at all** and reference
   removed features throughout.
5. `rpk` (`src/go/rpk`) still has commands for the removed subsystems.

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
