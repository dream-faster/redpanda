# Build time: where it goes and what is left to cut

This continues the work that added prebuilt `protoc`, opt-in unity builds
and the first header fixes. It answers four questions:

1. How long does each feature take to compile?
2. Which features could go while keeping a working cluster, and what would
   each save?
3. Would a different compiler or build tool help?
4. Should we fork Seastar (or other large dependencies) and cut them down?

Short answers:

- No single feature is large. The biggest optional ones cost 4–8% each,
  and everything optional added together is about a quarter of the build.
- Most of the cost of each `.cc` file is a floor that every file pays:
  parsing the same Seastar, fmt, libc++ and abseil headers again. Unity
  builds, header fixes and precompiled headers attack that floor; deleting
  features only shrinks the number of files that pay it.
- Wall-clock time has a cheaper lever than any code change: more cores,
  or remote execution, plus shortening a critical path that is mostly
  `rules_foreign_cc` configure scripts.
- We are already on the newest compiler (LLVM 22.1). A new compiler won't
  help; new compiler *features* (PCH, header modules) might.
- Forking Seastar to cut scope saves little. Compiling Seastar itself is
  about 5% of the build, and the parts we don't use are about 1%. What
  Seastar really costs is its headers in our own files, and a fork doesn't
  change that. A few patches in `bazel/thirdparty/seastar.BUILD` get most
  of what a fork would.

## Data

All numbers come from the `build-timing` workflow on the 8-core runner,
building `//src/v/redpanda:redpanda` cold, without a disk cache:

| run | what | config | wall | CPU (summed action time) |
|---|---|---|---:|---:|
| [38419810919][r1] | this branch, per-file | release | 2018s | 15,740s |
| [38432333831][r2] | this branch, `--config=unity` | release | 922s | 7,033s |
| [38432333831][r2] | this branch, `--config=unity` | fastbuild | 780s | 5,763s |
| [38407964297][r3] | `-ftime-trace` of every TU | fastbuild | — | 9,595s in clang |

[r1]: https://github.com/dream-faster/redpanda/actions/runs/38419810919
[r2]: https://github.com/dream-faster/redpanda/actions/runs/38432333831
[r3]: https://github.com/dream-faster/redpanda/actions/runs/38407964297

"CPU" is the sum of action durations, so it adds up to how long the build
would take on one core. Wall time on N cores is roughly
`max(critical path, CPU / N)`. Feature costs below are from the per-file
release run, because unity chunks mix several features in one action.

To reproduce, download a run's artifact and:

```bash
python3 tools/build-timing/feature_breakdown.py profile.json.gz [--files] \
    [--time-trace time_trace.json]
```

The workflow now writes this as `features.txt` in every run.

## 1. Compile time per feature

Release, one TU per file, 15,740 CPU-s in total. The **Cluster** column
says whether a broker cluster needs the feature: **keep**, **optional**
(a product feature we could drop), or **leftover** (no live user here).

### By area

| area | CPU-s | % |
|---|---:|---:|
| Kafka API (codecs, handlers, groups, client) | 3,675 | 23.3 |
| Cluster (controller, partitions, balancers, tx) | 3,305 | 21.0 |
| Third-party libraries | 2,038 | 12.9 |
| Admin API (v1 REST, v2 ConnectRPC, debug bundle) | 1,365 | 8.7 |
| Shared src/v libraries (config, model, rpc, utils) | 1,357 | 8.6 |
| Non-compile (foreign_cc configure/make, Go, genrules) | 976 | 6.2 |
| Storage | 845 | 5.4 |
| Raft | 741 | 4.7 |
| Application wiring (`application*.cc`) | 654 | 4.2 |
| Security (auth, ACLs, audit, TLS, license) | 499 | 3.2 |
| Host tools (Seastar's `ragel`) | 229 | 1.5 |

### Every feature

| feature | CPU-s | % | TUs | Cluster |
|---|---:|---:|---:|---|
| **Cluster** | | | | |
| Controller core: controller STM, topic/member tables, config, feature manager, bootstrap | 1,336 | 8.5 | 37 | keep |
| Transactions + idempotent producers: `rm_stm`, `tm_stm`, tx gateway, id allocator | 512 | 3.3 | 17 | keep idempotence; EOS optional |
| Partition + leader balancers, partition allocator | 377 | 2.4 | 16 | keep |
| Health monitor, node status, metadata dissemination | 303 | 1.9 | 11 | keep |
| Partition runtime: `partition`, manager, local STMs, shard placement | 219 | 1.4 | 8 | keep |
| Self-test (disk/network benchmarks behind the admin API) | 152 | 1.0 | 6 | optional |
| Users/ACLs/roles store (`security_frontend`, `security_manager`) | 138 | 0.9 | 4 | optional (auth) |
| Client quota store | 98 | 0.6 | 4 | optional |
| Phone-home metrics reporter + crash reporter | 68 | 0.4 | 2 | optional |
| Maintenance mode / drain | 44 | 0.3 | 2 | keep (rolling upgrades) |
| Produce-time dedup | 42 | 0.3 | 2 | keep |
| Topic metrics watcher | 18 | 0.1 | 1 | optional |
| **Raft + storage** | | | | |
| Raft | 741 | 4.7 | 30 | keep |
| Storage: log, segments, kvstore | 680 | 4.3 | 35 | keep |
| Storage: compaction | 165 | 1.0 | 9 | keep |
| **Kafka API** | | | | |
| Kafka client library (only user: audit logging) | 522 | 3.3 | 16 | optional |
| Generated codecs: data, metadata, topic/config admin | 419 | 2.7 | 34 | keep |
| Topic + config admin handlers | 397 | 2.5 | 12 | keep |
| Consumer groups (group manager, group STMs) | 315 | 2.0 | 10 | keep |
| Server core (connections, dispatch) | 290 | 1.8 | 8 | keep |
| Generated codecs: consumer groups | 277 | 1.8 | 22 | keep |
| Generated codecs: transactions | 192 | 1.2 | 16 | EOS optional |
| Partition proxy + `kafka_data` RPC | 186 | 1.2 | 10 | keep |
| Handlers: SASL, SCRAM, ACLs, roles | 182 | 1.2 | 6 | optional (auth) |
| Produce + fetch handlers | 175 | 1.1 | 7 | keep |
| Generated codecs: SASL, SCRAM, ACLs, roles | 163 | 1.0 | 16 | optional (auth) |
| Handlers: transactions | 148 | 0.9 | 4 | EOS optional |
| Metadata, list offsets, leader epoch | 147 | 0.9 | 4 | keep |
| Client quotas + throughput control | 95 | 0.6 | 4 | optional |
| Hand-written codecs | 63 | 0.4 | 6 | keep |
| Usage accounting | 54 | 0.3 | 2 | optional |
| Generated codecs: client quotas | 49 | 0.3 | 4 | optional |
| **Admin API and application** | | | | |
| Admin API v1: REST handlers + generated Swagger routes | 705 | 4.5 | 26 | keep (Helm chart, operator, rpk) |
| Application wiring: `application*.cc`, `main.cc` | 654 | 4.2 | 11 | keep |
| Admin API v2: ConnectRPC services, protobuf glue, generated `.pb.cc` | 601 | 3.8 | 26 | optional |
| Debug bundle | 58 | 0.4 | 5 | optional |
| **Security** | | | | |
| Audit logging | 170 | 1.1 | 7 | optional |
| Authentication: SASL PLAIN/SCRAM, mTLS | 126 | 0.8 | 9 | optional |
| TLS + OpenSSL wrappers | 82 | 0.5 | 10 | keep (see below) |
| Authorization: ACLs, RBAC | 67 | 0.4 | 4 | optional |
| Enterprise license checks | 53 | 0.3 | 3 | optional |
| **Shared src/v libraries** | | | | |
| Utilities: `utils`, `bytes`, `ssx`, `json`, `random`, `finjector`, ... | 323 | 2.1 | 33 | keep |
| Crash tracker, syschecks, resource mgmt, metrics | 264 | 1.7 | 23 | keep |
| Internal RPC + networking | 201 | 1.3 | 16 | keep |
| Config | 158 | 1.0 | 10 | keep |
| Model + serde | 158 | 1.0 | 14 | keep |
| Feature table + migrators | 145 | 0.9 | 7 | keep |
| Compression codecs | 108 | 0.7 | 9 | keep |
| HTTP client (only user: phone-home) | 30 | 0.2 | 4 | leftover |
| Cloud instance metadata (AWS/GCP/Azure IMDS) | 27 | 0.2 | 3 | leftover |
| **Third-party** | | | | |
| Seastar | 760 | 4.8 | 96 | keep |
| Protobuf runtime (Admin API v2, Seastar's Prometheus protobuf) | 432 | 2.7 | 77 | optional |
| Abseil (about half only for protobuf) | 302 | 1.9 | 133 | keep part |
| Boost compiled libraries | 297 | 1.9 | 113 | keep part |
| re2 | 63 | 0.4 | 19 | keep |
| yaml-cpp | 62 | 0.4 | 32 | keep |
| zlib, hwloc, pciaccess, xz, zstd, roaring, ada, lz4, snappy | 122 | 0.8 | 160+ | keep |
| **Tools and non-compile** | | | | |
| `rules_foreign_cc` configure/make: OpenSSL, hwloc | 356 | 2.3 | | keep |
| Seastar's `ragel` + `colm` (HTTP parser generator) | 229 | 1.5 | 102 | replaceable |
| Bootstrapping `pkg-config` and GNU make from source | 200 | 1.3 | | replaceable |
| `rules_foreign_cc` CMake: c-ares, base64 | 164 | 1.0 | | keep |
| Genrules (Kafka/RPC/Swagger code generators) | 154 | 1.0 | | keep |
| Go toolchain + stdlib (`pbgen` protoc plugin for Admin API v2) | 80 | 0.5 | | optional |

Already gone from the binary's build: Parquet, Thrift, Avro, `utils/xml`
and the test utilities. They are still in the tree, but nothing in
`//src/v/redpanda` depends on them.

### What a feature costs other files

Deleting a feature also stops other files from parsing its headers.
`feature_breakdown.py --time-trace` measures that. For the optional
features it is small next to their own files:

| feature | header time in other features (upper bound) | main edge |
|---|---:|---|
| ACLs/RBAC + users/roles store | ~345s | `cluster/commands.h` → `security_types.h` → `security/acl.h` (128 TUs) |
| TLS | ~129s | `config/endpoint_tls_config.h` → `tls_config.h` → `seastar/net/tls.hh` (283 TUs) |
| Audit logging | ~114s | `security/acl.h` → `audit/schemas/types.h`, `kafka/server/server.h` → `audit_log_manager.h` |
| Phone-home + HTTP client | ~86s | `cluster/controller.h` → `crash_reporter.h`, `metrics_reporter.h` → `http/client.h` |
| Admin API v2 | ~25s | `kafka/server/connection_context.h` → `kafka_connections.proto.h` |

These come from the fastbuild trace and count each header inclusively at
its first inclusion, so they overstate the saving: some of that time would
move to whichever header pulls in the same libc++/Seastar headers next.

## 2. Feature removal: what it would save

Each bundle below is everything that goes together, from the per-file
release build. Columns:

- **CPU-s / %**: per-file release build (out of 15,740s).
- **unity**: the same files in the unity build (out of 7,033s). It is
  exact for bundles that are whole Bazel libraries. For the others it is
  marked "≈", because unity chunks mix files, so the unity profile
  undercounts them.

| | bundle | CPU-s | % | unity | effort | risk |
|---|---|---:|---:|---:|---|---|
| A | Audit logging + the Kafka client library (its only user) | 692 | 4.4 | 322 | low | low; keep the `audit_*` cluster properties as accepted no-ops so the Helm chart keeps working |
| B | Admin API v2 (ConnectRPC) + protobuf runtime + protobuf-only abseil + `pbgen`/Go + Seastar's protobuf metrics format | 1,290 | 8.2 | 850 | medium | medium; anything calling v2 endpoints (newer `rpk` commands, e.g. Kafka connection listing) breaks; needs a Seastar patch (see 4) |
| C | Authentication + authorization: SASL/SCRAM/mTLS, ACLs, RBAC, their Kafka APIs and codecs, users/roles store, admin endpoints | 972 | 6.2 | ≈272 | high | high; touches the connection path and controller commands, and old ACL/user commands in the controller log must still be skipped on replay |
| D | Transactions (EOS) while keeping idempotent producers: tx Kafka APIs, `tm_stm`, tx gateway, `rm_partition_frontend`, tx parts of groups | 704 | 4.5 | ≈149 | high | high; `rm_stm` serves both idempotence and transactions and is tangled with group commit |
| E | Self-test | 152 | 1.0 | 50 | low | low |
| F | Debug bundle | 113 | 0.7 | 46 | low | low; running `rpk debug bundle` on the node still works, only the bundle requested remotely through the admin API goes |
| G | Usage accounting | 99 | 0.6 | ≈4 | low | low |
| H | Phone-home metrics + crash reporter + HTTP client + cloud IMDS + Boost.Beast/Asio + ada | 155 | 1.0 | 36 | low | low |
| I | Enterprise license checks | 53 | 0.3 | 25 | low | low; sanctions are already disabled |
| J | Client quotas | 241 | 1.5 | ≈69 | medium | only if no tenant needs quotas |
| K | Fault injection (`hbadger` routes) | 30 | 0.2 | 11 | low | low; the ducktape tests use it |

- **Easy, low-risk set (A, B, E–I, K):** about 2,600 CPU-s, 16% of the
  per-file build. In the unity build it is about 1,350 of 7,033 CPU-s
  (19%), because the bundles that are whole libraries (A, B) don't shrink
  under unity.
- **Adding auth and transactions (C, D):** about 4,300 CPU-s, 27%. Those
  two are the expensive and risky ones.
- **What stays:** cluster, Raft, storage, the Kafka data path and admin
  API v1 are about 70% of the build no matter what. Everything below is
  about making that part cheaper.

**TLS is not worth removing for build time.** Its own files cost 82s and
its headers about 130s. OpenSSL's configure/make (138s, and on the
critical path) stays anyway: SCRAM, license signatures and Seastar's
crypto provider all use it. Fixing the header chain (3c below) gets the
130s without losing TLS.

## 3. Levers that keep every feature

Ordered by return on effort. Percentages are of the unity release build
(7,033 CPU-s, 922s wall on 8 cores) unless noted.

### 3a. Hardware and build infrastructure (no source changes)

**The build is limited by throughput, not by its critical path.** The
unity build's critical path is 305s, and 8 cores take 922s. On N cores
the wall time is roughly `max(305s, 7,033s / N)`:

| runner | expected wall | note |
|---|---:|---|
| 8 cores (today) | ~920s | measured |
| 16 cores | ~470s | |
| 30 cores | ~330s | bound by the critical path |
| remote execution (100+ cores) | ~305s → ~150s | ~150s once 3b removes the foreign_cc chain |

Runners are billed per core-minute and the CPU-seconds don't change, so a
bigger runner costs about the same per build and finishes 2–3× sooner.
Remote execution (BuildBuddy, EngFlow, NativeLink, or self-hosted
Buildbarn) also gives developers a shared cache: after the first CI build
of a commit, a laptop rebuilds only what it changed.

### 3b. Shorten the critical path

268s of the 305s critical path are `rules_foreign_cc` work, not C++:

| action | time | fix |
|---|---:|---|
| OpenSSL `Configure` + make | 138s | Build OpenSSL with native Bazel rules (the BCR `openssl` module, 3.5.x), or ship a prebuilt one in the toolchain image |
| Bootstrap `pkg-config` from source | 85s | Register `rules_foreign_cc`'s preinstalled `make`/`pkg-config` toolchains ahead of the built ones. The toolchain image already has both |
| Bootstrap GNU make from source | 44s | same |
| hwloc `configure` (off the critical path) | 101s | native `cc_library` BUILD file |
| c-ares CMake (off the critical path) | 56s | native `cc_library` BUILD file (c-ares is in the BCR) |

Together that is about 420 CPU-s (6%) and moves the critical path from
305s to about 150s (code generators, the slowest cluster chunk, then the link). That only
pays off once 3a supplies the cores. On 8 cores it saves the 6%.

### 3c. Build-system changes

| lever | saving | effort |
|---|---:|---|
| **Unity for the generated Kafka codecs.** `generate_kafka_messages` makes one library per message with one source each, so `--config=unity` skips all 92 files. They cost **668 CPU-s, 9.5% of the unity build**. One library for all messages, chunked like the rest, should bring that to ~200s | **~6–7%** | low |
| Same for the 15 Swagger route files (`admin/api-doc`, 66s) and the 8 `.proto.cc` (169s) | ~2% | low |
| Unity for Seastar's own sources. `seastar.BUILD` is our file and 96 TUs at a ~5s floor each get no batching today | ~3% | low–medium |
| Check in the `ragel` output for Seastar's 3 HTTP parsers instead of building `ragel` and `colm` (179s unity, 229s per-file). The `.rl` files change maybe once a year | 2.5% | low |
| Debug info: release builds pass `-g`. `-gline-tables-only` (still enough for symbolized backtraces) or `-gsplit-dwarf` for CI and dev builds, keeping full `-g` for shipped images. Release vs fastbuild differs by 1,270 CPU-s (18%), and part of that is debug info | est. 5–10% | low |
| Turn unity on for CI. It is off today only to keep include hygiene enforced; a nightly non-unity build would still catch missing includes | 45% of CI build CPU | low |
| `-D_LIBCPP_REMOVE_TRANSITIVE_INCLUDES`: libc++ stops dragging in headers it doesn't need. It will expose missing includes that have to be fixed | unmeasured, est. 3–8% | medium |
| Precompiled headers for the common set (Seastar core, fmt, libc++, abseil containers): see 3e | est. 10–20% | medium–high |

### 3d. Header fixes

From the `-ftime-trace` run (fastbuild, one TU per file; under unity
each costs roughly a third to half as much). Each row is an include that
makes hundreds of TUs parse something they don't use:

| include edge | TUs | parse time | fix |
|---|---:|---:|---|
| `base/format_to.h` → `fmt/ostream.h`, `fmt/std.h`, `fmt/format.h` | 539 | 719s | Drop `fmt/ostream.h`: rewrite the `fmt::print(ostream&)` users in `named_type.h`, `to_string.h`, `acl.h`. `fmt/ostream.h` also pulls `<fstream>` and `fmt/chrono.h` |
| `storage/ntp_config.h` → `config/configuration.h` | 189 | 410s | Move the default-value lookups out of line. Every topic-properties user parses all cluster config |
| `config/property.h` → `rjson_serialization.h` → `endpoint_tls_config.h` → `tls_config.h` → `seastar/net/tls.hh` | 289 | 347s | Split TLS (de)serialization out of `rjson_serialization.h` |
| `base/oncore.h`, `model/record.h` → `seastar/core/smp.hh` | 288 | 276s | They only need `this_shard_id()`: include `seastar/core/shard_id.hh` |
| `kafka/protocol/types.h` → `absl/container/btree_map.h` | 151 | 244s | Forward-declare or move the btree use out of the codec header |
| `kafka/server/request_context.h` → `group_router.h` → `coordinator_ntp_mapper.h` → `cluster/metadata_cache.h` | 30 | 233s | Every handler parses the group router; forward-declare it |
| `cluster/state_machine_registry.h` → `raft/consensus.h` | 45 | 178s | Forward-declare `consensus` |
| `config/configuration.h` → `seastar/net/ip.hh` | 262 | 90s + 18s instantiating ARP | `ip.hh` is Seastar's *native* stack (ARP, IPv4 routing), which we don't use; `inet_address.hh` is enough |
| `cluster/types.h` (846s over 162 TUs) and `cluster/commands.h` (615s over 121 TUs) | | | Split into per-command headers; most files need one or two request types |

The first six rows add up to about 2,200s, a quarter of the src/v
frontend time in that build. Under unity it would be closer to 8–12%.
Each fix is small and can be done on its own.

Template instantiation shows the same pattern:
`std::basic_format_string<char>` instantiated in 934 TUs (98s) is libc++
22's `<ostream>` including `<print>` in C++23 mode. That disappears from
any TU that stops including `<ostream>` (see the `fmt/ostream.h` row).

### 3e. Compilers and build tools

| option | verdict |
|---|---|
| Newer compiler | We are on LLVM 22.1.0 (redpanda-data's build), the newest release. GCC is generally slower at template-heavy code and would mean giving up libc++. Nothing to gain. |
| Linker (mold, wild) | Linking is 5s of 922s. No gain. |
| ccache / sccache | Duplicates Bazel's action cache. Use a remote cache instead (3a). |
| Remote execution + shared cache | **Biggest wall-clock lever.** See 3a. |
| Precompiled headers | Clang supports `-include-pch` together with `-fpch-instantiate-templates`, and `-fpch-codegen` to emit inline code once. Bazel has no native PCH, so this needs a small Starlark rule that builds one `.pch` per configuration with the exact compile flags and adds it as an input. Each unity chunk still spends ~4s (p10) re-parsing the same headers; a PCH removes most of that. Est. 10–20%, but it makes every chunk depend on the PCH, so editing a header in it rebuilds everything. |
| Clang header modules (explicit) | Bazel's C++ rules support `header_modules` and `layering_check` (Google builds this way), but `toolchains_llvm` doesn't configure them. Parsing becomes deserializing a `.pcm`, for libc++, Seastar and fmt. Larger payoff than PCH, but a toolchain project, and Seastar's headers would need module maps. |
| C++20 named modules / `import std` | Bazel 9 has experimental support (`--experimental_cpp20_modules`, the `cpp20_modules` feature, and `module_interfaces` on `cc_library`), and libc++ 22 ships `std` modules. Mixing `#include <...>` of std headers with `import std` in one TU is still fragile, and every Seastar header includes std headers. Not ready for this codebase. |
| ClangBuildAnalyzer, `clang-include-cleaner`, IWYU | Useful to drive 3d. The `-ftime-trace` mode in `build-timing` already does the attribution. |
| `--config=release` vs fastbuild for development | fastbuild is 18% less CPU. Use it for the edit-compile-test loop and keep release for CI and images. |

## 4. Dependencies: fork Seastar?

### What Seastar costs

| | per-file build | unity build |
|---|---:|---:|
| Seastar's own 96 `.cc` | 760s (4.8%) | 484s (6.9%) |
| Building `ragel` + `colm` for its HTTP parsers | 229s (1.5%) | 179s (2.5%) |
| Dependencies only Seastar needs: lksctp, Boost program_options/filesystem/thread, hwloc configure | ~180s | ~180s |
| Its headers in our TUs (first-inclusion parse time, fastbuild trace) | ~1,360s (~16% of src/v) | est. ~6–8% |
| Its templates instantiated in our TUs (top 2,000 only) | ≥518s | |

### What redpanda doesn't use

We use the reactor, futures and coroutines, `sharded`, the memory
allocator, file I/O (aio/io_uring), the POSIX network stack, TLS, the
HTTP server (admin API), `json2code` (Swagger), Prometheus metrics,
`app_template`/program options, logging, `process` (debug bundle), the
c-ares DNS resolver and `alien`.

`src/v` includes nothing from the parts below, but they are compiled and
linked anyway:

| unused part | compile time (per-file) |
|---|---:|
| Native network stack + DPDK: `native-stack`, `tcp`, `udp`, `ip`, `arp`, `dhcp`, `virtio`, `ethernet`, `packet`, `proxy`, `dpdk` | ~75s |
| Seastar RPC framework (`src/rpc`, we have our own) | 49s |
| WebSocket client/server | 33s |
| HTTP client (`http/client.cc`, `retry_strategy.cc`) | 17s |
| Prometheus protobuf format (`metrics2.pb.cc`) | 9s, but it is why Seastar depends on protobuf |
| collectd exporter (`scollectd.cc`) | 8s |
| SCTP (`lksctp` dependency) | small |

That is about 190 CPU-s, 1.2% of the per-file build.

### Recommendation: patch, don't fork

A hard fork that cut Seastar down to what we use would save about 1–2% on
Seastar's own sources. It would also move every upstream fix (the reactor
and I/O scheduler get real fixes) onto us. The big cost, Seastar's
headers in our TUs, doesn't shrink with a fork unless we also rewrite
`future.hh`, `smp.hh` and `sstring.hh`. Those headers are what we use.

`bazel/thirdparty/seastar.BUILD` is already our own file, so most of the
win needs no fork at all:

1. Drop the unused sources from `srcs`: native stack, DPDK, RPC,
   WebSocket, HTTP client, collectd. It needs a small patch so
   `reactor.cc` stops registering the native stack.
2. Make the Prometheus protobuf format optional (a small patch, same
   approach as `abseil-forward-declare-cord.patch`). With bundle B, this
   removes protobuf, most of abseil and the Go toolchain from the build.
3. Check in the generated `ragel` parsers.
4. Unity-batch Seastar's sources (3c).
5. In our own headers, stop including Seastar headers we don't need:
   `smp.hh` and `net/ip.hh` (3d).

Together that is about 500–700 CPU-s per-file (3–4%) plus the header
savings, with an upstream rebase cost about the size of today's abseil
patch.

### Other dependencies

| dependency | cost | note |
|---|---:|---|
| Protobuf runtime | 432s | Only for Admin API v2 and Seastar's metrics. Goes with bundle B + Seastar patch 2 |
| Abseil | 302s | We use containers, hash, strings and random. log, flags, status, crc, synchronization, time and debugging (~170s) are only there for protobuf |
| Boost compiled libraries | 297s | Includes **Boost.Serialization, 48 TUs, 123s**, which nothing in `src/v` includes. It comes in transitively through a `rules_boost` target (likely `@boost//:uuid` or `@boost//:random`; confirm with `bazel cquery 'somepath(//src/v/redpanda, @boost//:serialization)'`) and can be cut with a `rules_boost` patch. Boost.Iostreams and xz/bzip2 (~50s) likewise |
| OpenSSL | 138s configure, on the critical path | Native build (3b) |
| hwloc, c-ares | 157s | Native builds (3b) |
| yaml-cpp, re2 | 125s | Used (config, SCRAM names, throughput groups). Keep |

## Suggested order

1. **Unity for generated codecs, Swagger and protos** (3c): ~8% of the
   unity build, low effort.
2. **Preinstalled make/pkg-config, native OpenSSL/hwloc/c-ares** (3b):
   ~6% CPU, halves the critical path.
3. **Bigger runner or remote execution** (3a): 2–3× wall time, no code.
4. **Feature bundle A (audit + Kafka client) and the small ones E–I,
   K:** ~8% per-file / ~6% unity, low risk.
5. **The six header fixes** in 3d: 8–12% under unity, each small.
6. **Bundle B + Seastar patches 1–3:** ~10% plus dropping protobuf and Go
   from the build.
7. Then decide on PCH, and on bundles C/D (auth, EOS), which are the
   expensive ones to remove.

Doing 1–6 should take the unity release build from ~7,000 to roughly
4,000–4,500 CPU-s. On a 30-core runner or remote execution, with the
critical path fixed, that is a cold build of 2–4 minutes.
