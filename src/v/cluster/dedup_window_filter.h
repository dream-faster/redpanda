// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "cluster/dedup_index_limits.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "hashing/xx.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "ssx/semaphore.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/defer.hh>

#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <stdexcept>
#include <vector>

namespace cluster {

/// \brief A fixed-width, process-stable digest of a dedup identity.
///
/// The index stores this instead of the identity bytes, so an entry costs the
/// same regardless of how large the Kafka key or header value is. Two
/// independently seeded xxhash64 passes give 128 bits: at the supported
/// 10^7-entry upper limit, the birthday collision probability is on the order
/// of 10^-25, far below the window-edge and generation-change
/// boundaries the feature already documents as best-effort.
///
/// xxhash64 is not collision-resistant against a chosen-input attacker, which
/// is deliberate: a producer that wants to suppress another producer's
/// identity can already do so directly by writing that identity first, since
/// dedup is first-wins on whatever identity it is given. Digesting adds no
/// capability an attacker does not already have.
///
/// Stability matters: the digest is serialized into snapshots and compared
/// across replicas, so it must not depend on a per-process hash seed the way
/// absl::Hash does.
struct dedup_identity_digest {
    uint64_t hi{0};
    uint64_t lo{0};

    friend bool operator==(
      const dedup_identity_digest&, const dedup_identity_digest&) = default;
};

/// The digest is already avalanched by xxhash64, so the map takes one half
/// verbatim rather than paying to re-mix it.
struct dedup_identity_digest_hash {
    using is_avalanching = void;

    uint64_t operator()(const dedup_identity_digest& d) const noexcept {
        return d.lo;
    }
};

/// \brief Digest one dedup identity.
///
/// Consumes the iobuf fragment by fragment, so no contiguous copy of the
/// identity is ever materialized. xxhash64's streaming API is independent of
/// how the input is split across update() calls, so a fragmented and a
/// contiguous iobuf holding the same bytes digest identically.
dedup_identity_digest dedup_digest_of(const iobuf& identity);

/// \brief Streaming form of dedup_digest_of(): feed the identity's bytes in
/// any pieces, then take the digest. Yields exactly what dedup_digest_of()
/// yields for the same bytes.
class dedup_digester {
public:
    void update(const char* data, size_t size) {
        _hi.update(data, size);
        _lo.update(data, size);
    }
    dedup_identity_digest digest() {
        return {.hi = _hi.digest(), .lo = _lo.digest()};
    }

private:
    // Arbitrary distinct constants; they only need to differ from each other
    // and stay fixed, since the digest is persisted in snapshots.
    incremental_xxhash64 _hi{0x9E3779B97F4A7C15ULL};
    incremental_xxhash64 _lo{0xC2B2AE3D27D4EB4FULL};
};

struct dedup_index_entry {
    dedup_identity_digest identity;
    model::timestamp timestamp;

    friend bool
    operator==(const dedup_index_entry&, const dedup_index_entry&) = default;
};

/// Outcome of extracting the dedup identity for one record under a given
/// identity source (see for_each_dedup_identity()).
enum class dedup_identity_lookup {
    /// The record has an identity; its digest was computed.
    identity,
    /// Key mode (key_header unset) and the record has no Kafka key: dedup
    /// cannot apply to this record. Matches the original, backward-compatible
    /// behavior of always admitting null-key records.
    no_identity,
    /// Header mode (key_header set) and the record has no occurrence of the
    /// configured header. Unlike no_identity, this is never a silent pass:
    /// callers must reject the whole request rather than admit the record
    /// un-deduplicated, since falling back to the Kafka key would silently
    /// defeat the point of choosing a header as the identity source.
    missing_header,
};

/// \brief Walk a batch's records reading only what dedup needs.
///
/// For each record, in order, calls emit(lookup, digest, timestamp) with the
/// identity digested straight from the batch's buffer -- the Kafka key, or
/// in header mode the value of the first header named *key_header -- and the
/// record's timestamp. Nothing is copied or shared, and nothing past the
/// identity is decoded: the rest of the record is skipped by its length.
/// This is the whole of what the produce and apply paths look at per record,
/// and it replaced parsing each record into a model::record (an iobuf per
/// key, value and header) on both.
///
/// emit returns whether to go on; the walk stops at the first false. The
/// digest is meaningful only for dedup_identity_lookup::identity.
///
/// The decoding mirrors model::parse_one_record_from_buffer(): batches reach
/// here already validated, but a malformed one still throws rather than
/// reading past its end.
template<typename F>
void for_each_dedup_identity(
  const model::record_batch& readable,
  const std::optional<ss::sstring>& key_header,
  F&& emit) {
    const auto base_ts = readable.header().first_timestamp.value();
    iobuf_const_parser parser(readable.data());
    auto digest_bytes = [&parser](int64_t len) {
        dedup_digester digester;
        const auto consumed = parser.consume(
          static_cast<size_t>(len), [&digester](const char* data, size_t n) {
              digester.update(data, n);
              return ss::stop_iteration::no;
          });
        if (consumed != static_cast<size_t>(len)) [[unlikely]] {
            throw std::out_of_range(
              fmt::format(
                "dedup identity of {} bytes, but only {} in the record",
                len,
                consumed));
        }
        return digester.digest();
    };
    auto skip_bytes = [&parser](int64_t len) {
        if (len > 0) {
            parser.skip(static_cast<size_t>(len));
        }
    };
    // Reads a length and checks it against what is left before it is used.
    auto read_length = [&parser]() {
        auto [len, _] = parser.read_varlong();
        if (len > static_cast<int64_t>(parser.bytes_left())) [[unlikely]] {
            throw std::out_of_range(
              fmt::format(
                "dedup: field of {} bytes, but only {} left in the batch",
                len,
                parser.bytes_left()));
        }
        return len;
    };
    for (int32_t i = 0; i < readable.record_count(); ++i) {
        // The length counts the bytes after itself. Whatever dedup does not
        // read is skipped as one span to this end, rather than header by
        // header.
        const auto size = read_length();
        if (size < 0) [[unlikely]] {
            throw std::out_of_range(
              fmt::format("dedup: record of negative size {}", size));
        }
        const size_t record_end = parser.bytes_consumed()
                                  + static_cast<size_t>(size);
        parser.skip(1); // attributes
        auto [ts_delta, _tv] = parser.read_varlong();
        parser.read_varlong(); // offset delta
        const auto key_len = read_length();
        auto lookup = dedup_identity_lookup::no_identity;
        dedup_identity_digest digest;
        if (!key_header) {
            // An empty key is an identity, a null one (-1) is not.
            if (key_len >= 0) {
                lookup = dedup_identity_lookup::identity;
                digest = digest_bytes(key_len);
            }
        } else {
            skip_bytes(key_len);
            skip_bytes(read_length()); // value
            const auto header_count = read_length();
            lookup = dedup_identity_lookup::missing_header;
            // The first occurrence of the configured header is the identity,
            // whatever follows it, as the model::record based lookup had it.
            for (int64_t h = 0; h < header_count; ++h) {
                const auto hkey_len = read_length();
                bool matches = false;
                if (hkey_len == static_cast<int64_t>(key_header->size())) {
                    const char* want = key_header->data();
                    matches = true;
                    parser.consume(
                      static_cast<size_t>(hkey_len),
                      [&want, &matches](const char* data, size_t n) {
                          matches = matches && std::memcmp(want, data, n) == 0;
                          want += n;
                          return ss::stop_iteration::no;
                      });
                } else {
                    skip_bytes(hkey_len);
                }
                const auto hval_len = read_length();
                if (!matches) {
                    skip_bytes(hval_len);
                    continue;
                }
                // A null value is not an identity: treated like an absent
                // header, so the request is rejected rather than every such
                // record deduplicating against the empty buffer.
                if (hval_len >= 0) {
                    lookup = dedup_identity_lookup::identity;
                    digest = digest_bytes(hval_len);
                }
                break;
            }
        }
        const auto at = parser.bytes_consumed();
        if (at > record_end) [[unlikely]] {
            throw std::out_of_range(
              fmt::format(
                "dedup: record fields run {} bytes past the record's size",
                at - record_end));
        }
        parser.skip(record_end - at);
        if (!emit(lookup, digest, model::timestamp{base_ts + ts_delta})) {
            return;
        }
    }
}

/// Per-request record of the index mutations performed while classifying one
/// produce request, sufficient to undo exactly this request's insertions if
/// its replication fails. This is not a general history: it lives only for
/// the duration of one replicate call on the leader.
struct dedup_request_undo {
    struct entry {
        dedup_identity_digest identity;
        /// The timestamp this request wrote into the index for the key.
        model::timestamp applied_timestamp;
        /// The timestamp the key held before, or std::nullopt if the key was
        /// newly inserted.
        std::optional<model::timestamp> previous_timestamp;

        friend bool operator==(const entry&, const entry&) = default;
    };

    chunked_vector<entry> entries;
};

struct dedup_filter_result {
    std::optional<model::record_batch> batch;
    dedup_request_undo undo;
    /// True iff header mode is active and some record in the request lacked
    /// the configured header: the caller must fail the whole request rather
    /// than replicate anything. batch and undo carry no state in this case;
    /// filter_request() does not mutate the index when this is set.
    bool missing_required_header{false};
};

struct dedup_index_snapshot {
    chunked_vector<dedup_index_entry> entries;
    model::timestamp max_timestamp{model::timestamp::min()};

    friend bool operator==(
      const dedup_index_snapshot&, const dedup_index_snapshot&) = default;
};

class dedup_window_filter;

/// \brief Entry count shared by every dedup index on one shard.
///
/// The per-partition ceiling does not bound a shard: partitions are placed on
/// shards by count, not by index size, so several saturated indices can share
/// one shard's memory. Every filter on the shard holds the same budget and
/// reports its own size changes into it; one that inserts while the shard is
/// near its limit trims itself the way it would near its own ceiling, and one
/// that cannot make room in its own index at the limit takes a bounded step
/// on the largest index instead, so an idle partition holding the budget
/// cannot leave a busy one without coverage for good.
struct dedup_shard_budget {
    explicit dedup_shard_budget(size_t max_entries)
      : max_entries(max_entries) {}

    size_t entries{0};
    const size_t max_entries;
    /// Held while a partition's snapshot is read and restored at startup, so
    /// at most one snapshot file is in memory per shard at a time. Partition
    /// starts are not otherwise bounded, and a snapshot can be as large as
    /// the index it holds.
    mutable ssx::semaphore restore_one_at_a_time{1, "dedup/restore"};
    /// Every filter holding this budget. Filters register on construction
    /// and leave on destruction; they are neither copyable nor movable, so
    /// the pointers stay valid.
    chunked_vector<dedup_window_filter*> filters;

    /// The filter holding the most entries. Rescanned only every
    /// largest_refresh_interval lookups: it is consulted on every insert under
    /// shard pressure, a shard can host many partitions, and index sizes move
    /// slowly next to that, so a slightly stale answer costs nothing.
    dedup_window_filter* largest();
    /// Drop a filter from the registry and the cache.
    void forget(dedup_window_filter*);

    static constexpr size_t largest_refresh_interval = 256;
    dedup_window_filter* largest_cache{nullptr};
    size_t lookups_since_refresh{0};
};

/// \brief Write-path, first-wins deduplication.
///
/// Records are dropped at produce time (before Raft replication) when their
/// dedup identity was already seen within the configured window. This is
/// "first-wins": the earliest record for a given identity within the window
/// is kept; later duplicates within the window are silently discarded.
///
/// The identity is normally the record's Kafka key (the original,
/// backward-compatible behavior). When a dedup key header is configured
/// (set_key_header()), the identity is instead the value of that header on
/// each record; the Kafka key is not consulted, and a record without an
/// occurrence of the configured header can never be admitted un-deduplicated
/// -- see filter_request().
///
/// Only call filter()/filter_request() on plain (non-idempotent,
/// non-transactional) produce batches. Idempotent and transactional batches
/// must bypass this filter so their producer sequence numbers remain intact.
///
/// The index is a deterministic function of the data batches in the Raft
/// log: the replicated dedup STM rebuilds it on every replica by calling
/// populate() for each record's identity as it applies. Leaders mutate the
/// same map directly at classification time (filter_request) and undo
/// exactly one request's mutations if its replication fails
/// (revert_request).
///
/// Memory is bounded three ways. Each entry costs the same regardless of
/// identity size, because the index stores a dedup_identity_digest rather
/// than the identity bytes. Entries older than the window (relative to the
/// most recent timestamp seen) can never cause a drop again, and a cursor
/// that advances a few entries per new identity drops them as it goes.
/// Finally, a hard per-partition entry limit prevents a large window,
/// high-cardinality traffic, or non-advancing producer timestamps from
/// exhausting shard memory, and an optional dedup_shard_budget applies the
/// same rule to the sum of every index on the shard. The same cursor keeps the
/// index under that limit: from a soft limit a tenth below it, the cursor also
/// drops the entries a completed pass found to be in the oldest quarter, so the
/// effective window narrows to roughly the limit divided by the new-identity
/// arrival rate rather than dedup switching off for new identities.
///
/// The shard budget makes a partition's effective capacity depend on which
/// other partitions share its shard, and replicas of one partition rarely share
/// a shard with the same set. Under shard pressure replicas therefore trim
/// different amounts: the same best-effort divergence capacity eviction
/// already has (see _sweep_pos), reached from a different cause.
///
/// No path into the filter does work proportional to the index. That is a
/// hard requirement, not an optimization: sweeps happen at the same log
/// position on every replica, so a full pass stalls the leader and then the
/// followers together, and at a multi-million entry ceiling that was long
/// enough to leave nobody free to complete an acks=all quorum.
///
/// The "most recent timestamp seen" is a client-supplied CreateTime with no
/// ordering guarantee, so every record timestamp is clamped to the broker's
/// clock before it reaches the index; without that, one future-dated record
/// would advance the eviction cutoff past the whole index and silently
/// disable dedup for the partition -- see the log-derived dedup RFC, boundary
/// B5. Reordering within the window is still unbounded by the clamp, so
/// window-edge eviction remains approximate.
///
/// Both ways the filter can silently stop deduplicating -- timestamp skew and
/// entry-limit saturation -- are counted, and dedup_stm exports the counters
/// per partition.
class dedup_window_filter {
public:
    // Shared with the cluster-level dedup_max_entries_per_partition setting,
    // which is what actually supplies this in production
    // (application_start.cc). The constructor parameter keeps the filter
    // independent of configuration plumbing and makes small-capacity behavior
    // directly testable.
    static constexpr size_t default_max_entries
      = DEFAULT_DEDUP_MAX_ENTRIES_PER_PARTITION;

    explicit dedup_window_filter(
      std::chrono::milliseconds window,
      size_t max_entries = default_max_entries,
      ss::lw_shared_ptr<dedup_shard_budget> shard_budget = nullptr);

    dedup_window_filter(const dedup_window_filter&) = delete;
    dedup_window_filter& operator=(const dedup_window_filter&) = delete;
    dedup_window_filter(dedup_window_filter&&) = delete;
    dedup_window_filter& operator=(dedup_window_filter&&) = delete;
    ~dedup_window_filter();

    /// \brief Filter duplicate records from a batch.
    ///
    /// Returns the original batch unchanged if no records are filtered (fast
    /// path; preserves the original compression, attrs, and timestamps).
    /// Returns a rebuilt batch, preserving the original compression and record
    /// metadata, with duplicates removed if some are filtered. Returns
    /// std::nullopt if every record is a duplicate, or if the request is
    /// rejected because header mode is active and some record lacked the
    /// configured header (see filter_request()); callers that need to
    /// distinguish these two cases must use filter_request() directly.
    ///
    /// Compressed batches are decompressed into a temporary for inspection; the
    /// original compressed batch is left untouched.
    std::optional<model::record_batch> filter(model::record_batch batch);

    /// Filter a batch and additionally return this request's undo list so the
    /// caller can revert the index mutations if replication fails.
    ///
    /// When header mode is active (key_header() is set) and any record in
    /// the batch has no occurrence of the configured header, the whole
    /// request is rejected: the returned result has
    /// missing_required_header set, batch is std::nullopt, and no index
    /// state is mutated. Callers must fail the request rather than replicate
    /// it -- falling back to the Kafka key would silently defeat the purpose
    /// of configuring a header as the identity source.
    dedup_filter_result filter_request(model::record_batch batch);

    /// Undo the index mutations of one request previously returned by
    /// filter_request(). Compare-and-revert: an entry is only restored or
    /// erased when the map still holds the timestamp this request wrote; keys
    /// overwritten by a later request are left untouched. Eviction sweeps and
    /// the max-timestamp watermark are never reverted (expired entries cannot
    /// influence future decisions, and a monotonically advanced watermark
    /// only sharpens eviction).
    void revert_request(const dedup_request_undo&);

    /// \brief The index state worth persisting, in the map's array order.
    ///
    /// Entries already outside the window are left out: a lookup would treat
    /// them as expired anyway, so persisting them would only grow the
    /// snapshot and the cost of restoring it. They are filtered here rather
    /// than erased from the index because an erase at snapshot time lands at
    /// a different moment on every replica, and the index size it changes
    /// feeds capacity decisions -- see sweep_step().
    dedup_index_snapshot snapshot() const;

    /// \brief Invoke emit(identity, timestamp) for every entry snapshot()
    /// would include, for callers that would otherwise copy the index twice:
    /// once into a dedup_index_snapshot and again into their own wire type.
    template<typename F>
    void for_each_live_entry(F&& emit) const {
        const int64_t cutoff = _max_ts.value() - _window.count();
        for (const auto& [identity, timestamp] : _map) {
            if (timestamp.value() >= cutoff) {
                emit(identity, timestamp);
            }
        }
    }

    /// \brief for_each_live_entry(), yielding to the reactor between chunks
    /// of the index.
    ///
    /// The index changes while the walk is suspended: the produce path
    /// inserts, sweeps and reverts without the apply lock. An insert appends
    /// to the map's value array and is emitted or not depending on where the
    /// cursor is, which a convergent snapshot tolerates either way. An erase
    /// moves the array's last entry into the hole, and when the hole is
    /// behind the cursor that entry would never be reached: erases record
    /// such entries for every walk in progress (see note_erase()), and the
    /// walk emits them at the end. So every entry that was in the index for
    /// the whole walk is emitted, as a non-yielding pass would have.
    ///
    /// Returns false if the index was cleared or replaced during the walk: what
    /// was emitted then describes no single index, and the caller starts over.
    template<typename F>
    ss::future<bool> for_each_live_entry_async(F emit) {
        live_walk walk;
        _walks.push_back(&walk);
        auto unregister = ss::defer(
          [this, &walk]() noexcept { std::erase(_walks, &walk); });
        const int64_t cutoff = _max_ts.value() - _window.count();
        while (!walk.invalidated && walk.cursor < _map.size()) {
            const auto end = std::min(walk.cursor + walk_chunk, _map.size());
            // emit may mutate the index; the bound is re-read each step.
            for (; walk.cursor < end && walk.cursor < _map.size()
                   && !walk.invalidated;
                 ++walk.cursor) {
                const auto& [identity, timestamp] = _map.values()[walk.cursor];
                if (timestamp.value() >= cutoff) {
                    emit(identity, timestamp);
                }
            }
            co_await ss::coroutine::maybe_yield();
        }
        if (walk.invalidated) {
            co_return false;
        }
        for (const auto& [identity, timestamp] : walk.displaced) {
            if (timestamp.value() >= cutoff) {
                emit(identity, timestamp);
            }
        }
        co_return true;
    }

    /// \brief Install a snapshot, replacing any current index state.
    ///
    /// A snapshot holding more identities than this filter may keep -- its
    /// own max_entries(), or what the shard budget has left -- is reduced to
    /// the most recent ones. restore_from() is the only place that selection
    /// happens, so callers must not pre-trim.
    void restore(const dedup_index_snapshot&);

    /// \brief restore() from entries the caller produces, without a
    /// dedup_index_snapshot in between.
    ///
    /// \p for_each_entry(emit) must call emit(identity, timestamp) once per
    /// entry, \p count times in all, produce the same entries each time it is
    /// called, and return a future that resolves once it has. It may yield
    /// between entries: a restore of millions of entries at startup is what
    /// stays off the reactor that way. It runs up to four times: once to find
    /// the oldest entry -- or, when they do not all fit, to find the trim
    /// point, which is the oldest kept entry -- once to measure the capacity
    /// cutoff, and once each to insert the entries older than that cutoff and
    /// the rest. Passes over the snapshot keep the transient cost at 8 bytes
    /// per entry, and only when trimming, rather than a full copy of the
    /// entries.
    ///
    /// The entries old enough to trim are inserted first. The sweep cursor
    /// starts at the front of the index, so after a restart that filled the
    /// shard they are the first it finds; inserted in snapshot order, they
    /// can sit anywhere, and every new identity at the limit is admitted
    /// unindexed until the cursor reaches them.
    ///
    /// Nothing else mutates the index while a restore yields: restores run
    /// under the STM's apply lock, and a produce request waits for the
    /// snapshot to be hydrated before it reaches the filter. The shard budget
    /// is told about the entries as they go in, so a restore on another
    /// partition measuring its headroom meanwhile sees them.
    template<typename ForEach>
    ss::future<> restore_from(
      size_t count, model::timestamp max_timestamp, ForEach for_each_entry) {
        const auto keep = begin_restore(count);
        // Kept: every entry newer than `keep_from`, and the first
        // `at_keep_from` entries exactly at it.
        int64_t keep_from = std::numeric_limits<int64_t>::min();
        size_t at_keep_from = 0;
        const bool trimming = keep > 0 && keep < count;
        if (trimming) {
            // Keep the most recent identities. The index is a dense hash map
            // whose iteration is insertion order, so snapshots run roughly
            // oldest first; taking a prefix would drop exactly the entries
            // most likely to still match an incoming duplicate.
            chunked_vector<int64_t> stamps;
            stamps.reserve(count);
            co_await ss::coroutine::without_preemption_check(for_each_entry(
              [&stamps](dedup_identity_digest, model::timestamp ts) {
                  stamps.push_back(ts.value());
              }));
            const auto smallest_kept
              = stamps.begin() + static_cast<std::ptrdiff_t>(count - keep);
            std::nth_element(stamps.begin(), smallest_kept, stamps.end());
            keep_from = *smallest_kept;
            const auto newer = static_cast<size_t>(std::count_if(
              stamps.begin(), stamps.end(), [keep_from](int64_t ts) {
                  return ts > keep_from;
              }));
            // Everything newer than keep_from fits by construction; entries
            // exactly at it fill what is left, first come first kept. The
            // trim point is one of the kept entries, so at least one is.
            at_keep_from = keep - newer;
        }
        if (keep == 0) {
            begin_restore_pass(max_timestamp, no_pass_oldest);
            finish_restore_pass();
            finish_restore(count);
            co_return;
        }
        // When trimming the oldest kept entry is the trim point itself.
        int64_t oldest = keep_from;
        if (!trimming) {
            oldest = no_pass_oldest;
            co_await ss::coroutine::without_preemption_check(for_each_entry(
              [&oldest](dedup_identity_digest, model::timestamp ts) {
                  oldest = std::min(oldest, ts.value());
              }));
        }
        begin_restore_pass(max_timestamp, oldest);
        co_await ss::coroutine::without_preemption_check(
          for_each_entry([this, trimming, keep_from](
                           dedup_identity_digest, model::timestamp ts) {
              if (!trimming || ts.value() > keep_from) {
                  count_in_pass(ts);
              }
          }));
        if (trimming) {
            for (size_t i = 0; i < at_keep_from; ++i) {
                count_in_pass(model::timestamp{keep_from});
            }
        }
        const int64_t trim_below = finish_restore_pass();

        size_t ties_left = at_keep_from;
        size_t inserted = 0;
        auto insert_part = [&](bool older_part) {
            return for_each_entry(
              [this,
               trimming,
               keep_from,
               trim_below,
               older_part,
               &ties_left,
               &inserted](dedup_identity_digest identity, model::timestamp ts) {
                  if ((ts.value() < trim_below) != older_part) {
                      return;
                  }
                  if (!trimming || ts.value() > keep_from) {
                      restore_entry(identity, ts);
                  } else if (ts.value() == keep_from && ties_left > 0) {
                      restore_entry(identity, ts);
                      --ties_left;
                  } else {
                      return;
                  }
                  if ((++inserted & (restore_sync_interval - 1)) == 0) {
                      sync_shard_budget();
                  }
              });
        };
        if (trim_below > oldest) {
            co_await ss::coroutine::without_preemption_check(insert_part(true));
        }
        co_await ss::coroutine::without_preemption_check(insert_part(false));
        finish_restore(count - keep);
    }

    /// \brief Record an identity admitted into the log.
    ///
    /// Keeps the most recent timestamp per identity (idempotent, max-wins),
    /// so applying the same log prefix any number of times converges. Used
    /// by the STM apply path and by state restoration helpers. Participates
    /// in eviction accounting so follower maps stay bounded.
    void populate(const iobuf& identity, model::timestamp ts);
    /// The same, for an identity already digested (for_each_dedup_identity()).
    void populate(dedup_identity_digest identity, model::timestamp ts);

    /// \brief Reset all index state.
    void clear();

    std::chrono::milliseconds window() const { return _window; }

    /// \brief Update the window in place (e.g., on topic config change). The
    /// existing per-identity state is preserved; the new window applies to
    /// all subsequent comparisons.
    void set_window(std::chrono::milliseconds w) { _window = w; }

    /// \brief Configure the dedup identity source (e.g., on topic config
    /// change). std::nullopt (the default) selects the Kafka key; a set
    /// value names the header whose value becomes the identity. Existing
    /// per-identity state is preserved across the change -- callers that
    /// need to invalidate stale state when the source changes (key vs.
    /// header, or one header name vs. another) must clear() explicitly, the
    /// same way a dedup generation bump invalidates state on disable.
    void set_key_header(std::optional<ss::sstring> h) {
        _key_header = std::move(h);
    }

    const std::optional<ss::sstring>& key_header() const { return _key_header; }

    size_t map_size() const { return _map.size(); }
    size_t max_entries() const { return _max_entries; }
    const dedup_shard_budget* shard_budget() const {
        return _shard_budget.get();
    }
    model::timestamp max_timestamp() const { return _max_ts; }

    /// Records dropped as duplicates. The denominator that makes the two
    /// counters below readable.
    size_t dropped_records() const { return _dropped_records; }
    /// Records whose CreateTime was ahead of the broker's clock and was
    /// clamped. Non-zero means producer clock skew is reaching the index.
    size_t skew_clamped_records() const { return _skew_clamped_records; }
    /// Records admitted without being indexed because the index was at its
    /// entry limit. Non-zero means dedup coverage is degraded for new
    /// identities on this partition.
    size_t unindexed_records() const { return _unindexed_records; }
    /// Identities dropped while installing a snapshot larger than this node
    /// may hold: a peer configured with a larger per-partition limit, or a
    /// shard whose budget other partitions had already filled. Non-zero means
    /// dedup coverage here is narrower than the snapshot it restored.
    size_t truncated_entries() const { return _truncated_entries; }
    /// Still-live identities dropped to make room at the entry limit. Non-zero
    /// means the effective window is narrower than the configured one:
    /// roughly max_entries() divided by the new-identity arrival rate.
    size_t capacity_evicted_entries() const {
        return _capacity_evicted_entries;
    }
    /// Bounded emergency sweep steps taken because a new identity arrived
    /// with the index exactly at the hard ceiling. Non-zero means the soft
    /// limit is not keeping ahead of the arrival rate.
    size_t capacity_sweeps() const { return _capacity_sweeps; }
    /// Entries the sweep cursor has visited, over the filter's lifetime.
    /// Every path into the filter visits a bounded number, so the per-call
    /// delta is what proves there is no O(index) work on the produce or apply
    /// path. Exposed for tests rather than as a metric.
    size_t sweep_visits() const { return _sweep_visits; }
    /// Produce requests rebuilt because some, but not all, of their records
    /// were dropped. These are the only requests that pay for a batch rebuild
    /// and a recompression, so this is the rate that drives the dedup
    /// feature's cost on the produce path.
    size_t rebuilt_requests() const { return _rebuilt_requests; }

private:
    /// Bound a client-supplied CreateTime by the broker's clock, counting the
    /// record when the clamp bites. A no-op for any timestamp at or behind
    /// broker time, so log replay is unaffected.
    model::timestamp clamp_to_broker_time(model::timestamp);

    bool is_duplicate(
      dedup_identity_digest identity,
      model::timestamp ts,
      dedup_request_undo* undo);

    /// \brief Advance the sweep cursor over at most \p budget entries.
    ///
    /// Drops the expired ones, and -- once the index is within
    /// capacity_soft_margin_divisor of the ceiling -- the ones below the
    /// capacity cutoff. The cursor persists across calls and wraps at the end
    /// of the value array, so successive calls walk the whole index in
    /// bounded slices instead of one pass. This is the only eviction the
    /// produce and apply paths perform.
    void sweep_step(size_t budget);

    /// One bounded sweep step taken because a new identity stamped
    /// \p incoming arrived with the index at the hard ceiling. Returns whether
    /// it freed a slot; if not, the caller admits the record unindexed.
    bool make_room_for_one(model::timestamp incoming);

    /// At the shard limit, when the bounded step found nothing below the
    /// capacity cutoff: drop the oldest entry the step just walked past, if it
    /// is older than \p incoming.
    void replace_oldest_visited(model::timestamp incoming);

    /// At the shard limit: take one bounded sweep step on the largest index
    /// on the shard, if it is larger than this one, before this index trims
    /// itself. Returns whether this index may now insert.
    bool take_room_from_largest(model::timestamp incoming);

    /// Freeze the bucket frame a pass measures entries against, and clear its
    /// histogram.
    void begin_pass();
    /// Turn a completed pass's histogram into the next capacity cutoff and
    /// start the following pass.
    void finish_pass();
    /// Add \p stamp to the histogram of the pass in progress.
    void count_in_pass(model::timestamp stamp);

    /// Erase the entry the cursor is on. The map moves its last entry into
    /// the freed slot, so the caller must re-examine the same index rather
    /// than advance.
    void erase_at(size_t index);

    /// A walk over the value array in progress (for_each_live_entry_async()).
    struct live_walk {
        /// Entries before it have been emitted.
        size_t cursor{0};
        /// Entries an erase moved from ahead of the cursor to behind it.
        chunked_vector<dedup_index_entry> displaced;
        /// The index was cleared or replaced under the walk.
        bool invalidated{false};
    };
    /// Called before an entry is erased, with its index: records the entry
    /// the map will move into its place for every walk that has passed the
    /// index but not reached the end of the array.
    void note_erase(size_t index);
    /// Called when the whole value array is discarded.
    void invalidate_walks();
    /// Entries emitted between preemption checks. 24 bytes each on the wire,
    /// so about 100 KiB of snapshot per check.
    static constexpr size_t walk_chunk = 4096;
    /// Restored entries between reports to the shard budget. A power of two.
    static constexpr size_t restore_sync_interval = 4096;

    /// Report the map's size change since the last call to the shard budget.
    /// Called after every insert or erase, so the budget is exact whenever a
    /// capacity check reads it.
    void sync_shard_budget();

    /// Whether a new identity can be inserted without exceeding this
    /// partition's ceiling or the shard's.
    bool at_capacity() const;

    /// Clear the index for a restore of \p count entries and return how many
    /// of them it may keep.
    size_t begin_restore(size_t count);
    /// Set the watermark from a snapshot and start a pass over the entries it
    /// keeps, whose oldest timestamp is \p oldest.
    void begin_restore_pass(model::timestamp max_timestamp, int64_t oldest);
    /// Turn the restore pass into the capacity cutoff, and return it.
    int64_t finish_restore_pass();
    /// Insert one restored entry. A snapshot taken while the index was being
    /// mutated can hold an identity twice (see for_each_live_entry_async());
    /// the newer timestamp wins, as it does in populate().
    void restore_entry(dedup_identity_digest identity, model::timestamp ts) {
        auto [it, inserted] = _map.try_emplace(identity, ts);
        if (!inserted) {
            it->second = std::max(it->second, ts);
        }
    }
    /// Account for the restored entries.
    void finish_restore(size_t truncated);

    /// Whether this index is within capacity_soft_margin_divisor of its own
    /// ceiling, so its cursor drops entries below the capacity cutoff.
    bool over_own_soft_limit() const;
    /// Whether the shard is within capacity_soft_margin_divisor of its limit.
    bool over_shard_soft_limit() const;
    /// Whether no index on the shard is larger than this one by more than a
    /// tenth of its size; those are the indices whose cursors trim under shard
    /// pressure. Requires a shard budget.
    bool among_the_largest() const;
    /// Clear the shard budget's largest-index cache if it names this index.
    void stop_being_largest();

    // Entries the cursor visits per new-identity attempt. This matches the
    // throughput of the periodic full passes it replaced -- those swept the
    // whole index once per map_size/8 attempts, so eight entries per attempt
    // amortized -- but spread evenly instead of arriving as one burst. The
    // burst was the problem: at a 6M ceiling a full pass stalled the shard's
    // reactor for ~230ms, and because sweeps fire at the same log position on
    // every replica, the followers stalled together ~0.5s later and left no
    // one free to complete an acks=all quorum.
    static constexpr size_t sweep_budget = 8;
    // Extra entries the cursor may visit when a new identity arrives with the
    // index exactly at the hard ceiling: enough to find something freeable
    // while the soft limit keeps the tail trimmed, small enough to pay on
    // every record. The cursor advances either way, so repeated attempts walk
    // the index and refresh the cutoff rather than redoing the same work.
    static constexpr size_t capacity_emergency_budget = 64;
    // Margin below the hard ceiling, as a divisor of max_entries, at which
    // the cursor starts dropping entries below the capacity cutoff: a tenth,
    // so trimming begins at 90% and the index converges on its target instead
    // of dropping a quarter of itself in one step at the ceiling.
    static constexpr size_t capacity_soft_margin_divisor = 10;

    // Fraction of the index the capacity cutoff designates as droppable:
    // size/capacity_evict_divisor, so a quarter, measured over a completed
    // cursor pass.
    static constexpr size_t capacity_evict_divisor = 4;
    // Buckets in the histogram that locates the cutoff timestamp. Fixed and
    // stack-allocated, so this adds no per-entry memory -- the entry ceiling
    // exists to bound this index, and an LRU list would have cost 8-16 bytes
    // on every entry.
    static constexpr size_t capacity_evict_buckets = 512;

    std::chrono::milliseconds _window;
    const size_t _max_entries;
    ss::lw_shared_ptr<dedup_shard_budget> _shard_budget;
    // This filter's contribution to _shard_budget->entries.
    size_t _budget_accounted{0};
    std::optional<ss::sstring> _key_header;
    chunked_hash_map<
      dedup_identity_digest,
      model::timestamp,
      dedup_identity_digest_hash>
      _map;
    // Highest record timestamp seen; used as the reference "now" for eviction.
    model::timestamp _max_ts{model::timestamp::min()};
    // Cursor into _map.values(), the dense array the hash map keeps entries
    // in. Erasing moves the array's last entry into the freed slot, so an
    // erase at the cursor leaves an unvisited entry there.
    //
    // Replicas agree on this because they insert the same identities in the
    // same order and so lay the array out identically. revert_request() is
    // the exception: it erases entries a follower never inserted, which
    // permutes the leader's array. The two then sweep set-equal maps in
    // different orders and can drop different still-live entries for
    // capacity. That is bounded -- it needs the index above 90% of its
    // ceiling, and which live entry goes is already best-effort -- but it is
    // a divergence the whole-map passes this replaced did not have, and it is
    // inherent to doing bounded work: a cursor is a position in a layout.
    size_t _sweep_pos{0};
    // Walks in progress; one per snapshot being taken, so at most a few.
    std::vector<live_walk*> _walks;
    // Histogram of the entries the pass in progress has visited, measured
    // against the frame frozen at the start of that pass. _max_ts moves while
    // the cursor walks, and buckets shifting underneath it would describe no
    // single moment, so the frame is fixed for the pass.
    std::array<size_t, capacity_evict_buckets> _pass_counts{};
    size_t _pass_visited{0};
    int64_t _pass_lo{0};
    int64_t _pass_span{0};
    int64_t _pass_width{1};
    // Oldest timestamp the pass in progress has visited, which the next pass
    // starts its frame from.
    static constexpr int64_t no_pass_oldest
      = std::numeric_limits<int64_t>::max();
    int64_t _pass_oldest{no_pass_oldest};
    // Timestamp below which the oldest quarter of the index lay when the last
    // pass completed. timestamp::min() means that pass found no bucket prefix
    // small enough to cut, so nothing is dropped for capacity -- more than a
    // quarter shares one narrow range, and dropping it would throw away
    // coverage nobody asked to lose.
    model::timestamp _capacity_cutoff{model::timestamp::min()};
    size_t _sweep_visits{0};
    size_t _dropped_records{0};
    size_t _skew_clamped_records{0};
    size_t _unindexed_records{0};
    size_t _truncated_entries{0};
    size_t _capacity_evicted_entries{0};
    size_t _rebuilt_requests{0};
    size_t _capacity_sweeps{0};
};

} // namespace cluster
