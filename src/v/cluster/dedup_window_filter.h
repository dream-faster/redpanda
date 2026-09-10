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
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "model/record.h"
#include "model/timestamp.h"

#include <chrono>
#include <cstdint>
#include <optional>

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

struct dedup_index_entry {
    dedup_identity_digest identity;
    model::timestamp timestamp;

    friend bool
    operator==(const dedup_index_entry&, const dedup_index_entry&) = default;
};

/// Outcome of extracting the dedup identity for one record under a given
/// identity source (see dedup_identity_for_record()).
enum class dedup_identity_lookup {
    /// The record has an identity; the output parameter was populated.
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

/// \brief Extract the dedup identity for one record.
///
/// With key_header unset, the identity is the record's Kafka key (the
/// original behavior). With key_header set, the identity is the value of the
/// record's first header whose key equals *key_header (first-match is
/// deterministic when a record carries repeated header keys); the Kafka key
/// is never consulted in this mode. On dedup_identity_lookup::identity, *out
/// points into r and is valid only as long as r is.
dedup_identity_lookup dedup_identity_for_record(
  model::record& r,
  const std::optional<ss::sstring>& key_header,
  const iobuf** out);

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
    size_t inserts_since_evict{0};

    friend bool operator==(
      const dedup_index_snapshot&, const dedup_index_snapshot&) = default;
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
/// than the identity bytes. And entries older than the window (relative to
/// the most recent timestamp seen) can never cause a drop again and are
/// swept opportunistically as new identities are inserted; the sweep
/// interval scales with the index so that scanning it stays amortized O(1)
/// per insertion. Finally, a hard per-partition entry limit prevents a large
/// window, high-cardinality traffic, or non-advancing producer timestamps from
/// exhausting shard memory. Once that limit is reached, new identities are
/// admitted but left unindexed until a later eviction sweep makes room;
/// already-indexed identities continue to deduplicate normally. The "most
/// recent timestamp seen" is a client-supplied CreateTime with no ordering
/// guarantee, so every record timestamp is clamped to the broker's clock
/// before it reaches the index; without that, one future-dated record would
/// advance the eviction cutoff past the whole index and silently disable
/// dedup for the partition -- see the log-derived dedup RFC, boundary B5.
/// Reordering within the window is still unbounded by the clamp, so
/// window-edge eviction remains approximate.
///
/// Both ways the filter can silently stop deduplicating -- timestamp skew and
/// entry-limit saturation -- are counted, and dedup_stm exports the counters
/// per partition.
class dedup_window_filter {
public:
    // One million is also the default of the cluster-level
    // dedup_max_entries_per_partition setting. The constructor parameter keeps
    // the filter independent of configuration plumbing and makes
    // small-capacity behavior directly testable.
    static constexpr size_t default_max_entries = 1'000'000;

    explicit dedup_window_filter(
      std::chrono::milliseconds window,
      size_t max_entries = default_max_entries);

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

    dedup_index_snapshot snapshot() const;
    void restore(const dedup_index_snapshot&);

    /// \brief Record an identity admitted into the log.
    ///
    /// Keeps the most recent timestamp per identity (idempotent, max-wins),
    /// so applying the same log prefix any number of times converges. Used
    /// by the STM apply path and by state restoration helpers. Participates
    /// in eviction accounting so follower maps stay bounded.
    void populate(const iobuf& identity, model::timestamp ts);

    /// \brief Drop entries older than the window relative to the most recent
    /// timestamp seen. Called opportunistically from filter() to bound memory;
    /// exposed for testing.
    void evict_expired();

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

private:
    /// Bound a client-supplied CreateTime by the broker's clock, counting the
    /// record when the clamp bites. A no-op for any timestamp at or behind
    /// broker time, so log replay is unaffected.
    model::timestamp clamp_to_broker_time(model::timestamp);

    bool is_duplicate(
      dedup_identity_digest identity,
      model::timestamp ts,
      dedup_request_undo* undo);
    void maybe_evict();

    // Floor on the number of new-identity attempts between opportunistic
    // eviction sweeps. Attempts are counted even at the hard entry limit so a
    // saturated index still rechecks for expired entries periodically. The
    // actual interval scales with the map (see maybe_evict()) so that a sweep,
    // which is O(map size), stays amortized O(1) per attempt instead of
    // degrading linearly as the index grows.
    static constexpr size_t min_evict_interval = 10000;
    // Fraction of the map worth of new-identity attempts before sweeping:
    // interval = max(min_evict_interval, map_size / evict_size_divisor).
    static constexpr size_t evict_size_divisor = 8;

    std::chrono::milliseconds _window;
    const size_t _max_entries;
    std::optional<ss::sstring> _key_header;
    chunked_hash_map<
      dedup_identity_digest,
      model::timestamp,
      dedup_identity_digest_hash>
      _map;
    // Highest record timestamp seen; used as the reference "now" for eviction.
    model::timestamp _max_ts{model::timestamp::min()};
    // Kept under its original name because it is persisted in snapshots; at
    // the entry limit it also counts new-identity attempts that were not
    // indexed.
    size_t _inserts_since_evict{0};
    size_t _dropped_records{0};
    size_t _skew_clamped_records{0};
    size_t _unindexed_records{0};
};

} // namespace cluster
