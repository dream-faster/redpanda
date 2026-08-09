// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "bytes/bytes.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "model/record.h"
#include "model/timestamp.h"

#include <chrono>
#include <optional>

namespace cluster {

struct dedup_index_entry {
    bytes key;
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
        bytes key;
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
/// Memory is bounded: entries older than the window (relative to the most
/// recent timestamp seen) can never cause a drop again and are swept
/// opportunistically as new identities are inserted. The "most recent
/// timestamp seen" is a client-supplied CreateTime with no ordering
/// guarantee, so an anomalously future-timestamped record can advance the
/// eviction cutoff early and evict an entry a later, correctly-ordered
/// duplicate should still have matched -- see the log-derived dedup RFC,
/// boundary B5.
class dedup_window_filter {
public:
    explicit dedup_window_filter(std::chrono::milliseconds window);

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
    model::timestamp max_timestamp() const { return _max_ts; }

private:
    bool is_duplicate(
      const iobuf& identity, model::timestamp ts, dedup_request_undo* undo);
    void maybe_evict();

    // Number of new-identity insertions between opportunistic eviction
    // sweeps.
    static constexpr size_t evict_after_inserts = 10000;

    std::chrono::milliseconds _window;
    std::optional<ss::sstring> _key_header;
    chunked_hash_map<bytes, model::timestamp> _map;
    // Highest record timestamp seen; used as the reference "now" for eviction.
    model::timestamp _max_ts{model::timestamp::min()};
    size_t _inserts_since_evict{0};
};

} // namespace cluster
