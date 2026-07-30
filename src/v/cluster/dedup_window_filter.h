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
};

struct dedup_index_snapshot {
    chunked_vector<dedup_index_entry> entries;
    model::timestamp max_timestamp{model::timestamp::min()};
    size_t inserts_since_evict{0};

    friend bool operator==(
      const dedup_index_snapshot&, const dedup_index_snapshot&) = default;
};

/// \brief Write-path, first-wins per-key deduplication.
///
/// Records are dropped at produce time (before Raft replication) when their
/// key was already seen within the configured window. This is "first-wins":
/// the earliest record for a given key within the window is kept; later
/// duplicates within the window are silently discarded.
///
/// Only call filter() on plain (non-idempotent, non-transactional) produce
/// batches. Idempotent and transactional batches must bypass this filter so
/// their producer sequence numbers remain intact.
///
/// The index is a deterministic function of the data batches in the Raft
/// log: the replicated dedup STM rebuilds it on every replica by calling
/// populate() for each keyed record it applies. Leaders mutate the same map
/// directly at classification time (filter_request) and undo exactly one
/// request's mutations if its replication fails (revert_request).
///
/// Memory is bounded: entries older than the window (relative to the most
/// recent timestamp seen) can never cause a drop again and are swept
/// opportunistically as new keys are inserted.
class dedup_window_filter {
public:
    explicit dedup_window_filter(std::chrono::milliseconds window);

    /// \brief Filter duplicate records from a batch.
    ///
    /// Returns the original batch unchanged if no records are filtered (fast
    /// path; preserves the original compression, attrs, and timestamps).
    /// Returns a rebuilt batch, preserving the original compression and record
    /// metadata, with duplicates removed if some are filtered. Returns
    /// std::nullopt if every record is a duplicate.
    ///
    /// Compressed batches are decompressed into a temporary for inspection; the
    /// original compressed batch is left untouched.
    std::optional<model::record_batch> filter(model::record_batch batch);

    /// Filter a batch and additionally return this request's undo list so the
    /// caller can revert the index mutations if replication fails.
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

    /// \brief Record a key admitted into the log.
    ///
    /// Keeps the most recent timestamp per key (idempotent, max-wins), so
    /// applying the same log prefix any number of times converges. Used by
    /// the STM apply path and by state restoration helpers. Participates in
    /// eviction accounting so follower maps stay bounded.
    void populate(const iobuf& key, model::timestamp ts);

    /// \brief Drop entries older than the window relative to the most recent
    /// timestamp seen. Called opportunistically from filter() to bound memory;
    /// exposed for testing.
    void evict_expired();

    /// \brief Reset all index state.
    void clear();

    std::chrono::milliseconds window() const { return _window; }

    /// \brief Update the window in place (e.g., on topic config change). The
    /// existing per-key state is preserved; the new window applies to all
    /// subsequent comparisons.
    void set_window(std::chrono::milliseconds w) { _window = w; }

    size_t map_size() const { return _map.size(); }
    model::timestamp max_timestamp() const { return _max_ts; }

private:
    bool is_duplicate(
      const iobuf& key, model::timestamp ts, dedup_request_undo* undo);
    void maybe_evict();

    // Number of new-key insertions between opportunistic eviction sweeps.
    static constexpr size_t evict_after_inserts = 10000;

    std::chrono::milliseconds _window;
    chunked_hash_map<bytes, model::timestamp> _map;
    // Highest record timestamp seen; used as the reference "now" for eviction.
    model::timestamp _max_ts{model::timestamp::min()};
    size_t _inserts_since_evict{0};
};

} // namespace cluster
