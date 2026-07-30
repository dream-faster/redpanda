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

/// An ordered, forward mutation to the dedup index. A timestamp is a put and
/// std::nullopt is a delete.
struct dedup_index_mutation {
    bytes key;
    std::optional<model::timestamp> timestamp;

    friend bool operator==(
      const dedup_index_mutation&, const dedup_index_mutation&) = default;
};

struct dedup_filter_result {
    std::optional<model::record_batch> batch;
    chunked_vector<dedup_index_entry> admitted;
    chunked_vector<dedup_index_mutation> mutations;
    model::timestamp max_timestamp{model::timestamp::min()};
    size_t inserts_since_evict{0};
};

struct dedup_index_snapshot {
    chunked_vector<dedup_index_entry> entries;
    model::timestamp max_timestamp{model::timestamp::min()};
    size_t inserts_since_evict{0};

    friend bool operator==(
      const dedup_index_snapshot&, const dedup_index_snapshot&) = default;
};

struct dedup_index_undo {
    struct entry {
        bytes key;
        std::optional<model::timestamp> previous_timestamp;

        friend bool operator==(const entry&, const entry&) = default;
    };

    chunked_vector<entry> entries;
    std::chrono::milliseconds previous_window{0};
    model::timestamp previous_max_timestamp{model::timestamp::min()};
    size_t previous_inserts_since_evict{0};
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
/// The replicated dedup STM owns this filter. Leaders use a speculative copy
/// while followers apply admitted-key updates from the Raft log.
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

    /// Filter a batch and return both the admitted keyed records and the
    /// ordered forward mutations. Admitted records retain v0 compatibility;
    /// mutations let current followers apply the exact physical transition
    /// without re-running eviction policy.
    dedup_filter_result filter_with_updates(model::record_batch batch);

    /// Apply already-admitted records from a replicated state update.
    /// Returns an undo delta used to create Raft snapshots at older offsets.
    dedup_index_undo apply(
      const chunked_vector<dedup_index_entry>&,
      std::chrono::milliseconds window);

    /// Apply a legacy admitted-key update without retaining reverse history.
    void apply_no_undo(
      const chunked_vector<dedup_index_entry>&,
      std::chrono::milliseconds window);

    /// Apply an ordered forward mutation emitted by a leader. Unlike apply(),
    /// this does not re-run eviction policy on the follower: the leader's
    /// explicit puts, deletes, and resulting scalar state are authoritative.
    /// The returned undo is temporary compatibility state for historical Raft
    /// snapshots and is removed once checkpoint-based snapshots are enabled.
    dedup_index_undo apply_forward(
      const chunked_vector<dedup_index_mutation>&,
      std::chrono::milliseconds window,
      model::timestamp max_timestamp,
      size_t inserts_since_evict);

    /// Apply an ordered forward mutation without retaining reverse history.
    void apply_forward_no_undo(
      const chunked_vector<dedup_index_mutation>&,
      std::chrono::milliseconds window,
      model::timestamp max_timestamp,
      size_t inserts_since_evict);

    /// Undo a transition previously returned by apply().
    void revert(const dedup_index_undo&);

    dedup_index_snapshot snapshot() const;
    void restore(const dedup_index_snapshot&);

    /// \brief Seed the map with a key and its timestamp.
    ///
    /// Used by tests and state restoration helpers. Keeps the most recent
    /// timestamp per key.
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
    using undo_entry_map
      = chunked_hash_map<bytes, std::optional<model::timestamp>>;

    bool is_duplicate(
      const iobuf& key,
      model::timestamp ts,
      chunked_vector<dedup_index_entry>* admitted,
      chunked_vector<dedup_index_mutation>* mutations);
    void apply_admitted(const dedup_index_entry&, undo_entry_map*);
    void maybe_evict(
      undo_entry_map* = nullptr,
      chunked_vector<dedup_index_mutation>* = nullptr);
    void evict_expired(
      undo_entry_map*, chunked_vector<dedup_index_mutation>* = nullptr);

    // Number of new-key insertions between opportunistic eviction sweeps.
    static constexpr size_t evict_after_inserts = 10000;

    std::chrono::milliseconds _window;
    chunked_hash_map<bytes, model::timestamp> _map;
    // Highest record timestamp seen; used as the reference "now" for eviction.
    model::timestamp _max_ts{model::timestamp::min()};
    size_t _inserts_since_evict{0};
};

} // namespace cluster
