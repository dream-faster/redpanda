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
#include "model/record.h"
#include "model/timestamp.h"

#include <chrono>
#include <optional>

namespace cluster {

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
/// On leadership change, call clear() to reset the in-memory map. A future
/// improvement is to rebuild the map from the log tail on becoming leader.
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
    /// Returns a rebuilt, uncompressed batch with duplicates removed if some
    /// are filtered. Returns std::nullopt if every record is a duplicate.
    ///
    /// Compressed batches are decompressed into a temporary for inspection; the
    /// original compressed batch is left untouched.
    std::optional<model::record_batch> filter(model::record_batch batch);

    /// \brief Seed the map with a key and its timestamp.
    ///
    /// Used to pre-populate the map from the committed log tail after a
    /// leadership change. Keeps the most recent timestamp per key.
    void populate(const iobuf& key, model::timestamp ts);

    /// \brief Drop entries older than the window relative to the most recent
    /// timestamp seen. Called opportunistically from filter() to bound memory;
    /// exposed for testing.
    void evict_expired();

    /// \brief Reset state (e.g., on leadership change).
    void clear();

    std::chrono::milliseconds window() const { return _window; }

    /// \brief Update the window in place (e.g., on topic config change). The
    /// existing per-key state is preserved; the new window applies to all
    /// subsequent comparisons.
    void set_window(std::chrono::milliseconds w) { _window = w; }

    size_t map_size() const { return _map.size(); }

private:
    bool is_duplicate(const iobuf& key, model::timestamp ts);
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
