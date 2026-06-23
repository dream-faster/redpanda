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
class dedup_window_filter {
public:
    explicit dedup_window_filter(std::chrono::milliseconds window);

    /// \brief Filter duplicate records from a batch.
    ///
    /// Returns the original batch if no records are filtered (fast path).
    /// Returns a rebuilt batch with duplicates removed if some are filtered.
    /// Returns std::nullopt if every record in the batch is a duplicate.
    ///
    /// Compressed batches are decompressed before filtering. The returned
    /// batch, when rebuilt, is always uncompressed.
    std::optional<model::record_batch> filter(model::record_batch batch);

    /// \brief Seed the map with a key and its timestamp.
    ///
    /// Used to pre-populate the map from the committed log tail after a
    /// leadership change. Keeps the most recent timestamp per key.
    void populate(const iobuf& key, model::timestamp ts);

    /// \brief Reset state (e.g., on leadership change).
    void clear();

    size_t map_size() const { return _map.size(); }

private:
    bool is_duplicate(const iobuf& key, model::timestamp ts);

    std::chrono::milliseconds _window;
    chunked_hash_map<bytes, model::timestamp> _map;
};

} // namespace cluster
