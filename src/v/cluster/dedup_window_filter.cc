// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/dedup_window_filter.h"

#include "hashing/xx.h"
#include "model/batch_builder.h"
#include "model/batch_compression.h"
#include "model/record.h"

#include <seastar/core/lowres_clock.hh>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <ranges>

namespace cluster {

dedup_identity_lookup dedup_identity_for_record(
  model::record& r,
  const std::optional<ss::sstring>& key_header,
  const iobuf** out) {
    if (key_header) {
        for (auto& h : r.headers()) {
            if (h.key() != std::string_view{*key_header}) {
                continue;
            }
            // A header value can be null, which is not the same as empty: the
            // iobuf behind it is empty either way, so digesting it would give
            // every record with a null-valued header the identity of the empty
            // buffer and silently deduplicate them against each other.
            // Treated like an absent header instead -- the client asked for
            // header-keyed dedup and supplied no identity -- so the request is
            // rejected rather than losing records.
            if (h.value_size() < 0) {
                return dedup_identity_lookup::missing_header;
            }
            *out = &h.value();
            return dedup_identity_lookup::identity;
        }
        return dedup_identity_lookup::missing_header;
    }
    if (!r.has_key()) {
        return dedup_identity_lookup::no_identity;
    }
    *out = &r.key();
    return dedup_identity_lookup::identity;
}

dedup_identity_digest dedup_digest_of(const iobuf& identity) {
    // Arbitrary distinct constants; they only need to differ from each other
    // and stay fixed, since the digest is persisted in snapshots.
    incremental_xxhash64 hi{0x9E3779B97F4A7C15ULL};
    incremental_xxhash64 lo{0xC2B2AE3D27D4EB4FULL};
    for (const auto& frag : identity) {
        hi.update(frag.get(), frag.size());
        lo.update(frag.get(), frag.size());
    }
    return {.hi = hi.digest(), .lo = lo.digest()};
}

dedup_window_filter::dedup_window_filter(
  std::chrono::milliseconds window, size_t max_entries)
  : _window(window)
  , _max_entries(max_entries) {
    begin_pass();
}

namespace {

model::timestamp broker_now() {
    return model::timestamp{
      std::chrono::duration_cast<std::chrono::milliseconds>(
        ss::lowres_system_clock::now().time_since_epoch())
        .count()};
}

} // namespace

model::timestamp
dedup_window_filter::clamp_to_broker_time(model::timestamp ts) {
    // Client CreateTime is untrusted input. Redpanda admits records up to
    // log_message_timestamp_after_max_ms (one hour by default) ahead of the
    // broker, and _max_ts is a running maximum over those values, so a single
    // future-dated record would push the eviction cutoff (_max_ts - _window)
    // past every entry in the index. The next sweep would then empty it, and
    // dedup would stay off for the partition until the generation is bumped.
    //
    // Clamping is the identity for any record older than the broker's clock --
    // which is every record seen during log replay -- so the index remains a
    // deterministic function of the log rather than of replay timing. See the
    // log-derived dedup RFC, boundary B5.
    const auto now = broker_now();
    if (ts > now) {
        ++_skew_clamped_records;
        return now;
    }
    return ts;
}

bool dedup_window_filter::is_duplicate(
  dedup_identity_digest identity,
  model::timestamp ts,
  dedup_request_undo* undo) {
    auto it = _map.find(identity);
    if (it != _map.end()) {
        auto stored_ts = it->second;
        auto diff_ms = ts.value() - stored_ts.value();
        if (diff_ms >= -_window.count() && diff_ms <= _window.count()) {
            ++_dropped_records;
            return true;
        }
        _max_ts = std::max(_max_ts, ts);
        // Idempotent max-wins, matching populate(): never let a speculative,
        // possibly out-of-order classification regress the stored timestamp
        // below what a later deterministic apply() would compute for the
        // same identity.
        if (ts.value() > stored_ts.value()) {
            it->second = ts;
        }
        if (undo) {
            undo->entries.push_back(
              {.identity = identity,
               .applied_timestamp = it->second,
               .previous_timestamp = stored_ts});
        }
        return false;
    }
    _max_ts = std::max(_max_ts, ts);
    // Counted even when the ceiling leaves the identity unindexed, so the
    // cursor keeps advancing while saturated.
    ++_inserts_since_evict;
    // Before the insert, not after: a follower's populate() sweeps at the same
    // point, and capacity trimming is decision-affecting, so the two orders
    // must match or replicas drift. See populate().
    sweep_step(sweep_budget);
    if (_map.size() >= _max_entries && !make_room_for_one()) {
        // The bounded step found nothing freeable. The record is admitted but
        // this identity is not remembered.
        ++_unindexed_records;
        return false;
    }
    _map.emplace(identity, ts);
    if (undo) {
        undo->entries.push_back(
          {.identity = identity,
           .applied_timestamp = ts,
           .previous_timestamp = std::nullopt});
    }
    return false;
}

std::optional<model::record_batch>
dedup_window_filter::filter(model::record_batch batch) {
    return filter_request(std::move(batch)).batch;
}

dedup_filter_result
dedup_window_filter::filter_request(model::record_batch batch) {
    // Decompress into a temporary so the original (possibly compressed) batch
    // can be returned untouched when nothing is filtered.
    std::optional<model::record_batch> decompressed;
    if (batch.compressed()) {
        decompressed = model::decompress_batch_sync(batch);
    }
    model::record_batch& readable = decompressed ? *decompressed : batch;

    // Copied, not referenced: the batch header is read again after the
    // iterator below has taken a share of the records.
    const auto header = readable.header();
    const auto base_ts = header.first_timestamp;
    const int32_t total = readable.record_count();

    // Walk the records by sharing them rather than copying.
    // record_batch::for_each_record() yields records through
    // record_batch_copy_iterator, which deep-copies every key, value, and
    // header of every record; record_batch_iterator refcounts them instead.
    // That matters because this runs on every plain produce.
    //
    // Classification is deferred until the whole batch has been inspected:
    // in header mode one record missing the configured header rejects the
    // entire request, and the index must not have been mutated by then.
    // Digesting each identity up front is what makes deferring cheap -- what
    // has to survive the pass is 32 bytes per record, not the records
    // themselves, whose iobufs would otherwise pin a multiple of the batch
    // size for the duration of the call.
    struct classified {
        dedup_identity_digest identity;
        model::timestamp timestamp;
        bool has_identity{false};
        bool dropped{false};
    };
    chunked_vector<classified> records;
    records.reserve(static_cast<size_t>(total));

    {
        auto it = model::record_batch_iterator::create(readable.share());
        while (it.has_next()) {
            auto r = it.next();
            const iobuf* identity = nullptr;
            const auto lookup = dedup_identity_for_record(
              r, _key_header, &identity);
            if (lookup == dedup_identity_lookup::missing_header) {
                // Nothing has been mutated yet, so the whole request can be
                // rejected without unwinding anything.
                return {.missing_required_header = true};
            }
            classified c{
              .timestamp = clamp_to_broker_time(
                model::timestamp{base_ts.value() + r.timestamp_delta()})};
            if (lookup == dedup_identity_lookup::identity) {
                c.identity = dedup_digest_of(*identity);
                c.has_identity = true;
            }
            records.push_back(c);
        }
    }

    // Classify in record order, so two records sharing an identity within
    // one request resolve first-wins the same way they would across
    // requests.
    dedup_request_undo undo;
    undo.entries.reserve(static_cast<size_t>(total));
    int32_t kept = 0;
    for (auto& c : records) {
        c.dropped = c.has_identity
                    && is_duplicate(c.identity, c.timestamp, &undo);
        if (!c.dropped) {
            ++kept;
        }
    }

    if (kept == total) {
        // Nothing filtered: return the original batch unchanged (preserving
        // its compression, attrs, and timestamps).
        return {.batch = std::move(batch), .undo = std::move(undo)};
    }
    if (kept == 0) {
        return {.batch = std::nullopt, .undo = std::move(undo)};
    }

    ++_rebuilt_requests;

    // Rebuild with surviving records while preserving batch and record
    // metadata. Offset deltas are made contiguous because this is a produce
    // batch, not a compacted batch with intentional offset gaps.
    model::batch_builder builder;
    builder.set_batch_type(header.type);
    builder.set_base_offset(header.base_offset);
    builder.set_batch_timestamp(header.attrs.timestamp_type(), base_ts);
    // From the original batch, not `header`: `readable` may be the
    // decompressed temporary, whose attrs no longer name the compression the
    // rebuilt batch should be written back with.
    builder.set_compression(batch.header().attrs.compression());
    builder.set_producer_id(header.producer_id);
    builder.set_producer_epoch(header.producer_epoch);
    builder.set_base_sequence(header.base_sequence);
    if (header.attrs.is_transactional()) {
        builder.set_transactional();
    }
    if (header.attrs.is_control()) {
        builder.set_control();
    }

    // Second sharing pass, taken only when something was actually dropped.
    // Re-parsing is cheap next to holding every record's iobufs alive across
    // the classification above, and the fast path above never reaches here.
    int32_t output_offset_delta = 0;
    size_t idx = 0;
    auto rebuild = model::record_batch_iterator::create(readable.share());
    while (rebuild.has_next()) {
        auto r = rebuild.next();
        if (records[idx++].dropped) {
            continue;
        }
        builder.add_record(
          model::record(
            r.attributes(),
            r.timestamp_delta(),
            output_offset_delta++,
            r.share_key_opt(),
            r.share_value_opt(),
            std::move(r.headers())));
    }

    return {.batch = std::move(builder).build_sync(), .undo = std::move(undo)};
}

void dedup_window_filter::revert_request(const dedup_request_undo& undo) {
    // Only this request's own entries are restored. If classifying it forced
    // a capacity eviction, the slice that eviction dropped is gone for good --
    // a failed request can cost coverage that was never its own. Bounded by
    // one slice and consistent with the feature's best-effort framing, but
    // unlike the expired sweep it is not free.
    //
    // Reverse order so multiple mutations of the same key within one request
    // unwind correctly: the last entry restores the state the previous entry
    // wrote, and the first entry restores the pre-request state.
    for (const auto& entry : undo.entries | std::ranges::views::reverse) {
        auto it = _map.find(entry.identity);
        if (it == _map.end() || it->second != entry.applied_timestamp) {
            // A later request overwrote (or evicted) this key; its state wins.
            continue;
        }
        if (entry.previous_timestamp) {
            it->second = *entry.previous_timestamp;
        } else {
            _map.erase(it);
        }
    }
}

void dedup_window_filter::populate(const iobuf& key, model::timestamp ts) {
    ts = clamp_to_broker_time(ts);
    _max_ts = std::max(_max_ts, ts);
    auto identity = dedup_digest_of(key);

    // Followers apply the same capacity rule as the leader does in
    // is_duplicate(), and they must apply it at the same point. Expiry timing
    // never changed a dedup decision -- an expired entry cannot match -- but
    // capacity trimming drops entries that still could, so when the cursor
    // runs relative to the insert is decision-affecting. Both paths therefore
    // look the identity up, sweep, and only then insert.
    //
    // That costs this path the single-probe try_emplace it used to do for a
    // new identity: a miss is now a find plus an emplace. Determinism between
    // leader and follower is worth more than the probe.
    auto it = _map.find(identity);
    if (it != _map.end()) {
        if (ts.value() > it->second.value()) {
            it->second = ts;
        }
        return;
    }
    ++_inserts_since_evict;
    sweep_step(sweep_budget);
    if (_map.size() >= _max_entries && !make_room_for_one()) {
        ++_unindexed_records;
        return;
    }
    _map.emplace(identity, ts);
}

void dedup_window_filter::evict_expired() {
    // An entry is only meaningful while a future record could still be within
    // _window of it. Once _max_ts has advanced beyond the window, the entry can
    // never cause a drop again (the next lookup would treat it as expired), so
    // it is safe to remove.
    const int64_t cutoff = _max_ts.value() - _window.count();
    std::erase_if(
      _map, [cutoff](const auto& kv) { return kv.second.value() < cutoff; });
}

size_t dedup_window_filter::soft_limit() const {
    // A tenth below the ceiling, and at least one entry below it so a tiny
    // configured ceiling still leaves the cursor a margin to trim in.
    return _max_entries
           - std::max<size_t>(1, _max_entries / capacity_soft_margin_divisor);
}

void dedup_window_filter::erase_at(size_t index) {
    _map.erase(_map.begin() + static_cast<std::ptrdiff_t>(index));
}

void dedup_window_filter::begin_pass() {
    _pass_counts.fill(0);
    _pass_visited = 0;
    // The frame the pass measures entries against, frozen for its duration:
    // _max_ts advances while the cursor walks, and buckets shifting underneath
    // it would describe no single moment. Rounding the width up keeps
    // offset / width below capacity_evict_buckets for every offset in
    // [0, span], and dividing rather than scaling avoids the overflow a
    // multiplying form would risk -- redpanda.dedup.window.ms has no
    // upper-bound validator.
    _pass_span = std::max<int64_t>(_window.count(), 0);
    _pass_lo = _max_ts.value() - _pass_span;
    _pass_width = _pass_span / static_cast<int64_t>(capacity_evict_buckets - 1)
                  + 1;
}

void dedup_window_filter::finish_pass() {
    // Take whole buckets while they stay under the target, so the cutoff never
    // designates more than the intended quarter. This is the rule the old
    // whole-map histogram applied, over a pass's worth of entries instead.
    const size_t target = _pass_visited / capacity_evict_divisor;
    size_t accumulated = 0;
    size_t bucket = 0;
    while (bucket < _pass_counts.size()
           && accumulated + _pass_counts[bucket] <= target) {
        accumulated += _pass_counts[bucket];
        ++bucket;
    }
    if (accumulated == 0) {
        _capacity_cutoff = model::timestamp::min();
    } else {
        _capacity_cutoff = model::timestamp{
          _pass_lo + static_cast<int64_t>(bucket) * _pass_width};
    }
    begin_pass();
}

void dedup_window_filter::sweep_step(size_t budget) {
    const int64_t expiry_cutoff = _max_ts.value() - _window.count();
    const auto soft = soft_limit();

    for (size_t step = 0; step < budget; ++step) {
        if (_map.empty()) {
            _sweep_pos = 0;
            return;
        }
        if (_sweep_pos >= _map.size()) {
            // The cursor reached the end, so the histogram it accumulated now
            // covers the index: turn it into the next cutoff, and wrap. An
            // erase elsewhere can also leave the cursor past the end, which
            // just ends the pass early on a partial histogram -- a soft target
            // can afford that, and it stays a function of the call sequence.
            _sweep_pos = 0;
            finish_pass();
        }
        ++_sweep_visits;
        const auto stamp = _map.values()[_sweep_pos].second;
        if (stamp.value() < expiry_cutoff) {
            // Expired: it can never match again, so dropping it costs no
            // coverage. The cursor does not advance -- the erase moved the
            // array's last, still unvisited, entry into this slot.
            erase_at(_sweep_pos);
            continue;
        }
        if (_map.size() >= soft && stamp.value() < _capacity_cutoff.value()) {
            // Inside the window, so this is coverage genuinely lost. Doing it
            // here, a few entries at a time from the soft limit upwards, is
            // what replaced dropping a quarter of the index in one step once
            // it hit the ceiling.
            erase_at(_sweep_pos);
            ++_capacity_evicted_entries;
            continue;
        }
        // restore() can clamp _max_ts below an entry's own timestamp, and the
        // frame is a pass old, so an offset outside the window is reachable.
        const auto offset = std::clamp<int64_t>(
          stamp.value() - _pass_lo, 0, _pass_span);
        ++_pass_counts[static_cast<size_t>(offset / _pass_width)];
        ++_pass_visited;
        ++_sweep_pos;
    }
}

bool dedup_window_filter::make_room_for_one() {
    // One bounded extra step, never a scan. Freeing nothing leaves the caller
    // to admit the record unindexed, which is the trade the refused whole-map
    // sweep already made -- but at a cost that no longer depends on how large
    // the index is. The cursor advances either way, so successive attempts
    // walk the index and eventually complete a pass, recomputing the cutoff.
    ++_capacity_sweeps;
    sweep_step(capacity_emergency_budget);
    return _map.size() < _max_entries;
}

dedup_index_snapshot dedup_window_filter::snapshot() const {
    dedup_index_snapshot result{
      .max_timestamp = _max_ts, .inserts_since_evict = _inserts_since_evict};
    result.entries.reserve(_map.size());
    for (const auto& [identity, timestamp] : _map) {
        result.entries.push_back({identity, timestamp});
    }
    return result;
}

void dedup_window_filter::restore(const dedup_index_snapshot& snapshot) {
    _map.clear();
    const auto restore_count = std::min(snapshot.entries.size(), _max_entries);
    _map.reserve(restore_count);
    if (restore_count == snapshot.entries.size()) {
        for (const auto& entry : snapshot.entries) {
            _map.emplace(entry.identity, entry.timestamp);
        }
    } else {
        // Keep the most recent identities. The index is a dense hash map whose
        // iteration is insertion order, so snapshot() emits roughly oldest
        // first; taking a prefix would drop exactly the entries most likely to
        // still match an incoming duplicate. Ordering a pointer view leaves
        // the caller's snapshot untouched (and it could not be copied anyway:
        // chunked_vector is move-only). nth_element is O(n) and only runs when
        // the snapshot exceeds this node's ceiling.
        chunked_vector<const dedup_index_entry*> by_recency;
        by_recency.reserve(snapshot.entries.size());
        for (const auto& entry : snapshot.entries) {
            by_recency.push_back(&entry);
        }
        std::nth_element(
          by_recency.begin(),
          by_recency.begin() + static_cast<std::ptrdiff_t>(restore_count),
          by_recency.end(),
          [](const dedup_index_entry* a, const dedup_index_entry* b) {
              return a->timestamp > b->timestamp;
          });
        for (size_t i = 0; i < restore_count; ++i) {
            _map.emplace(by_recency[i]->identity, by_recency[i]->timestamp);
        }
        _truncated_entries += snapshot.entries.size() - restore_count;
    }
    // A snapshot written by a broker without the clamp can carry a poisoned
    // watermark; clamping here lets such a partition heal on restart rather
    // than needing a dedup_generation bump. Not counted as record skew.
    _max_ts = std::min(snapshot.max_timestamp, broker_now());
    _inserts_since_evict = snapshot.inserts_since_evict;
    // The cursor indexes the value array it was walking, and the histogram
    // describes that array's contents; neither survives a different map. Both
    // are reset rather than persisted: a pass is bounded work that the next
    // few hundred records redo, and persisting them would put a wire field in
    // the snapshot that every replica would have to agree on. begin_pass()
    // runs last because the frame it freezes is read from _max_ts.
    _sweep_pos = 0;
    _capacity_cutoff = model::timestamp::min();
    begin_pass();
}

void dedup_window_filter::clear() {
    // Assigned rather than cleared: this is a dense map, so clear() keeps the
    // bucket array and value storage it had grown to -- tens of MiB at the
    // default entry ceiling. The callers that reach here (dedup disabled, a
    // generation change, a discarded or applied raft snapshot) all want that
    // memory back. restore() keeps using clear(), since it refills
    // immediately and the capacity is worth keeping there.
    _map = {};
    _max_ts = model::timestamp::min();
    _inserts_since_evict = 0;
    _sweep_pos = 0;
    _capacity_cutoff = model::timestamp::min();
    begin_pass();
}

} // namespace cluster
