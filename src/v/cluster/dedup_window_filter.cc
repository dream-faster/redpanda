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
#include "model/batch_compression.h"
#include "model/record.h"
#include "utils/vint.h"

#include <seastar/core/lowres_clock.hh>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <iterator>
#include <ranges>

namespace cluster {

static_assert(
  MAX_DEDUP_MAX_ENTRIES_PER_PARTITION <= dedup_compact_index::max_size,
  "the index's positions must cover the largest configurable ceiling");

dedup_identity_digest dedup_digest_of(const iobuf& identity) {
    dedup_digester digester;
    for (const auto& frag : identity) {
        digester.update(frag.get(), frag.size());
    }
    return digester.digest();
}

dedup_window_filter::dedup_window_filter(
  std::chrono::milliseconds window,
  size_t max_entries,
  ss::lw_shared_ptr<dedup_shard_budget> shard_budget)
  : _window(window)
  , _max_entries(std::min(max_entries, dedup_compact_index::max_size))
  , _shard_budget(std::move(shard_budget)) {
    if (_shard_budget) {
        _shard_budget->filters.push_back(this);
    }
    begin_pass();
}

dedup_window_filter::~dedup_window_filter() {
    if (_shard_budget) {
        _shard_budget->entries -= _budget_accounted;
        _shard_budget->forget(this);
    }
}

namespace {

// A tenth below the ceiling, and at least one entry below it so a tiny
// configured ceiling still leaves the cursor a margin to trim in.
size_t soft_limit_of(size_t ceiling, size_t margin_divisor) {
    return ceiling
           - std::min(ceiling, std::max<size_t>(1, ceiling / margin_divisor));
}

} // namespace

dedup_window_filter* dedup_shard_budget::largest() {
    if (
      largest_cache == nullptr
      || ++lookups_since_refresh >= largest_refresh_interval) {
        lookups_since_refresh = 0;
        largest_cache = nullptr;
        for (auto* f : filters) {
            if (
              largest_cache == nullptr
              || f->map_size() > largest_cache->map_size()) {
                largest_cache = f;
            }
        }
    }
    return largest_cache;
}

void dedup_shard_budget::forget(dedup_window_filter* f) {
    // Order is irrelevant, so the last filter takes the leaving one's place.
    for (size_t i = 0; i < filters.size(); ++i) {
        if (filters[i] == f) {
            filters[i] = filters.back();
            filters.pop_back();
            break;
        }
    }
    if (largest_cache == f) {
        largest_cache = nullptr;
    }
}

void dedup_window_filter::stop_being_largest() {
    // An emptied index must not keep answering largest() until the next
    // refresh: room would be taken from an index with nothing to give, and
    // every other index would compare itself against an empty one.
    if (_shard_budget && _shard_budget->largest_cache == this) {
        _shard_budget->largest_cache = nullptr;
    }
}

void dedup_window_filter::sync_shard_budget() {
    if (!_shard_budget) {
        return;
    }
    // A restore in progress counts as the entries it is going to hold, so the
    // room it was granted stays its own while it fills it.
    const auto size = std::max(_index.size(), _budget_reserved);
    _shard_budget->entries = _shard_budget->entries - _budget_accounted + size;
    _budget_accounted = size;
}

bool dedup_window_filter::at_capacity() const {
    if (_index.size() >= _max_entries) {
        return true;
    }
    return _shard_budget
           && _shard_budget->entries >= _shard_budget->max_entries;
}

bool dedup_window_filter::over_own_soft_limit() const {
    return _index.size()
           >= soft_limit_of(_max_entries, capacity_soft_margin_divisor);
}

bool dedup_window_filter::among_the_largest() const {
    // Within a tenth of the largest index's size counts as the largest. With
    // an exact rule, an index tied with an idle one could trim itself only
    // while level with it, so it never built headroom below the shard limit:
    // every pass that ran out of entries old enough to trim left it at the
    // limit. The margin lets it trim down into the soft zone the way an index
    // nearing its own ceiling does. It is relative to the largest index, not
    // to the shard: with many small idle indices filling the shard, a margin
    // of a tenth of the shard would count a new, nearly empty index as one of
    // the largest, so it would only ever trim itself and never grow.
    auto* largest = _shard_budget->largest();
    if (largest == nullptr || largest == this) {
        return true;
    }
    const auto largest_size = largest->_index.size();
    const auto margin = std::max<size_t>(
      1, largest_size / capacity_soft_margin_divisor);
    return largest_size <= _index.size() + margin;
}

bool dedup_window_filter::over_shard_soft_limit() const {
    return _shard_budget
           && _shard_budget->entries >= soft_limit_of(
                _shard_budget->max_entries, capacity_soft_margin_divisor);
}

bool dedup_window_filter::over_shard_reclaim_limit() const {
    return _shard_budget
           && _shard_budget->entries >= soft_limit_of(
                _shard_budget->max_entries, reclaim_margin_divisor);
}

namespace {

model::timestamp broker_now() {
    return model::timestamp{
      std::chrono::duration_cast<std::chrono::milliseconds>(
        ss::lowres_system_clock::now().time_since_epoch())
        .count()};
}

int64_t saturating_sub(int64_t a, int64_t b) {
    int64_t r{};
    if (__builtin_sub_overflow(a, b, &r)) [[unlikely]] {
        return b < 0 ? std::numeric_limits<int64_t>::max()
                     : std::numeric_limits<int64_t>::min();
    }
    return r;
}

int64_t saturating_add(int64_t a, int64_t b) {
    int64_t r{};
    if (__builtin_add_overflow(a, b, &r)) [[unlikely]] {
        return b < 0 ? std::numeric_limits<int64_t>::min()
                     : std::numeric_limits<int64_t>::max();
    }
    return r;
}

/// Whether two timestamps are at most \p window apart. Either can be at the
/// extremes of int64: the index reads an expired entry as its minimum, and
/// client timestamps saturate.
bool within_window(int64_t a, int64_t b, int64_t window) {
    const auto diff = static_cast<__int128>(a) - b;
    return diff >= -window && diff <= window;
}

} // namespace

int64_t dedup_window_filter::expiry_cutoff() const {
    return saturating_sub(_max_ts.value(), _window.count());
}

int64_t dedup_window_filter::reclaim_cutoff() const {
    const auto own = expiry_cutoff();
    if (
      !_shard_budget
      || _shard_time_seen == std::numeric_limits<int64_t>::min()) {
        return own;
    }
    const auto idle = std::max<int64_t>(
      saturating_sub(_shard_budget->newest_timestamp, _shard_time_seen), 0);
    return saturating_add(own, idle);
}

void dedup_window_filter::note_shard_time(model::timestamp ts) {
    if (!_shard_budget) {
        return;
    }
    _shard_budget->newest_timestamp = std::max(
      _shard_budget->newest_timestamp, ts.value());
    _shard_time_seen = _shard_budget->newest_timestamp;
}

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
    note_shard_time(ts);
    const auto pos = _index.find(identity);
    if (pos != dedup_compact_index::npos) {
        const auto stored_ts = model::timestamp{_index.timestamp_at(pos)};
        if (
          stored_ts.value() != dedup_compact_index::expired
          && within_window(ts.value(), stored_ts.value(), _window.count())) {
            ++_dropped_records;
            return true;
        }
        _max_ts = std::max(_max_ts, ts);
        // Idempotent max-wins, matching populate(): never let a speculative,
        // possibly out-of-order classification regress the stored timestamp
        // below what a later deterministic apply() would compute for the
        // same identity.
        if (ts.value() > stored_ts.value()) {
            _index.set_timestamp_at(pos, ts.value());
        }
        if (undo) {
            // What the index holds now, which is what a revert compares
            // against: a write the index could only store as expired reads
            // back as expired.
            undo->entries.push_back(
              {.identity = identity,
               .applied_timestamp = model::timestamp{_index.timestamp_at(pos)},
               .previous_timestamp = stored_ts});
        }
        return false;
    }
    _max_ts = std::max(_max_ts, ts);
    // Before the insert, not after: a follower's populate() sweeps at the same
    // point, and capacity trimming is decision-affecting, so the two orders
    // must match or replicas drift. See populate().
    sweep_for_new_identity();
    if (
      (at_capacity() && !take_room_from_largest(ts) && !make_room_for_one(ts))
      || !_index.push_back(identity, ts.value())) {
        // The bounded step found nothing freeable, or the index's table could
        // not take the identity. The record is admitted but this identity is
        // not remembered.
        ++_unindexed_records;
        return false;
    }
    sync_shard_budget();
    if (undo) {
        undo->entries.push_back(
          {.identity = identity,
           .applied_timestamp = model::timestamp{_index.timestamp_at(
             _index.size() - 1)},
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

    // Copied, not referenced: the rebuild below reads it after `readable`
    // has been walked.
    const auto header = readable.header();
    const auto base_ts = header.first_timestamp;
    const int32_t total = readable.record_count();

    // Only the identities and timestamps are read from the records, straight
    // from the batch's buffer (see for_each_dedup_identity()): nothing is
    // copied or shared per record, which matters because this runs on every
    // plain produce.
    //
    // Classification is deferred until the whole batch has been inspected:
    // in header mode one record missing the configured header rejects the
    // entire request, and the index must not have been mutated by then.
    // Digesting each identity up front is what makes deferring cheap -- what
    // has to survive the pass is 64 bytes per record, the identity and where
    // the record lies, not the records themselves, whose iobufs would
    // otherwise pin a multiple of the batch size for the duration of the
    // call.
    struct classified {
        dedup_identity_digest identity;
        model::timestamp timestamp;
        dedup_record_span span;
        bool has_identity{false};
        bool dropped{false};
    };
    chunked_vector<classified> records;
    records.reserve(static_cast<size_t>(total));

    bool missing_header = false;
    for_each_dedup_identity(
      readable,
      _key_header,
      [this, &records, &missing_header](
        dedup_identity_lookup lookup,
        dedup_identity_digest digest,
        model::timestamp ts,
        const dedup_record_span& span) {
          if (lookup == dedup_identity_lookup::missing_header) {
              missing_header = true;
              return false;
          }
          records.push_back(
            {.identity = digest,
             .timestamp = clamp_to_broker_time(ts),
             .span = span,
             .has_identity = lookup == dedup_identity_lookup::identity});
          return true;
      });
    if (missing_header) {
        // Nothing has been mutated yet, so the whole request can be rejected
        // without unwinding anything.
        return {.missing_required_header = true};
    }

    // Classify in record order, so two records sharing an identity within
    // one request resolve first-wins the same way they would across
    // requests.
    dedup_request_undo undo;
    undo.entries.reserve(static_cast<size_t>(total));
    chunked_vector<dedup_identity_digest> dropped;
    int32_t kept = 0;
    const size_t n = records.size();
    const auto identity_at =
      [&records](size_t j) -> const dedup_identity_digest* {
        return records[j].has_identity ? &records[j].identity : nullptr;
    };
    for (size_t i = 0; i < n; ++i) {
        prefetch_ahead(i, n, identity_at);
        auto& c = records[i];
        c.dropped = c.has_identity
                    && is_duplicate(c.identity, c.timestamp, &undo);
        if (c.dropped) {
            dropped.push_back(c.identity);
        } else {
            ++kept;
        }
    }

    if (kept == total) {
        // Nothing filtered: return the original batch unchanged (preserving
        // its compression, attrs, and timestamps).
        return {.batch = std::move(batch), .undo = std::move(undo)};
    }
    if (kept == 0) {
        return {
          .batch = std::nullopt,
          .undo = std::move(undo),
          .dropped = std::move(dropped)};
    }

    ++_rebuilt_requests;

    // The batch is rebuilt from the surviving records' bytes rather than
    // parsed into records and serialized again: each is copied as it is but
    // for its offset delta, made contiguous because this is a produce batch,
    // not a compacted one with intentional gaps, and the length in front of
    // it that counts the delta's bytes. The result is what rebuilding it
    // through model::batch_builder gives (dedup_window_filter_test pins the
    // two together), without an allocation per record.
    //
    // Nothing past the identity walk is decoded, so nothing here can fail on
    // the records themselves; compression can still fail. The insertions are
    // undone before an error propagates: the request fails and nothing of it
    // stays behind.
    try {
        // A survivor's length once its offset delta is re-encoded as
        // offset_delta: what reserving exactly and writing the length need.
        const auto spliced_size =
          [](const dedup_record_span& span, int32_t offset_delta) {
              return static_cast<int64_t>(
                       span.end - span.body - span.offset_delta_size)
                     + static_cast<int64_t>(vint::vint_size(offset_delta));
          };
        size_t spliced_bytes = 0;
        int32_t offset_delta = 0;
        for (const auto& c : records) {
            if (!c.dropped) {
                const auto record_size = spliced_size(c.span, offset_delta++);
                spliced_bytes += vint::vint_size(record_size)
                                 + static_cast<size_t>(record_size);
            }
        }
        iobuf data;
        data.reserve_memory(spliced_bytes);
        iobuf_const_parser parser(readable.data());
        std::array<uint8_t, vint::max_length> varint{};
        const auto append_varint = [&data, &varint](int64_t v) {
            const auto n = vint::serialize(v, varint.data());
            data.append(reinterpret_cast<const char*>(varint.data()), n);
        };
        const auto copy = [&data, &parser](size_t from, size_t to) {
            parser.skip(from - parser.bytes_consumed());
            parser.consume(to - from, [&data](const char* src, size_t n) {
                data.append(src, n);
                return ss::stop_iteration::no;
            });
        };
        int64_t max_timestamp_delta = std::numeric_limits<int64_t>::min();
        offset_delta = 0;
        for (const auto& c : records) {
            if (c.dropped) {
                continue;
            }
            const auto& span = c.span;
            const auto offset_delta_begin = span.body + span.offset_delta_at;
            append_varint(spliced_size(span, offset_delta));
            copy(span.body, offset_delta_begin);
            append_varint(offset_delta++);
            copy(offset_delta_begin + span.offset_delta_size, span.end);
            max_timestamp_delta = std::max(
              max_timestamp_delta, span.timestamp_delta);
        }

        // As model::batch_builder::build_header() lays it out for the same
        // records, but for a max timestamp that saturates where the builder
        // would overflow: the deltas are client-controlled, as in
        // for_each_dedup_identity().
        model::record_batch_attributes attrs;
        if (header.attrs.is_control()) {
            attrs.set_control_type();
        }
        if (header.attrs.is_transactional()) {
            attrs.set_transactional_type();
        }
        attrs.set_timestamp_type(header.attrs.timestamp_type());
        model::record_batch_header::context ctx;
        ctx.term = model::term_id{};
        ctx.owner_shard = ss::this_shard_id();
        auto max_timestamp = base_ts;
        if (
          header.attrs.timestamp_type() == model::timestamp_type::create_time) {
            int64_t max_ts{};
            if (__builtin_add_overflow(base_ts(), max_timestamp_delta, &max_ts))
              [[unlikely]] {
                max_ts = max_timestamp_delta < 0
                           ? std::numeric_limits<int64_t>::min()
                           : std::numeric_limits<int64_t>::max();
            }
            max_timestamp = model::timestamp{max_ts};
        }
        model::record_batch_header out{
          .header_crc = 0,
          .size_bytes = static_cast<int32_t>(
            model::packed_record_batch_header_size + data.size_bytes()),
          .base_offset = header.base_offset,
          .type = header.type,
          .crc = 0,
          .attrs = attrs,
          .last_offset_delta = kept - 1,
          .first_timestamp = base_ts,
          .max_timestamp = max_timestamp,
          .producer_id = header.producer_id,
          .producer_epoch = header.producer_epoch,
          .base_sequence = header.base_sequence,
          .record_count = kept,
          .ctx = ctx,
        };
        model::record_batch rebuilt(
          out, std::move(data), model::record_batch::tag_ctor_ng{});
        // From the original batch, not `header`: `readable` may be the
        // decompressed temporary, whose attrs no longer name the compression
        // the rebuilt batch should be written back with.
        if (
          const auto codec = batch.header().attrs.compression();
          codec != model::compression::none) {
            rebuilt = model::compress_batch_sync(codec, std::move(rebuilt));
        } else {
            rebuilt.header().reset_size_checksum_metadata(rebuilt.data());
        }
        return {
          .batch = std::move(rebuilt),
          .undo = std::move(undo),
          .dropped = std::move(dropped)};
    } catch (...) {
        revert_request(undo);
        throw;
    }
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
        const auto pos = _index.find(entry.identity);
        if (
          pos == dedup_compact_index::npos
          || _index.timestamp_at(pos) != entry.applied_timestamp.value()) {
            // A later request overwrote (or evicted) this key; its state wins.
            continue;
        }
        if (entry.previous_timestamp) {
            _index.set_timestamp_at(pos, entry.previous_timestamp->value());
        } else {
            note_erase(pos);
            _index.erase_at(pos);
        }
    }
    sync_shard_budget();
}

void dedup_window_filter::populate(
  const chunked_vector<dedup_index_entry>& entries) {
    const size_t n = entries.size();
    const auto identity_at = [&entries](size_t j) {
        return &entries[j].identity;
    };
    for (size_t i = 0; i < n; ++i) {
        prefetch_ahead(i, n, identity_at);
        populate(entries[i].identity, entries[i].timestamp);
    }
}

void dedup_window_filter::populate(const iobuf& key, model::timestamp ts) {
    populate(dedup_digest_of(key), ts);
}

void dedup_window_filter::populate(
  dedup_identity_digest identity, model::timestamp ts) {
    ts = clamp_to_broker_time(ts);
    _max_ts = std::max(_max_ts, ts);
    note_shard_time(ts);

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
    const auto pos = _index.find(identity);
    if (pos != dedup_compact_index::npos) {
        if (ts.value() > _index.timestamp_at(pos)) {
            _index.set_timestamp_at(pos, ts.value());
        }
        return;
    }
    sweep_for_new_identity();
    if (
      (at_capacity() && !take_room_from_largest(ts) && !make_room_for_one(ts))
      || !_index.push_back(identity, ts.value())) {
        ++_unindexed_records;
        return;
    }
    sync_shard_budget();
}

void dedup_window_filter::erase_at(size_t index) {
    note_erase(index);
    _index.erase_at(index);
    sync_shard_budget();
}

void dedup_window_filter::note_erase(size_t index) {
    if (_walks.empty()) [[likely]] {
        return;
    }
    const size_t last = _index.size() - 1;
    if (index == last) {
        // Nothing moves.
        return;
    }
    for (auto* walk : _walks) {
        // The entry at `last` moves to `index`. It needs recording only when
        // the walk has passed `index` but has not emitted `last` yet.
        if (index < walk->cursor && last >= walk->cursor) {
            walk->displaced.push_back(
              {_index.identity_at(last),
               model::timestamp{_index.timestamp_at(last)}});
        }
    }
}

void dedup_window_filter::invalidate_walks() {
    for (auto* walk : _walks) {
        walk->invalidated = true;
    }
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
    //
    // The frame starts at the oldest entry the previous pass saw, when that is
    // inside the window, rather than at the window's edge: nothing older can
    // have been added since, and an index written in a burst covers a sliver
    // of a long window. Measured against the whole window it would fall into
    // a handful of buckets, and whole buckets are all the cutoff can take, so
    // it would mark far less than a quarter -- or nothing -- for trimming.
    const int64_t window_lo = saturating_sub(
      _max_ts.value(), std::max<int64_t>(_window.count(), 0));
    _pass_lo = _pass_oldest == no_pass_oldest
                 ? window_lo
                 : std::max(window_lo, std::min(_pass_oldest, _max_ts.value()));
    _pass_span = std::max<int64_t>(
      saturating_sub(_max_ts.value(), _pass_lo), 0);
    _pass_width = _pass_span / static_cast<int64_t>(capacity_evict_buckets - 1)
                  + 1;
    _pass_oldest = no_pass_oldest;
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

void dedup_window_filter::sweep_for_new_identity() {
    reclaim_expired_elsewhere();
    sweep_step(sweep_budget);
}

void dedup_window_filter::reclaim_expired_elsewhere() {
    if (
      !_shard_budget || _shard_budget->filters.size() < 2
      || !over_shard_reclaim_limit()) {
        return;
    }
    auto& filters = _shard_budget->filters;
    // Two tries, so landing on this index still reaches another one.
    for (int attempt = 0; attempt < 2; ++attempt) {
        auto* other = filters[_shard_budget->reclaim_next++ % filters.size()];
        if (other == this || other->_budget_reserved > 0) {
            // Itself, or mid-restore: a restore's claim on the budget stands
            // whatever its index holds, and its cursor is not running.
            continue;
        }
        other->reclaim_expired_step(sweep_budget);
        return;
    }
}

void dedup_window_filter::reclaim_expired_step(size_t budget) {
    const int64_t cutoff = reclaim_cutoff();
    for (size_t step = 0; step < budget && !_index.empty(); ++step) {
        if (_reclaim_pos >= _index.size()) {
            _reclaim_pos = 0;
        }
        if (!is_live(_index.timestamp_at(_reclaim_pos), cutoff)) {
            // As in sweep_step(), the erase moved an unvisited entry here.
            erase_at(_reclaim_pos);
            ++_reclaimed_entries;
            continue;
        }
        ++_reclaim_pos;
    }
}

void dedup_window_filter::sweep_step(size_t budget) {
    const int64_t cutoff = expiry_cutoff();
    // Only the largest indices trim themselves under shard pressure. If every
    // inserting partition trimmed itself instead, a small busy one would keep
    // shrinking while an idle one that filled the budget kept it all. Whether
    // this is one of them is decided once per step; whether the shard is
    // still over its soft limit is checked per entry, as the own soft limit
    // is, so the step stops trimming once its own erases have brought the
    // shard back under it.
    const bool largest_here = _shard_budget && over_shard_soft_limit()
                              && among_the_largest();

    for (size_t step = 0; step < budget; ++step) {
        if (_index.empty()) {
            _sweep_pos = 0;
            return;
        }
        if (_sweep_pos >= _index.size()) {
            // The cursor reached the end, so the histogram it accumulated now
            // covers the index: turn it into the next cutoff, and wrap. An
            // erase elsewhere can also leave the cursor past the end, which
            // just ends the pass early on a partial histogram -- a soft target
            // can afford that, and it stays a function of the call sequence.
            _sweep_pos = 0;
            finish_pass();
        }
        ++_sweep_visits;
        const auto stamp = model::timestamp{_index.timestamp_at(_sweep_pos)};
        if (!is_live(stamp.value(), cutoff)) {
            // Expired: it can never match again, so dropping it costs no
            // coverage. The cursor does not advance -- the erase moved the
            // array's last, still unvisited, entry into this slot.
            erase_at(_sweep_pos);
            continue;
        }
        if (
          stamp.value() < _capacity_cutoff.value()
          && ((largest_here && over_shard_soft_limit()) || over_own_soft_limit())) {
            // Inside the window, so this is coverage genuinely lost. Doing it
            // here, a few entries at a time from the soft limit upwards, is
            // what replaced dropping a quarter of the index in one step once
            // it hit the ceiling.
            erase_at(_sweep_pos);
            ++_capacity_evicted_entries;
            continue;
        }
        count_in_pass(stamp);
        ++_sweep_pos;
    }
}

void dedup_window_filter::count_in_pass(model::timestamp stamp, size_t times) {
    // restore() can clamp _max_ts below an entry's own timestamp, and the
    // frame is a pass old, so an offset outside the window is reachable.
    const auto offset = static_cast<int64_t>(std::clamp<__int128>(
      static_cast<__int128>(stamp.value()) - _pass_lo, 0, _pass_span));
    _pass_counts[static_cast<size_t>(offset / _pass_width)] += times;
    _pass_visited += times;
    if (times > 0) {
        _pass_oldest = std::min(_pass_oldest, stamp.value());
    }
}

bool dedup_window_filter::make_room_for_one(model::timestamp incoming) {
    // One bounded extra step, never a scan. Freeing nothing leaves the caller
    // to admit the record unindexed, which is the trade the refused whole-map
    // sweep already made -- but at a cost that no longer depends on how large
    // the index is. The cursor advances either way, so successive attempts
    // walk the index and eventually complete a pass, recomputing the cutoff.
    ++_capacity_sweeps;
    sweep_step(capacity_emergency_budget);
    if (
      at_capacity() && _shard_budget && _index.size() < _max_entries
      && !_index.empty()) {
        replace_oldest_visited(incoming);
    }
    return !at_capacity();
}

void dedup_window_filter::replace_oldest_visited(model::timestamp incoming) {
    // The cutoff marks the oldest quarter as of the last completed pass, and
    // the cursor drops those entries as it reaches them. Under shard pressure
    // other partitions' inserts drive this cursor too -- log replay after a
    // restart drives it hardest -- so it can use them all up well before the
    // pass ends, and until it wraps nothing it walks is below the cutoff:
    // every new identity on the shard would be admitted unindexed while the
    // index keeps older ones. Replacing the oldest of the entries just ahead
    // of the cursor keeps the newer identity instead, at a bounded extra cost.
    // Ahead, not behind: an erase moves the map's last entry into the freed
    // slot, and behind the cursor that entry would go unvisited -- unexpired
    // and uncounted -- for the rest of the pass.
    //
    // Only for the shard budget. At its own ceiling an index still admits
    // unindexed what it cannot make room for (see
    // CapacityEvictionFailsOpenWhenNoSliceCanBeFreed), and only an entry
    // strictly older than the incoming one is replaced, so an index whose
    // entries all share the incoming timestamp keeps them.
    if (_sweep_pos >= _index.size()) {
        // The step ended with the cursor at the end, so there is nothing ahead
        // of it in this pass. Wrap as the next step would, and look at the
        // start of the next pass instead: on a small index that is every
        // entry, and an empty window would replace nothing.
        _sweep_pos = 0;
        finish_pass();
    }
    // As many as it takes for the shard to hold one more, not one: a shard
    // can stand above its limit, not only at it, and freeing one entry per
    // new identity would then leave every new identity unindexed for good.
    // Bounded by the window either way.
    size_t need = 1;
    if (_shard_budget && _shard_budget->entries >= _shard_budget->max_entries) {
        need = std::min(
          _shard_budget->entries - _shard_budget->max_entries + 1,
          capacity_emergency_budget);
    }
    for (; need > 0 && _sweep_pos < _index.size(); --need) {
        const auto begin = _sweep_pos;
        const auto end = std::min(
          begin + capacity_emergency_budget, _index.size());
        std::optional<size_t> oldest;
        for (size_t i = begin; i < end; ++i) {
            if (
              !oldest
              || _index.timestamp_at(i) < _index.timestamp_at(*oldest)) {
                oldest = i;
            }
        }
        if (!oldest || !(_index.timestamp_at(*oldest) < incoming.value())) {
            return;
        }
        erase_at(*oldest);
        ++_capacity_evicted_entries;
    }
}

bool dedup_window_filter::take_room_from_largest(model::timestamp incoming) {
    if (
      !_shard_budget || _index.size() >= _max_entries
      || _shard_budget->entries < _shard_budget->max_entries) {
        // Without a shard budget, at its own ceiling, or with the shard not
        // full, the room this index needs can only come from itself.
        return false;
    }
    if (among_the_largest()) {
        // The room has to come from this index itself, which is then the one
        // shard pressure trims.
        return false;
    }
    auto* largest = _shard_budget->largest();
    if (largest->_budget_reserved > 0) {
        // Mid-restore, its claim on the budget stands whatever it holds right
        // now, so nothing freed from it is room for anyone else.
        return false;
    }
    // The largest index's own cursor decides what goes, so it loses its
    // oldest entries as it would at its own ceiling -- once its next pass has
    // found them. That is a change to another partition's index driven by
    // this one's traffic, so replicas of the larger partition, which share a
    // shard with different neighbours, can diverge: the same best-effort
    // divergence the shard budget already accepts.
    largest->make_room_for_one(incoming);
    return !at_capacity();
}

dedup_index_snapshot dedup_window_filter::snapshot() const {
    dedup_index_snapshot result{.max_timestamp = _max_ts};
    result.entries.reserve(_index.size());
    for_each_live_entry(
      [&result](dedup_identity_digest identity, model::timestamp timestamp) {
          result.entries.push_back({identity, timestamp});
      });
    return result;
}

void dedup_window_filter::restore(const dedup_index_snapshot& snapshot) {
    // The walk below never yields, so the restore runs to completion before
    // the call returns and the future is ready to take.
    restore_from(
      snapshot.entries.size(),
      snapshot.max_timestamp,
      [&snapshot](auto&& emit) {
          for (const auto& entry : snapshot.entries) {
              emit(entry.identity, entry.timestamp);
          }
          return ss::now();
      })
      .get();
}

size_t dedup_window_filter::begin_restore(size_t count) {
    // Assigned rather than cleared, so a restore that keeps fewer entries than
    // the index held does not inherit its bucket array.
    invalidate_walks();
    _index = {};
    sync_shard_budget();
    stop_being_largest();
    // Bounded by the shard's headroom as well as by this partition's ceiling.
    // Restores run at startup, before anything else on the shard has settled,
    // and a restore that ignored the shard limit is exactly how a broker
    // could fail to start: every partition loading its full snapshot at once.
    // Partitions that restore later get less when the shard is full; that
    // narrows their coverage, which is recoverable, where an allocation
    // failure at startup is not.
    size_t allowed = _max_entries;
    if (_shard_budget) {
        const auto used = _shard_budget->entries;
        const auto headroom = used >= _shard_budget->max_entries
                                ? 0
                                : _shard_budget->max_entries - used;
        allowed = std::min(allowed, headroom);
    }
    const auto keep = std::min(count, allowed);
    // Claimed now, not as the entries go in: the restore yields between
    // chunks, and meanwhile the shard's other partitions replay their logs
    // and insert into whatever the budget still shows free. Counted only
    // afterwards, the restored entries landed on top of theirs, and the shard
    // sat above its limit -- where freeing one entry per new identity never
    // gets it back under, so every new identity on the shard was admitted
    // unindexed.
    _budget_reserved = keep;
    sync_shard_budget();
    _index.reserve(keep);
    return keep;
}

void dedup_window_filter::begin_restore_pass(
  model::timestamp max_timestamp, int64_t oldest) {
    // A snapshot written by a broker without the clamp can carry a poisoned
    // watermark; clamping here lets such a partition heal on restart rather
    // than needing a dedup_generation bump. Not counted as record skew.
    _max_ts = std::min(max_timestamp, broker_now());
    // The cursor indexes the value array it was walking, and the histogram
    // describes that array's contents; neither survives a different map, and
    // neither is persisted, which would put a wire field in the snapshot that
    // every replica would have to agree on. The cutoff is measured over the
    // restored entries instead of waiting for the cursor: until a pass
    // completes nothing is old enough to trim, so a restore that filled the
    // shard would leave every partition on it admitting new identities
    // unindexed for a whole pass -- hundreds of thousands of records on a
    // large index. The frame starts at the oldest restored entry.
    _sweep_pos = 0;
    _reclaim_pos = 0;
    _pass_oldest = oldest;
    begin_pass();
}

int64_t dedup_window_filter::finish_restore_pass() {
    finish_pass();
    return _capacity_cutoff.value();
}

void dedup_window_filter::finish_restore(size_t truncated) {
    _truncated_entries += truncated;
    // Settle the claim to what was actually restored: fewer, when the
    // snapshot held an identity twice.
    _budget_reserved = 0;
    sync_shard_budget();
    // A restored index ages from here, as if its newest entry had just
    // arrived: the shard's clock says nothing about how long the partition
    // was down.
    note_shard_time(_max_ts);
}

void dedup_window_filter::clear() {
    // Assigned rather than cleared, so the memory goes back: the callers that
    // reach here (dedup disabled, a generation change, a discarded or applied
    // raft snapshot) all want it back.
    invalidate_walks();
    _index = {};
    _budget_reserved = 0;
    sync_shard_budget();
    stop_being_largest();
    _max_ts = model::timestamp::min();
    _sweep_pos = 0;
    _reclaim_pos = 0;
    _shard_time_seen = std::numeric_limits<int64_t>::min();
    _capacity_cutoff = model::timestamp::min();
    _pass_oldest = no_pass_oldest;
    begin_pass();
}

} // namespace cluster
