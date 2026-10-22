// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/dedup_window_filter.h"

#include "base/vassert.h"
#include "hashing/xx.h"
#include "model/batch_compression.h"
#include "model/record.h"
#include "utils/vint.h"

#include <seastar/core/lowres_clock.hh>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstring>
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

void dedup_window_filter::set_max_entries(size_t max_entries) {
    _max_entries = std::min(max_entries, dedup_compact_index::max_size);
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

bool dedup_window_filter::counts_for_capacity() const {
    if (
      _index.size()
      >= soft_limit_of(_max_entries, capacity_count_margin_divisor)) {
        return true;
    }
    return _shard_budget
           && _shard_budget->entries >= soft_limit_of(
                _shard_budget->max_entries, capacity_count_margin_divisor);
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

/// Writes a buffer of a size known up front into fragments it allocates
/// itself, at most an iobuf fragment's largest each, with plain stores:
/// iobuf::append() keeps its bookkeeping for every call, and the rebuild
/// makes four of them per record.
class exact_iobuf_writer {
public:
    explicit exact_iobuf_writer(size_t size)
      : _size(size)
      , _left(size) {}

    void append(const char* src, size_t n) {
        while (n > 0) {
            if (_p == _e) {
                roll();
            }
            const size_t k = std::min(n, static_cast<size_t>(_e - _p));
            std::memcpy(_p, src, k);
            _p += k;
            src += k;
            n -= k;
        }
    }
    void append_varint(int64_t v) {
        if (_e - _p >= static_cast<ptrdiff_t>(vint::max_length)) {
            _p += vint::serialize(v, reinterpret_cast<uint8_t*>(_p));
            return;
        }
        std::array<uint8_t, vint::max_length> buf{};
        const auto n = vint::serialize(v, buf.data());
        append(reinterpret_cast<const char*>(buf.data()), n);
    }
    iobuf finish() && {
        flush();
        // Also what keeps the fragments free of slack: they were allocated
        // to add up to the size, so writing that many bytes filled them.
        vassert(
          _out.size_bytes() == _size,
          "dedup rebuild wrote {} bytes, sized for {}",
          _out.size_bytes(),
          _size);
        return std::move(_out);
    }

private:
    void roll() {
        flush();
        // The size given up front is what gets written, so no fragment is
        // left with room that would stay allocated behind the batch.
        const size_t n = std::max<size_t>(
          std::min<size_t>(_left, iobuf::max_linearize_size), 1);
        _buf = ss::temporary_buffer<char>(n);
        _p = _buf.get_write();
        _e = _p + n;
        _left -= std::min(_left, n);
    }
    void flush() {
        if (_buf.size() > 0) {
            _buf.trim(static_cast<size_t>(_p - _buf.get()));
            // As a fragment of its own: appending the buffer instead copies
            // one no larger than the last into a freshly allocated fragment.
            _out.append(std::make_unique<iobuf::fragment>(std::move(_buf)));
        }
        _p = _e = nullptr;
    }

    size_t _size;
    size_t _left;
    iobuf _out;
    ss::temporary_buffer<char> _buf;
    char* _p{nullptr};
    char* _e{nullptr};
};

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
  dedup_request_undo* undo,
  size_t* matched) {
    dassert(lookup_ready(), "dedup: lookup without a lookup table");
    note_shard_time(ts);
    const auto pos = _index.find(identity);
    if (pos != dedup_compact_index::npos) {
        const auto stored_ts = model::timestamp{_index.timestamp_at(pos)};
        if (
          stored_ts.value() != dedup_compact_index::expired
          && within_window(ts.value(), stored_ts.value(), _window.count())) {
            ++_dropped_records;
            if (matched) {
                *matched = pos;
            }
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
    note_stored(_index.size() - 1);
    note_admitted(ts.value());
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
    auto admitting = admit_batch();
    vassert(
      lookup_ready(),
      "dedup: filtering without a lookup table (deferred: {}, {} of {} "
      "indexed)",
      _lookup_deferred,
      _index.indexed(),
      _index.size());
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
    // has to survive the pass is 32 bytes per record, not the records
    // themselves, whose iobufs would otherwise pin a multiple of the batch
    // size for the duration of the call.
    //
    // Where each record lies is found again by the rebuild, which only a
    // request with something dropped pays for; kept here, it made each
    // record's entry 80 bytes written for every produce.
    struct classified {
        dedup_identity_digest identity;
        model::timestamp timestamp;
        // The record's length less its offset delta's bytes, the part of it
        // a rebuild keeps as it is. Saturated: a record this long, which only
        // decompression can produce, cannot be kept in a rebuilt batch.
        uint32_t unchanged{0};
        bool has_identity{false};
        bool dropped{false};
    };
    static_assert(sizeof(classified) == 32);
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
          const size_t unchanged
            = (span.end - span.body)
              - (span.after_offset_delta - span.offset_delta);
          records.push_back(
            {.identity = digest,
             .timestamp = clamp_to_broker_time(ts),
             .unchanged = static_cast<uint32_t>(std::min<size_t>(
               unchanged, std::numeric_limits<uint32_t>::max())),
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
    // Positions from here on hold only what this request inserts: an erase
    // moves the array's last entry into its hole, and while the array is
    // longer than this, its last entry is one of this request's. A record
    // found there repeats one earlier in the request.
    const size_t own_from = _index.size();
    const size_t n = records.size();
    const auto identity_at =
      [&records](size_t j) -> const dedup_identity_digest* {
        return records[j].has_identity ? &records[j].identity : nullptr;
    };
    for (size_t i = 0; i < n; ++i) {
        prefetch_ahead(i, n, identity_at);
        auto& c = records[i];
        size_t matched = 0;
        c.dropped = c.has_identity
                    && is_duplicate(c.identity, c.timestamp, &undo, &matched);
        if (c.dropped) {
            if (matched < own_from) {
                dropped.push_back(c.identity);
            }
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
        // A kept record's length counts its new offset delta's bytes in
        // place of the old one's.
        const auto length_of = [](const classified& c, int32_t delta) {
            return static_cast<int64_t>(c.unchanged)
                   + static_cast<int64_t>(vint::vint_size(delta));
        };
        size_t size = 0;
        {
            int32_t offset_delta = 0;
            for (const auto& c : records) {
                if (c.dropped) {
                    continue;
                }
                const auto length = length_of(c, offset_delta++);
                size += vint::vint_size(length) + static_cast<size_t>(length);
            }
        }
        // A batch's size is an int32, which also bounds any record whose
        // `unchanged` saturated.
        if (
          size > static_cast<size_t>(std::numeric_limits<int32_t>::max())
                   - model::packed_record_batch_header_size) [[unlikely]] {
            throw std::out_of_range(
              fmt::format(
                "dedup: rebuilt batch of {} bytes of records is larger than "
                "a batch can be",
                size));
        }
        exact_iobuf_writer data(size);
        // One reader finds each record's fields, the other copies the bytes
        // between them. The walk above already checked every length against
        // the buffer.
        dedup_record_reader scan(readable.data());
        dedup_record_reader source(readable.data());
        const auto copy_to = [&data, &source](size_t to) {
            source.consume(
              to - source.bytes_consumed(),
              [&data](const char* src, size_t n) { data.append(src, n); });
        };
        int64_t max_timestamp_delta = std::numeric_limits<int64_t>::min();
        int32_t offset_delta = 0;
        for (const auto& c : records) {
            const auto length = static_cast<size_t>(scan.read_varlong());
            const size_t body = scan.bytes_consumed();
            const size_t end = body + length;
            if (c.dropped) {
                scan.skip(length);
                continue;
            }
            scan.skip(1); // attributes
            const auto timestamp_delta = scan.read_varlong();
            const size_t old_delta = scan.bytes_consumed();
            scan.read_varlong();
            const size_t after_delta = scan.bytes_consumed();
            scan.skip(end - after_delta);

            data.append_varint(length_of(c, offset_delta));
            source.skip(body - source.bytes_consumed());
            copy_to(old_delta);
            data.append_varint(offset_delta++);
            source.skip(after_delta - old_delta);
            copy_to(end);
            max_timestamp_delta = std::max(
              max_timestamp_delta, timestamp_delta);
        }
        iobuf records_data = std::move(data).finish();

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
            // Saturating, as the walk's per-record timestamps are: the delta
            // is client-controlled.
            max_timestamp = model::timestamp{
              saturating_add(base_ts(), max_timestamp_delta)};
        }
        model::record_batch_header out{
          .header_crc = 0,
          .size_bytes = static_cast<int32_t>(
            model::packed_record_batch_header_size + records_data.size_bytes()),
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
          out, std::move(records_data), model::record_batch::tag_ctor_ng{});
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
    // Callers keep the lookup table while a request can still fail (see
    // dedup_stm::_unsettled): without it, what this request inserted cannot
    // be found to take back out.
    vassert(
      lookup_ready() || undo.entries.empty(),
      "dedup: reverting a request without a lookup table");
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
            note_stored(pos);
        } else {
            note_erase(pos);
            note_erase_behind_cursor(pos);
            _index.erase_at(pos);
        }
    }
    sync_shard_budget();
}

void dedup_window_filter::populate(
  const chunked_vector<dedup_index_entry>& entries) {
    auto batch = admit_batch();
    if (!lookup_ready()) {
        for (const auto& e : entries) {
            populate_unindexed(e.identity, e.timestamp);
        }
        return;
    }
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
    if (!lookup_ready()) {
        populate_unindexed(identity, ts);
        return;
    }
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
    note_stored(_index.size() - 1);
    note_admitted(ts.value());
    sync_shard_budget();
}

void dedup_window_filter::populate_unindexed(
  dedup_identity_digest identity, model::timestamp ts) {
    ts = clamp_to_broker_time(ts);
    _max_ts = std::max(_max_ts, ts);
    note_shard_time(ts);
    // As populate() does for a new identity. Without the lookup table there
    // is no telling whether it is in the index already; if it is, the newer
    // timestamp wins when build_lookup() finds the two.
    sweep_for_new_identity();
    if (
      (at_capacity() && !take_room_from_largest(ts) && !make_room_for_one(ts))
      || !_index.append_unindexed(identity, ts.value())) {
        ++_unindexed_records;
        return;
    }
    note_stored(_index.size() - 1);
    note_admitted(ts.value());
    sync_shard_budget();
}

void dedup_window_filter::defer_lookup() {
    if (_lookup_deferred) {
        return;
    }
    _lookup_deferred = true;
    _index.drop_lookup();
}

ss::future<> dedup_window_filter::build_lookup() {
    _lookup_deferred = false;
    if (lookup_ready()) {
        co_return;
    }
    if (_building) {
        co_await _building->get_shared_future();
        co_return;
    }
    _building.emplace();
    auto done = ss::defer([this]() noexcept {
        _building->set_value();
        _building.reset();
    });
    const auto epoch = _index_epoch;
    const auto interrupted = [this, epoch] {
        return _lookup_deferred || _index_epoch != epoch;
    };
    // A throw part way (an allocation) must not leave the table part built:
    // nothing would resume it, building() would hold off every trim, and the
    // next build expects nothing indexed.
    auto unwind = ss::defer([this, epoch]() noexcept {
        if (_index_epoch == epoch && !_index.fully_indexed()) {
            _index.drop_lookup();
        }
    });
    _index.begin_bulk_index();
    while (_index.prepare_step(build_prepare_chunk)) {
        co_await ss::coroutine::maybe_yield();
        if (interrupted()) {
            co_return;
        }
    }
    size_t steps = 0;
    while (!_index.fully_indexed()) {
        size_t indexed_as = 0;
        switch (_index.index_next(indexed_as)) {
        case dedup_compact_index::index_outcome::indexed:
            break;
        case dedup_compact_index::index_outcome::duplicate: {
            // Kept twice while unindexed: one entry, the newer timestamp.
            const auto pos = _index.indexed();
            if (_index.timestamp_at(pos) > _index.timestamp_at(indexed_as)) {
                _index.set_timestamp_at(indexed_as, _index.timestamp_at(pos));
            }
            erase_at(pos);
            break;
        }
        case dedup_compact_index::index_outcome::refused:
            erase_at(_index.indexed());
            ++_unindexed_records;
            break;
        }
        if ((++steps % build_index_chunk) == 0) {
            co_await ss::coroutine::maybe_yield();
            if (interrupted()) {
                co_return;
            }
        }
    }
}

void dedup_window_filter::erase_at(size_t index) {
    note_erase(index);
    note_erase_behind_cursor(index);
    _index.erase_at(index);
    sync_shard_budget();
}

void dedup_window_filter::note_erase_behind_cursor(size_t index) {
    // The array's last entry moves to \p index, where this pass's cursor has
    // been already: the pass will not see it.
    if (index < _sweep_pos && index + 1 != _index.size()) {
        _pass_saw_all = false;
    }
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
    _pass_uncounted = false;
    _pass_saw_all = true;
    _pass_rebase_expired = _index.expired_by_rebase();
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
    // A pass that counted only part of what it visited -- the sweep began
    // counting partway through it -- would take its quarter of that part
    // alone, and the entries it left uncounted below the cutoff could make
    // the cutoff mark most of the index. The next pass counts every entry.
    if (accumulated == 0 || _pass_uncounted) {
        _capacity_cutoff = model::timestamp::min();
    } else {
        _capacity_cutoff = model::timestamp{
          _pass_lo + static_cast<int64_t>(bucket) * _pass_width};
    }
    if (_pass_saw_all) {
        _oldest_bound = _pass_oldest;
        _rebase_expired_seen = _pass_rebase_expired;
    }
    begin_pass();
}

void dedup_window_filter::restart_pass() {
    // The pass the skipped steps interrupted is framed as of when it began:
    // for an index that filled while its sweep had nothing to do, that is the
    // empty index's frame, which puts every entry in one bucket. That pass
    // would end without a cutoff, and the index would go a whole pass more
    // without one just as it nears a limit, where the cutoff decides what it
    // gives up. The bound frames a fresh pass over the index as it is now.
    _sweep_skipped = false;
    _sweep_pos = 0;
    _pass_oldest = _oldest_bound;
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
    if (building()) {
        return;
    }
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
    if (building()) {
        return;
    }
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
    // Far from every limit the cutoff decides nothing, so the visit skips the
    // histogram. The oldest timestamp is still taken: it frames the next pass.
    const bool counting = counts_for_capacity();
    if (
      !counting && _oldest_bound >= cutoff
      && _index.expired_by_rebase() == _rebase_expired_seen) {
        // Nothing has expired, and nothing is near a limit to trim for.
        _sweep_skipped = true;
        return;
    }
    if (_sweep_skipped) {
        restart_pass();
    }

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
        if (counting) {
            count_in_pass(stamp);
        } else {
            _pass_oldest = std::min(_pass_oldest, stamp.value());
            _pass_uncounted = true;
        }
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
    const auto own_before = _index.size();
    const auto shard_before = _shard_budget ? _shard_budget->entries : 0;
    sweep_step(capacity_emergency_budget);
    const bool may_replace = _index.size() > _max_entries
                             || (_shard_budget && _index.size() < _max_entries);
    if (at_capacity() && !_index.empty() && may_replace) {
        replace_oldest_visited(incoming);
    }
    return !at_capacity() || shrank_over_limit(own_before, shard_before);
}

bool dedup_window_filter::shrank_over_limit(
  size_t own_before, size_t shard_before) const {
    const bool over
      = own_before > _max_entries
        || (_shard_budget && shard_before > _shard_budget->max_entries);
    return over && own_before >= _index.size() + 2;
}

void dedup_window_filter::replace_oldest_visited(model::timestamp incoming) {
    if (building()) {
        return;
    }
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
    // Only for the shard budget, or for an index above its own ceiling, which
    // only a ceiling lowered at runtime leaves it at. Exactly at its own
    // ceiling an index still admits unindexed what it cannot make room for
    // (see CapacityEvictionFailsOpenWhenNoSliceCanBeFreed), and only an entry
    // strictly older than the incoming one is replaced, so an index whose
    // entries all share the incoming timestamp keeps them.
    if (
      _sweep_pos > 0
      && _sweep_pos + capacity_emergency_budget > _index.size()) {
        // Fewer than a window's worth of entries are left ahead of the cursor,
        // and the array's tail is the newest arrivals: the oldest of a few of
        // them is still a recent identity, which a resend would then get
        // through. Wrap as the next step would, and look at the start of the
        // next pass instead -- on a small index that is every entry. The tail
        // goes unvisited this pass, so the pass bounds nothing.
        if (_sweep_pos < _index.size()) {
            _pass_saw_all = false;
        }
        _sweep_pos = 0;
        finish_pass();
    }
    // As many as it takes for the shard to hold one more, not one: a shard
    // can stand above its limit, not only at it, and freeing one entry per
    // new identity would then leave every new identity unindexed for good.
    // Bounded by the window either way.
    size_t need = 1;
    if (_index.size() > _max_entries) {
        need = _index.size() - _max_entries + 1;
    }
    if (_shard_budget && _shard_budget->entries >= _shard_budget->max_entries) {
        need = std::max(
          need, _shard_budget->entries - _shard_budget->max_entries + 1);
    }
    need = std::min(need, capacity_emergency_budget);
    // Above a limit, rather than at it: a lowered limit, or a restore that
    // claimed room before others filled it.
    const bool over_limit
      = _index.size() > _max_entries
        || (_shard_budget && _shard_budget->entries > _shard_budget->max_entries);
    const int64_t bound = std::min(incoming.value(), _batch_floor);
    for (; need > 0 && _sweep_pos < _index.size(); --need) {
        if (
          over_limit && _index.size() > capacity_emergency_budget
          && _sweep_pos + capacity_emergency_budget > _index.size()) {
            // Too few entries left ahead for a full window: wrap, as above.
            // The array's tail is where new identities were just appended, so
            // the oldest of a short window there is likely one of them, and
            // replacing many per insert would otherwise drop the newest
            // identities while older ones stay.
            if (_sweep_pos < _index.size()) {
                _pass_saw_all = false;
            }
            _sweep_pos = 0;
            finish_pass();
        }
        // The oldest entry of a window that is older than `below`.
        const auto oldest_below = [this](
                                    size_t from, size_t to, int64_t below) {
            std::optional<size_t> oldest;
            for (size_t i = from; i < to; ++i) {
                const auto stamp = _index.timestamp_at(i);
                if (
                  stamp < below
                  && (!oldest || stamp < _index.timestamp_at(*oldest))) {
                    oldest = i;
                }
            }
            return oldest;
        };
        const auto begin = _sweep_pos;
        const auto end = std::min(
          begin + capacity_emergency_budget, _index.size());
        // Entries older than anything this batch has admitted go first: a
        // batch's own identities are the room for its next one only when
        // nothing older is to be had. They are behind the cursor -- an erase
        // that moved one also moved the cursor past it -- or in the array's
        // tail, where the batch is appended, so a window without an older
        // entry is in that tail, and the rest of the array is searched
        // instead, a few windows at most. That search keeps its own place,
        // so successive ones cover the array rather than the same windows,
        // and the cursor and its pass are left as they are.
        auto oldest = oldest_below(begin, end, bound);
        for (size_t window = 0; !oldest && window < replace_search_windows;
             ++window) {
            if (_replace_search_pos >= _index.size()) {
                _replace_search_pos = 0;
            }
            const size_t from = _replace_search_pos;
            const size_t to = std::min(
              from + capacity_emergency_budget, _index.size());
            oldest = oldest_below(from, to, bound);
            if (!oldest) {
                _replace_search_pos = to;
            }
        }
        if (!oldest) {
            // A batch larger than all this index may hold beside what is
            // older: it gives up its own oldest, as any index at its ceiling
            // gives up its oldest.
            oldest = oldest_below(begin, end, incoming.value());
        }
        if (!oldest) {
            break;
        }
        erase_at(*oldest);
        ++_capacity_evicted_entries;
        if (*oldest >= _sweep_pos) {
            // The erase moved the array's last entry -- the newest, as a rule
            // an identity just admitted -- into the freed slot. Left ahead of
            // the cursor it would be a candidate for the next replacement:
            // visit up to and over the slot, so it is behind the cursor for
            // the rest of the pass.
            visit_through(*oldest);
        }
    }
    if (over_limit) {
        // Up to capacity_emergency_budget entries a call shrinks this index
        // far faster than largest() refreshes. Left cached as the largest, an
        // index trimming itself would go on counting as the largest until it
        // was empty, while a larger one kept everything.
        stop_being_largest();
    }
}

void dedup_window_filter::visit_through(size_t pos) {
    const int64_t cutoff = expiry_cutoff();
    const bool counting = counts_for_capacity();
    for (; _sweep_pos <= pos && _sweep_pos < _index.size(); ++_sweep_pos) {
        ++_sweep_visits;
        const auto stamp = model::timestamp{_index.timestamp_at(_sweep_pos)};
        if (!is_live(stamp.value(), cutoff)) {
            // Left for the next pass to drop: erasing here would move another
            // entry in ahead of the cursor.
            continue;
        }
        if (counting) {
            count_in_pass(stamp);
        } else {
            _pass_oldest = std::min(_pass_oldest, stamp.value());
            _pass_uncounted = true;
        }
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
    const auto shard_before = _shard_budget->entries;
    // The largest index's own cursor decides what goes, so it loses its
    // oldest entries as it would at its own ceiling -- once its next pass has
    // found them. That is a change to another partition's index driven by
    // this one's traffic, so replicas of the larger partition, which share a
    // shard with different neighbours, can diverge: the same best-effort
    // divergence the shard budget already accepts.
    largest->make_room_for_one(incoming);
    // Above the limit, as in make_room_for_one(): the insert may go ahead
    // once the shard has shrunk by two.
    return !at_capacity()
           || (shard_before > _shard_budget->max_entries
               && shard_before >= _shard_budget->entries + 2);
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
    ++_index_epoch;
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
    ++_index_epoch;
    _budget_reserved = 0;
    sync_shard_budget();
    stop_being_largest();
    _max_ts = model::timestamp::min();
    _sweep_pos = 0;
    _reclaim_pos = 0;
    _shard_time_seen = std::numeric_limits<int64_t>::min();
    _capacity_cutoff = model::timestamp::min();
    _pass_oldest = no_pass_oldest;
    _oldest_bound = std::numeric_limits<int64_t>::max();
    _rebase_expired_seen = 0;
    _sweep_skipped = false;
    begin_pass();
}

} // namespace cluster
