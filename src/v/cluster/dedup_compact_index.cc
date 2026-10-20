// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/dedup_compact_index.h"

#include "base/vassert.h"

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <stdexcept>
#include <string>

namespace cluster {

namespace {

// A lookup slot: 0 is empty; otherwise the probe distance in the top 8 bits
// and the entry's position + 1 in the low 24.
constexpr uint32_t slot_pos_bits = 24;
constexpr uint32_t slot_pos_mask = (uint32_t{1} << slot_pos_bits) - 1;
constexpr uint32_t slot_dist_one = uint32_t{1} << slot_pos_bits;

constexpr uint32_t make_slot(uint32_t pos, uint32_t dist) {
    return (dist << slot_pos_bits) | (pos + 1);
}
constexpr uint32_t slot_pos(uint32_t slot) {
    return (slot & slot_pos_mask) - 1;
}
constexpr uint32_t slot_dist(uint32_t slot) { return slot >> slot_pos_bits; }

static_assert(
  dedup_compact_index::max_size + 1 == slot_pos_mask,
  "every position + 1 must fit the slot's position field");
static_assert(dedup_compact_index::max_probe_distance == 255);

// Inserting the slot that would take a segment past this many keys grows or
// splits it first. Robin hood ordering keeps probe sequences short at this
// load, and the empty slots it leaves are what bound every probe loop.
constexpr uint32_t max_keys(uint32_t slots) { return slots / 8 * 7; }

constexpr uint32_t first_block_min_capacity = 16;

using i128 = __int128;

int64_t clamp_base(i128 base) {
    constexpr i128 lowest = std::numeric_limits<int64_t>::min();
    constexpr i128 highest = std::numeric_limits<int64_t>::max()
                             - dedup_compact_index::block_span;
    return static_cast<int64_t>(std::clamp(base, lowest, highest));
}

// A fixed, well mixed function of its argument (the murmur3 finalizer), for
// the per-segment and per-block choices below: deterministic, so replicas
// that apply the same operations still lay the table out identically.
constexpr uint64_t mix(uint64_t x) {
    x ^= x >> 33;
    x *= 0xff51'afd7'ed55'8ccdULL;
    x ^= x >> 33;
    x *= 0xc4ce'b9fe'1a85'ec53ULL;
    x ^= x >> 33;
    return x;
}

// Keys a segment holding key hash \p h may take before it grows or splits.
// A full-size segment splits somewhere between 5/8 and 7/8 full, at a point
// picked by where it sits in the directory. With a uniform hash every segment
// fills at the same rate, so a threshold they all shared had every one of them
// split within a few thousand inserts of the others: each split bounded, but
// together a whole-table rehash inside a single task. Spread over a quarter of
// a segment, the splits of one generation are spread over a fifth of the
// inserts that double the index, and the next generation drifts further apart.
uint32_t key_limit(uint32_t slots, uint32_t depth, uint64_t h) {
    constexpr uint32_t full = dedup_compact_index::max_segment_slots;
    if (slots < full) {
        return max_keys(slots);
    }
    constexpr uint32_t spread = full / 4;
    const uint64_t prefix = depth == 0 ? 0 : h >> (64 - depth);
    return max_keys(full)
           - static_cast<uint32_t>(
             mix(prefix ^ (uint64_t{depth} << 56)) % spread);
}

// Where a block's span starts below the timestamps it must hold: somewhere in
// [slack / 8, 3 * slack / 8) below \p lowest, picked by the block. Blocks
// filled together (a restore, a burst) would otherwise run out of room
// together, and every one of them re-base in the same few thousand writes.
int64_t jittered_base(i128 lowest, i128 slack, size_t block, int64_t salt) {
    const auto jitter = static_cast<i128>(
      mix(block ^ (static_cast<uint64_t>(salt) * 0x9e37'79b9'7f4a'7c15ULL))
      % static_cast<uint64_t>(slack / 4 + 1));
    return clamp_base(lowest - slack / 8 - jitter);
}

// Room for \p more elements, growing geometrically, but never past \p cap:
// reserve() of an exact size would reallocate on every call.
template<typename V>
void reserve_more(V& v, size_t more, size_t cap) {
    if (v.capacity() < v.size() + more) {
        v.reserve(std::max(v.size() + more, std::min(2 * v.capacity(), cap)));
    }
}

} // namespace

dedup_timestamp_selector::dedup_timestamp_selector(
  int64_t lowest, int64_t highest, size_t k)
  : _base(lowest)
  , _hi(static_cast<uint64_t>(highest) - static_cast<uint64_t>(lowest))
  , _k(k) {
    start_pass();
}

void dedup_timestamp_selector::start_pass() noexcept {
    _counts.fill(0);
    const auto bits = static_cast<uint32_t>(std::bit_width(_hi - _lo));
    _shift = bits > radix_bits ? bits - radix_bits : 0;
}

void dedup_timestamp_selector::finish_pass() noexcept {
    // The k-th value is in the first bucket whose running count passes k.
    // The bounds check is for a caller that broke the contract; a correct
    // one always finds it.
    size_t bucket = 0;
    while (bucket + 1 < _counts.size() && _k >= _counts[bucket]) {
        _k -= _counts[bucket];
        ++bucket;
    }
    for (size_t above = bucket + 1; above < _counts.size(); ++above) {
        _greater += _counts[above];
    }
    const uint64_t width = uint64_t{1} << _shift;
    _lo += static_cast<uint64_t>(bucket) << _shift;
    _hi = _lo + std::min(_hi - _lo, width - 1);
    start_pass();
}

size_t dedup_compact_index::find(
  const dedup_identity_digest& identity) const noexcept {
    if (!lookup_ready()) {
        return npos;
    }
    const auto key = dedup_canonical_digest(identity);
    const uint64_t h = key.lo;
    const auto& seg = segment_for(h);
    uint32_t i = static_cast<uint32_t>(h) & seg.mask;
    for (uint32_t d = 0; d <= max_probe_distance; ++d) {
        ++_work;
        const uint32_t s = seg.slots[i];
        if (s == 0 || slot_dist(s) < d) {
            return npos;
        }
        if (slot_dist(s) == d) {
            // Same home slot: the only candidates worth reading the entry
            // array for.
            const auto pos = slot_pos(s);
            const auto& e = entry(pos);
            if (
              e.lo_lo == static_cast<uint32_t>(h)
              && e.lo_hi == static_cast<uint32_t>(h >> 32)
              && e.hi == static_cast<uint16_t>(key.hi)) {
                return pos;
            }
        }
        i = (i + 1) & seg.mask;
    }
    return npos;
}

void dedup_compact_index::prefetch(
  const dedup_identity_digest& identity, prefetch_stage stage) const noexcept {
    if (!lookup_ready()) {
        return;
    }
    const uint64_t h = identity.lo;
    const segment* seg = _directory[directory_index(h)];
    if (stage == prefetch_stage::segment) {
        __builtin_prefetch(seg);
        return;
    }
    const uint32_t* slot = &seg->slots[static_cast<uint32_t>(h) & seg->mask];
    if (stage == prefetch_stage::slot) {
        __builtin_prefetch(slot);
        return;
    }
    // The home slot's entry: where find() reads first when the key is
    // present and was not displaced, which at the table's load is most keys.
    if (*slot != 0) {
        const size_t pos = slot_pos(*slot);
        if (pos < _size) {
            // At 14 bytes an entry can straddle two cache lines.
            const auto* e = reinterpret_cast<const char*>(&entry(pos));
            __builtin_prefetch(e);
            __builtin_prefetch(e + sizeof(packed_entry) - 1);
            __builtin_prefetch(&_meta[pos >> block_shift]);
        }
    }
}

size_t dedup_compact_index::block_size(size_t b) const noexcept {
    const size_t begin = b << block_shift;
    return _size <= begin ? 0 : std::min(block_entries, _size - begin);
}

uint32_t dedup_compact_index::encode(size_t b, int64_t ts) {
    if (ts == expired) {
        return 0;
    }
    auto& m = _meta[b];
    ++m.writes;
    i128 offset = i128{ts} - m.base;
    if (offset >= 0 && offset <= block_span) {
        m.newest = std::max(m.newest, ts);
        return static_cast<uint32_t>(offset + 1);
    }
    if (
      offset < 0 && m.newest != expired
      && i128{ts} < i128{m.newest} - retained_span
      && m.writes < block_entries) {
        // Too far behind the block's newest entry to keep beside it; known
        // without a pass over the block.
        return 0;
    }
    rebase(b, ts);
    offset = i128{ts} - m.base;
    if (offset >= 0 && offset <= block_span) {
        m.newest = std::max(m.newest, ts);
        return static_cast<uint32_t>(offset + 1);
    }
    return 0;
}

void dedup_compact_index::rebase(size_t b, int64_t ts) {
    ++_rebases;
    auto& m = _meta[b];
    auto* entries = _blocks[b].get();
    const size_t n = block_size(b);
    _work += 2 * n;
    i128 lo = ts;
    i128 hi = ts;
    for (size_t i = 0; i < n; ++i) {
        const auto t = decode(m.base, entries[i].rel);
        if (t != expired) {
            lo = std::min<i128>(lo, t);
            hi = std::max<i128>(hi, t);
        }
    }
    // Keep everything if it fits with room to spare, and otherwise the newest
    // retained_span. Of what is left of the block's span, 1/8 to 3/8 goes
    // below, for a write older than anything kept, and the rest above, since
    // timestamps mostly move forward (see jittered_base()).
    const i128 keep_from = std::max<i128>(lo, hi - retained_span);
    const i128 slack = block_span - (hi - keep_from);
    const int64_t base = jittered_base(keep_from, slack, b, m.base);
    for (size_t i = 0; i < n; ++i) {
        auto& e = entries[i];
        const auto t = decode(m.base, e.rel);
        const i128 offset = i128{t} - base;
        if (t != expired && (offset < 0 || offset > block_span)) {
            ++_expired_by_rebase;
            e.rel = 0;
        } else {
            e.rel = t == expired ? 0 : static_cast<uint32_t>(offset + 1);
        }
    }
    m.base = base;
    m.newest = static_cast<int64_t>(hi);
    m.writes = 0;
}

void dedup_compact_index::set_timestamp_at(size_t pos, int64_t ts) {
    auto& e = entry(pos);
    e.rel = 0;
    e.rel = encode(pos >> block_shift, ts);
}

dedup_compact_index::segment
dedup_compact_index::make_segment(uint32_t slots, uint32_t depth) const {
    _work += slots;
    segment seg;
    seg.slots = std::make_unique<uint32_t[]>(slots);
    seg.mask = slots - 1;
    seg.depth = depth;
    return seg;
}

bool dedup_compact_index::slot_insert(
  segment& seg, uint64_t h, uint32_t pos) const noexcept {
    // Precondition: seg.count < max_keys(slots), so an empty slot exists and
    // both loops below end.
    uint32_t i = static_cast<uint32_t>(h) & seg.mask;
    uint32_t d = 0;
    for (;;) {
        ++_work;
        const uint32_t s = seg.slots[i];
        if (s == 0) {
            seg.slots[i] = make_slot(pos, d);
            ++seg.count;
            ++seg.changes;
            return true;
        }
        if (slot_dist(s) < d) {
            break;
        }
        i = (i + 1) & seg.mask;
        if (++d > max_probe_distance) {
            return false;
        }
    }
    // Slot i belongs to a key closer to its home: the new key takes it, and
    // the run up to the next empty slot moves one slot further from home.
    uint32_t end = i;
    while (seg.slots[end] != 0) {
        _work += 2;
        if (slot_dist(seg.slots[end]) == max_probe_distance) {
            return false;
        }
        end = (end + 1) & seg.mask;
    }
    while (end != i) {
        const uint32_t prev = (end - 1) & seg.mask;
        seg.slots[end] = seg.slots[prev] + slot_dist_one;
        end = prev;
    }
    seg.slots[i] = make_slot(pos, d);
    ++seg.count;
    ++seg.changes;
    return true;
}

uint32_t dedup_compact_index::slot_of(
  const segment& seg, uint64_t h, uint32_t pos) const noexcept {
    uint32_t i = static_cast<uint32_t>(h) & seg.mask;
    for (uint32_t d = 0; d <= max_probe_distance; ++d) {
        ++_work;
        const uint32_t s = seg.slots[i];
        if (s != 0 && slot_pos(s) == pos) {
            return i;
        }
        i = (i + 1) & seg.mask;
    }
    vassert(false, "dedup_compact_index: an entry has no lookup slot");
    __builtin_unreachable();
}

void dedup_compact_index::slot_erase(segment& seg, uint32_t i) const noexcept {
    // Backward shift: the run after the hole moves one slot closer to home,
    // up to the first key already at home or an empty slot.
    uint32_t next = (i + 1) & seg.mask;
    while (seg.slots[next] != 0 && slot_dist(seg.slots[next]) > 0) {
        ++_work;
        seg.slots[i] = seg.slots[next] - slot_dist_one;
        i = next;
        next = (next + 1) & seg.mask;
    }
    seg.slots[i] = 0;
    --seg.count;
    ++seg.changes;
}

std::vector<std::pair<uint64_t, uint32_t>>
dedup_compact_index::keys_of(const segment& seg) const {
    std::vector<std::pair<uint64_t, uint32_t>> keys;
    keys.reserve(seg.count);
    _work += seg.mask + 1;
    // The keys' hashes are read back from the entry array, at random
    // positions; fetching a few slots ahead overlaps those misses.
    constexpr uint32_t prefetch_distance = 16;
    for (uint32_t i = 0; i <= seg.mask; ++i) {
        if (i + prefetch_distance <= seg.mask) {
            const uint32_t ahead = seg.slots[i + prefetch_distance];
            if (ahead != 0) {
                __builtin_prefetch(&entry(slot_pos(ahead)));
            }
        }
        const uint32_t s = seg.slots[i];
        if (s != 0) {
            const auto pos = slot_pos(s);
            keys.emplace_back(hash_of(entry(pos)), pos);
        }
    }
    return keys;
}

bool dedup_compact_index::grow_for(uint64_t h) {
    segment& seg = segment_for(h);
    const uint32_t slots = seg.mask + 1;
    // Bits a split below this segment can use: the directory indexes at most
    // max_directory_depth of them.
    const uint32_t first = 63 - seg.depth;
    const uint32_t last = 64 - max_directory_depth;
    const uint64_t reachable = first < last
                                 ? 0
                                 : ((uint64_t{2} << (first - last)) - 1)
                                     << last;
    if (
      slots == max_segment_slots && seg.agreement_at == seg.changes
      && ((seg.ones | h) & (seg.zeros | ~h) & reachable) == 0) {
        // Refused before, and this key agrees with the same keys.
        return false;
    }
    // Everything that can throw happens before the table changes.
    const auto keys = keys_of(seg);
    if (slots < max_segment_slots) {
        auto bigger = make_segment(slots * 2, seg.depth);
        for (const auto& [key, pos] : keys) {
            if (!slot_insert(bigger, key, pos)) {
                return false;
            }
        }
        seg = std::move(bigger);
        return true;
    }

    // Split on the first bit after the segment's prefix that tells its keys,
    // or the incoming one, apart. With a uniform hash that is the next bit;
    // keys sharing a longer prefix would otherwise take a split per shared
    // bit, each moving every key and leaving an empty half behind.
    uint64_t ones = 0;
    uint64_t zeros = 0;
    for (const auto& [key, pos] : keys) {
        ones |= key;
        zeros |= ~key;
    }
    seg.agreement_at = seg.changes;
    seg.ones = ones;
    seg.zeros = zeros;
    const uint64_t differing = (ones | h) & (zeros | ~h) & reachable;
    if (differing == 0) {
        // No split the directory can make separates them.
        return false;
    }
    const auto bit = static_cast<uint32_t>(63 - std::countl_zero(differing));
    const uint32_t depth = 64 - bit;
    if (
      depth > _depth
      && (size_t{1} << depth) > std::max(
           min_directory_slots, _size / entries_per_directory_slot)) {
        // A directory this deep needs more entries behind it first: only
        // keys sharing a prefix longer than the index's size accounts for get
        // here, and they are refused rather than let the directory outgrow
        // the entries it indexes.
        return false;
    }
    const uint64_t split = uint64_t{1} << bit;
    size_t high_keys = 0;
    for (const auto& [key, pos] : keys) {
        high_keys += (key & split) != 0 ? 1 : 0;
    }
    // Each half sized for what it gets, with room to grow before splitting
    // again, so a lopsided split does not leave a large empty segment.
    auto slots_for = [](size_t n) {
        return std::clamp(
          std::bit_ceil(static_cast<uint32_t>(2 * n)),
          min_segment_slots,
          max_segment_slots);
    };
    auto low = make_segment(slots_for(keys.size() - high_keys), depth);
    auto high = std::make_unique<segment>(
      make_segment(slots_for(high_keys), depth));
    for (const auto& [key, pos] : keys) {
        if (!slot_insert((key & split) != 0 ? *high : low, key, pos)) {
            return false;
        }
    }
    // Each level the keys agree on hands the side they are not on to an
    // empty segment.
    std::vector<std::unique_ptr<segment>> empty;
    empty.reserve(depth - 1 - seg.depth);
    for (uint32_t level = seg.depth + 1; level < depth; ++level) {
        empty.push_back(
          std::make_unique<segment>(make_segment(min_segment_slots, level)));
    }
    reserve_more(_segments, empty.size() + 1, size_t{1} << max_directory_depth);
    if (depth > _depth) {
        // More bits of the hash pick the directory entry, so every entry
        // becomes a run of adjacent ones naming the same segment.
        const size_t factor = size_t{1} << (depth - _depth);
        _work += _directory.size() * factor;
        std::vector<segment*> deeper(_directory.size() * factor);
        for (size_t i = 0; i < _directory.size(); ++i) {
            std::fill_n(deeper.begin() + i * factor, factor, _directory[i]);
        }
        _directory = std::move(deeper);
        _depth = depth;
    }
    // The entries naming a segment of local depth L are an aligned run of
    // 2^(_depth - L); the half of it that bit 63 - L sets is the upper half.
    const size_t at = directory_index(h);
    for (uint32_t level = seg.depth; level + 1 < depth; ++level) {
        const size_t span = size_t{1} << (_depth - level);
        const size_t start = at & ~(span - 1);
        const bool upper = ((h >> (63 - level)) & 1) != 0;
        const size_t other = upper ? start : start + span / 2;
        std::fill_n(
          _directory.begin() + other, span / 2, empty[level - seg.depth].get());
    }
    {
        const size_t span = size_t{2} << (_depth - depth);
        const size_t start = at & ~(span - 1);
        std::fill_n(
          _directory.begin() + start + span / 2, span / 2, high.get());
    }
    seg = std::move(low);
    for (auto& e : empty) {
        _segments.push_back(std::move(e));
    }
    _segments.push_back(std::move(high));
    ++_segment_splits;
    return true;
}

bool dedup_compact_index::push_back(
  const dedup_identity_digest& identity, int64_t ts) {
    vassert(
      fully_indexed(),
      "dedup_compact_index: push_back with {} of {} entries indexed",
      _indexed,
      _size);
    if (_size >= max_size) {
        ++_refused_inserts;
        return false;
    }
    // Room for the entry first: everything that can throw happens before
    // the lookup table names a position that does not exist.
    make_room_for_entry();
    const auto key = dedup_canonical_digest(identity);
    if (!slot_index(key.lo, static_cast<uint32_t>(_size))) {
        ++_refused_inserts;
        return false;
    }
    write_entry(key, ts);
    ++_indexed;
    return true;
}

bool dedup_compact_index::append_unindexed(
  const dedup_identity_digest& identity, int64_t ts) {
    if (_size >= max_size) {
        ++_refused_inserts;
        return false;
    }
    make_room_for_entry();
    write_entry(dedup_canonical_digest(identity), ts);
    return true;
}

void dedup_compact_index::make_room_for_entry() {
    const size_t b = _size >> block_shift;
    if (b == 0 && _size == _first_block_capacity) {
        const size_t capacity = std::min(
          block_entries, std::max<size_t>(first_block_min_capacity, _size * 2));
        auto grown = std::make_unique_for_overwrite<packed_entry[]>(capacity);
        _work += _size;
        if (_size > 0) {
            std::copy_n(_blocks[0].get(), _size, grown.get());
        }
        if (_blocks.empty()) {
            _meta.reserve(1);
            _blocks.push_back(std::move(grown));
            _meta.emplace_back();
        } else {
            _blocks[0] = std::move(grown);
        }
        _first_block_capacity = capacity;
    } else if (b == _blocks.size()) {
        reserve_more(_meta, 1, (max_size >> block_shift) + 1);
        _blocks.push_back(
          std::make_unique_for_overwrite<packed_entry[]>(block_entries));
        _meta.emplace_back();
    }
}

void dedup_compact_index::write_entry(
  const dedup_identity_digest& key, int64_t ts) {
    const size_t b = _size >> block_shift;
    if ((_size & (block_entries - 1)) == 0) {
        // The block's first entry: centre its span on this timestamp, as a
        // re-base of a block holding only it would.
        _meta[b] = block_meta{
          .base = ts == expired ? 0 : jittered_base(ts, block_span, b, 0),
          .newest = expired,
          .writes = 0};
    }
    auto& e = entry(_size);
    e.lo_lo = static_cast<uint32_t>(key.lo);
    e.lo_hi = static_cast<uint32_t>(key.lo >> 32);
    e.hi = static_cast<uint16_t>(key.hi);
    e.rel = 0;
    ++_size;
    e.rel = encode(b, ts);
}

bool dedup_compact_index::slot_index(uint64_t h, uint32_t pos) {
    if (_directory.empty()) {
        _segments.reserve(1);
        _directory.reserve(1);
        _segments.push_back(
          std::make_unique<segment>(make_segment(min_segment_slots, 0)));
        _directory.push_back(_segments.back().get());
        _depth = 0;
    }
    // Each round either inserts or grows the segment: doubling it, or
    // splitting it one directory level deeper. Both are capped, so this ends.
    for (;;) {
        segment& seg = segment_for(h);
        if (
          seg.count < key_limit(seg.mask + 1, seg.depth, h)
          && slot_insert(seg, h, pos)) {
            return true;
        }
        if (!grow_for(h)) {
            return false;
        }
    }
}

void dedup_compact_index::drop_lookup() noexcept {
    _segments.clear();
    _directory.clear();
    _depth = 0;
    _indexed = 0;
    _preparing = false;
    _prepared = 0;
}

void dedup_compact_index::begin_bulk_index() {
    vassert(
      _indexed == 0,
      "dedup_compact_index: bulk indexing with {} entries indexed",
      _indexed);
    drop_lookup();
    if (_size == 0) {
        return;
    }
    // Keys per segment on average, below the fewest a full segment takes
    // before it splits (key_limit()) by enough that a segment drawing a few
    // standard deviations more than its share still fits.
    constexpr size_t bulk_keys_per_segment = 2400;
    uint32_t depth = 0;
    while (depth < max_directory_depth
           && (_size >> depth) > bulk_keys_per_segment) {
        ++depth;
    }
    const size_t segments = size_t{1} << depth;
    // Keys spread over the segments about as a Poisson draw does: room for
    // four standard deviations over the average keeps the segments that get
    // more from doubling, without sizing every segment for twice its share.
    const size_t average = (_size + segments - 1) / segments;
    const auto deviation = static_cast<size_t>(
      std::sqrt(static_cast<double>(average)));
    const size_t keys = average + 4 * deviation + 8;
    _bulk_slots = std::clamp(
      std::bit_ceil(static_cast<uint32_t>(keys / 7 * 8 + 8)),
      min_segment_slots,
      max_segment_slots);
    _directory.assign(segments, nullptr);
    _segments.reserve(segments);
    _depth = depth;
    _preparing = true;
    _prepared = 0;
}

bool dedup_compact_index::prepare_step(size_t n) {
    while (_preparing && n-- > 0) {
        _segments.push_back(
          std::make_unique<segment>(make_segment(_bulk_slots, _depth)));
        _directory[_prepared] = _segments.back().get();
        _preparing = ++_prepared < _directory.size();
    }
    return _preparing;
}

dedup_compact_index::index_outcome
dedup_compact_index::index_next(size_t& duplicate_of) {
    vassert(
      _indexed < _size && !_preparing,
      "dedup_compact_index: index_next at {} of {}, preparing: {}",
      _indexed,
      _size,
      _preparing);
    // The entries ahead are read in order, but each probes a segment of its
    // own. Start loading the slot a later one probes first, and closer in,
    // the entry that slot names, which the duplicate check reads.
    constexpr size_t slot_ahead = 16;
    constexpr size_t entry_ahead = 8;
    if (_indexed + slot_ahead < _size && lookup_ready()) {
        const uint64_t h = hash_of(entry(_indexed + slot_ahead));
        const segment* seg = _directory[directory_index(h)];
        __builtin_prefetch(&seg->slots[static_cast<uint32_t>(h) & seg->mask]);
    }
    if (_indexed + entry_ahead < _size && lookup_ready()) {
        const uint64_t h = hash_of(entry(_indexed + entry_ahead));
        const segment* seg = _directory[directory_index(h)];
        const uint32_t home = seg->slots[static_cast<uint32_t>(h) & seg->mask];
        if (home != 0 && slot_pos(home) < _indexed) {
            __builtin_prefetch(&entry(slot_pos(home)));
        }
    }
    const auto pos = _indexed;
    const auto identity = identity_at(pos);
    if (const auto found = find(identity); found != npos) {
        duplicate_of = found;
        return index_outcome::duplicate;
    }
    if (!slot_index(identity.lo, static_cast<uint32_t>(pos))) {
        ++_refused_inserts;
        return index_outcome::refused;
    }
    ++_indexed;
    return index_outcome::indexed;
}

void dedup_compact_index::erase_at(size_t pos) {
    const size_t last = _size - 1;
    // Erasing an indexed entry while some are not would move an unindexed
    // one into the indexed part.
    vassert(
      pos >= _indexed || fully_indexed(),
      "dedup_compact_index: erase of indexed {} with {} of {} indexed",
      pos,
      _indexed,
      _size);
    if (pos < _indexed) {
        const uint64_t h = hash_of(entry(pos));
        auto& seg = segment_for(h);
        slot_erase(seg, slot_of(seg, h, static_cast<uint32_t>(pos)));
    }
    if (pos != last) {
        const auto moved = entry(last);
        const auto ts = timestamp_at(last);
        if (last < _indexed) {
            const uint64_t h = hash_of(moved);
            auto& seg = segment_for(h);
            const auto slot = slot_of(seg, h, static_cast<uint32_t>(last));
            seg.slots[slot] = (seg.slots[slot] & ~slot_pos_mask)
                              | (static_cast<uint32_t>(pos) + 1);
        }
        auto& e = entry(pos);
        e = moved;
        e.rel = 0;
        e.rel = encode(pos >> block_shift, ts);
    }
    if (fully_indexed()) {
        --_indexed;
    }
    --_size;
    // Keep at most one empty block past the end, so popping and pushing
    // across a block boundary does not allocate every time.
    const size_t used = (_size + block_entries - 1) >> block_shift;
    while (_blocks.size() > std::max<size_t>(used + 1, 1)) {
        _blocks.pop_back();
        _meta.pop_back();
    }
}

void dedup_compact_index::reserve(size_t n) {
    n = std::min(n, max_size);
    const size_t blocks = (n + block_entries - 1) >> block_shift;
    _blocks.reserve(blocks);
    _meta.reserve(blocks);
    if (n > _first_block_capacity && _first_block_capacity < block_entries) {
        const size_t capacity = std::min(
          block_entries, std::max<size_t>(first_block_min_capacity, n));
        auto grown = std::make_unique_for_overwrite<packed_entry[]>(capacity);
        if (_size > 0) {
            std::copy_n(
              _blocks[0].get(), std::min(_size, block_entries), grown.get());
        }
        if (_blocks.empty()) {
            _blocks.push_back(std::move(grown));
            _meta.emplace_back();
        } else {
            _blocks[0] = std::move(grown);
        }
        _first_block_capacity = capacity;
    }
}

size_t dedup_compact_index::memory_usage() const noexcept {
    size_t bytes = 0;
    if (!_blocks.empty()) {
        bytes += _first_block_capacity * sizeof(packed_entry);
        bytes += (_blocks.size() - 1) * block_entries * sizeof(packed_entry);
    }
    bytes += _blocks.capacity() * sizeof(_blocks[0]);
    bytes += _meta.capacity() * sizeof(block_meta);
    for (const auto& seg : _segments) {
        bytes += sizeof(segment) + (size_t{seg->mask} + 1) * sizeof(uint32_t);
    }
    bytes += _segments.capacity() * sizeof(_segments[0]);
    bytes += _directory.capacity() * sizeof(_directory[0]);
    return bytes;
}

dedup_compact_index::table_stats dedup_compact_index::stats() const noexcept {
    table_stats s{
      .segments = _segments.size(),
      .directory_depth = _depth,
      .rebases = _rebases,
      .refused_inserts = _refused_inserts,
      .segment_splits = _segment_splits,
      .work = _work,
    };
    for (const auto& seg : _segments) {
        s.slots += size_t{seg->mask} + 1;
        for (uint32_t i = 0; i <= seg->mask; ++i) {
            if (seg->slots[i] != 0) {
                s.longest_probe = std::max(
                  s.longest_probe, slot_dist(seg->slots[i]));
            }
        }
    }
    return s;
}

void dedup_compact_index::check_invariants() const {
    auto fail = [](const std::string& what) {
        throw std::logic_error("dedup_compact_index: " + what);
    };
    if (_preparing) {
        fail("the lookup table is still being allocated");
    }
    if (_blocks.size() != _meta.size()) {
        fail("block and block metadata counts differ");
    }
    const size_t used = (_size + block_entries - 1) >> block_shift;
    if (
      _blocks.size() < used || _blocks.size() > std::max<size_t>(used + 1, 1)) {
        fail("block count does not match the size");
    }
    if (
      !_blocks.empty() && _size > 0
      && std::min(_size, block_entries) > _first_block_capacity) {
        fail("the first block is smaller than its entries");
    }
    // Directory: each segment is named by one aligned run of entries.
    if (!_directory.empty()) {
        if (_directory.size() != (size_t{1} << _depth)) {
            fail("directory size does not match its depth");
        }
        size_t i = 0;
        while (i < _directory.size()) {
            const auto* seg = _directory[i];
            if (seg->depth > _depth) {
                fail("segment deeper than the directory");
            }
            const size_t span = size_t{1} << (_depth - seg->depth);
            if (i % span != 0) {
                fail("segment run is not aligned");
            }
            for (size_t j = i; j < i + span; ++j) {
                if (_directory[j] != seg) {
                    fail("segment run is broken");
                }
            }
            i += span;
        }
    }
    if (_indexed > _size) {
        fail("more entries indexed than there are");
    }
    std::vector<bool> seen(_size, false);
    size_t keys = 0;
    for (const auto& seg : _segments) {
        if (seg->count >= seg->mask + 1) {
            fail("segment has no empty slot");
        }
        uint32_t counted = 0;
        for (uint32_t i = 0; i <= seg->mask; ++i) {
            const uint32_t s = seg->slots[i];
            if (s == 0) {
                continue;
            }
            ++counted;
            const auto pos = slot_pos(s);
            if (pos >= _indexed) {
                fail("slot names a position that is not indexed");
            }
            if (seen[pos]) {
                fail("two slots name one position");
            }
            seen[pos] = true;
            const uint64_t h = hash_of(entry(pos));
            if (&segment_for(h) != seg.get()) {
                fail("key is in the wrong segment");
            }
            const uint32_t home = static_cast<uint32_t>(h) & seg->mask;
            if (((i - home) & seg->mask) != slot_dist(s)) {
                fail("slot records the wrong probe distance");
            }
            const uint32_t prev = seg->slots[(i - 1) & seg->mask];
            if (slot_dist(s) > 0 && prev == 0) {
                fail("displaced key after an empty slot");
            }
            if (prev != 0 && slot_dist(s) > slot_dist(prev) + 1) {
                fail("robin hood order broken");
            }
        }
        if (counted != seg->count) {
            fail("segment count is wrong");
        }
        keys += counted;
    }
    if (keys != _indexed) {
        fail(
          "lookup table holds " + std::to_string(keys) + " keys for "
          + std::to_string(_indexed) + " indexed entries");
    }
    for (size_t pos = 0; pos < _size; ++pos) {
        if (pos < _indexed && find(identity_at(pos)) != pos) {
            fail("an entry is not found at its position");
        }
        const auto& m = _meta[pos >> block_shift];
        const auto t = timestamp_at(pos);
        if (t != expired && t > m.newest) {
            fail("an entry is newer than its block's newest");
        }
    }
}

} // namespace cluster
