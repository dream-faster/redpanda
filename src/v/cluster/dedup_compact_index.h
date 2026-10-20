// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

namespace cluster {

/// \brief A fixed-width, process-stable digest of a dedup identity.
///
/// The index stores this instead of the identity bytes, so an entry costs the
/// same regardless of how large the Kafka key or header value is. One
/// XXH3-128 pass is computed, and 80 bits are kept: all of `lo` (its low 64
/// bits) and the low 16 bits of `hi`, whose other bits are always zero (see
/// dedup_compact_index, which stores exactly those 10 bytes). At the
/// supported 10^7-entry upper limit, the chance that a new identity matches
/// some indexed one is on the order of 10^-17 per lookup, far below the
/// window-edge and generation-change boundaries the feature already documents
/// as best-effort.
///
/// XXH3 is not collision-resistant against a chosen-input attacker, which
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

/// The bits of dedup_identity_digest::hi that are part of the identity.
inline constexpr uint64_t dedup_digest_hi_mask = 0xffffULL;

/// The digest as the index keeps it. Digests computed by this build are
/// already in this form; one read from a snapshot written before the index
/// kept 80 bits is not.
constexpr dedup_identity_digest
dedup_canonical_digest(dedup_identity_digest d) noexcept {
    return {.hi = d.hi & dedup_digest_hi_mask, .lo = d.lo};
}

/// The digest is already avalanched by XXH3, so the map takes one half
/// verbatim rather than paying to re-mix it.
struct dedup_identity_digest_hash {
    using is_avalanching = void;

    uint64_t operator()(const dedup_identity_digest& d) const noexcept {
        return d.lo;
    }
};

/// \brief The k-th smallest of a multiset of timestamps, found in passes that
/// each look at every value once, in any order.
///
/// A radix select: each pass counts the values in the range still in question
/// into 4096 buckets and narrows the range to the bucket holding the k-th, so
/// at most six passes settle any int64 range, and fewer settle the narrow ones
/// timestamps usually span. The caller drives the passes and can yield
/// between any two values, which a std::nth_element over millions of values
/// -- tens of milliseconds -- cannot.
class dedup_timestamp_selector {
public:
    /// Select the \p k-th smallest (from 0) of values all within [\p lowest,
    /// \p highest]. \p k must be less than the number of values.
    dedup_timestamp_selector(int64_t lowest, int64_t highest, size_t k);

    /// Whether value() is known; until then, pass every value to add() and
    /// call finish_pass().
    bool done() const noexcept { return _lo == _hi; }
    void add(int64_t v) noexcept {
        const uint64_t offset = static_cast<uint64_t>(v)
                                - static_cast<uint64_t>(_base);
        if (offset >= _lo && offset <= _hi) {
            ++_counts[(offset - _lo) >> _shift];
        }
    }
    void finish_pass() noexcept;
    int64_t value() const noexcept {
        return static_cast<int64_t>(static_cast<uint64_t>(_base) + _lo);
    }
    /// Once done(): how many of the values are greater than value().
    size_t greater() const noexcept { return _greater; }

private:
    static constexpr uint32_t radix_bits = 12;
    void start_pass() noexcept;

    int64_t _base;
    // The range in question, as offsets from _base.
    uint64_t _lo{0};
    uint64_t _hi;
    // Rank of the value sought within that range.
    size_t _k;
    // Values above the range.
    size_t _greater{0};
    uint32_t _shift{0};
    std::array<size_t, size_t{1} << radix_bits> _counts{};
};

/// \brief The dedup index's table: identity digest to record timestamp, at 14
/// bytes per entry plus a 4-byte lookup slot.
///
/// The general-purpose map this replaced kept a 24-byte value and an 8-byte
/// bucket at up to 80% load: about 50 bytes an entry with allocation overhead.
/// Here an entry is a 10-byte digest and a 4-byte timestamp, and the lookup
/// table holds only the entry's position: 20-23 bytes an entry, so the same
/// memory holds more than twice the identities.
///
/// Entries live in a dense array in insertion order, and erasing one moves the
/// array's last entry into its place. That is the layout the filter's sweep
/// cursor and snapshot walks are built on, and replicas that apply the same
/// operations lay it out identically.
///
/// ## Timestamps
///
/// An entry stores its timestamp relative to the base of its block of 4096
/// positions: 32 bits, of which 0 means expired and the rest cover a span of
/// 2^32 - 2 milliseconds, about 49.7 days. A write that does not fit its block
/// re-bases the block: one bounded pass over its 4096 entries, which leaves
/// between 1/8 and 3/8 of the block's spare span below the oldest entry and
/// the rest above the newest -- at least 2^27 ms (1.5 days) below and 5 * 2^27
/// ms (7.8 days) above when the spare span is at its smallest, 2^30 ms -- so
/// it happens at most once per day or so of timestamp movement per block. When
/// the block's entries cannot all be kept together with room above them, those
/// more than retained_span behind the block's newest are stored as expired.
///
/// Expired is the only lossy outcome, and it can only make an entry stop
/// matching -- a duplicate admitted, never a record dropped. With a dedup
/// window up to retained_span (about 37 days), every entry it turns expired
/// was outside the window already. Timestamps are otherwise exact.
///
/// ## Lookup
///
/// Positions are found through a hash table of 4-byte slots (a 24-bit
/// position and an 8-bit probe distance) split into segments of at most 4096
/// slots, robin hood ordered within a segment. A directory indexed by the
/// digest's top bits names each key's segment (extendible hashing), so a
/// segment that fills up splits on its own: growth never rehashes the whole
/// table, which at millions of entries would hold the reactor for tens of
/// milliseconds. Every operation does work bounded by a segment or a block,
/// whatever the size of the index -- and so does every task's worth of
/// operations: a full-size segment splits at a fill between 5/8 and 7/8 that
/// depends on its place in the directory, and a block's base is placed
/// within its slack by its position, so segments filling at the same rate,
/// or blocks filled at the same time, do not all split or re-base in the
/// same few thousand calls.
///
/// A split goes straight to the first bit that tells the segment's keys apart,
/// so keys sharing a long prefix cost one split, not one per shared bit. A
/// segment that cannot take a key -- a probe sequence too long to record, or
/// keys no split within the directory's maximum depth separates, neither of
/// which a uniform hash reaches -- refuses the insert, and the caller admits
/// the record without indexing it, as it does at the entry limit. A segment
/// remembers which bits its keys agree on, so refusing the next key that
/// agrees with them too costs nothing more.
class dedup_compact_index {
public:
    /// What timestamp_at() returns for an entry stored as expired. Older than
    /// any real timestamp, so every age comparison treats it as outside the
    /// window, and writing it stores an expired entry.
    static constexpr int64_t expired = std::numeric_limits<int64_t>::min();
    static constexpr size_t npos = std::numeric_limits<size_t>::max();
    /// Positions are 24 bits in a lookup slot, and one value means empty.
    static constexpr size_t max_size = (size_t{1} << 24) - 2;

    /// Entries per timestamp block.
    static constexpr size_t block_shift = 12;
    static constexpr size_t block_entries = size_t{1} << block_shift;
    /// Milliseconds one block represents exactly: relative values 1..2^32-1.
    static constexpr int64_t block_span = (int64_t{1} << 32) - 2;
    /// Room a re-base leaves in the direction it ran out of.
    static constexpr int64_t rebase_headroom = int64_t{1} << 30;
    /// How far behind its block's newest entry an entry is still kept exactly
    /// when the block cannot keep them all.
    static constexpr int64_t retained_span = block_span - rebase_headroom;

    /// Largest segment, in slots: 16 KiB.
    static constexpr uint32_t max_segment_slots = 4096;
    /// Slots a segment starts with, so a small index stays small.
    static constexpr uint32_t min_segment_slots = 8;
    /// Directory depth cap: 2^13 segments, room for max_size at about half
    /// load, and a directory of at most 64 KiB.
    static constexpr uint32_t max_directory_depth = 13;
    /// Entries each directory slot past the first min_directory_slots needs:
    /// the directory deepens only as the index grows, so keys that share a
    /// long prefix cannot make it large while the index is small.
    static constexpr size_t entries_per_directory_slot = 64;
    static constexpr size_t min_directory_slots = 512;
    /// Longest probe distance a slot can record.
    static constexpr uint32_t max_probe_distance = 255;

    dedup_compact_index() = default;
    dedup_compact_index(const dedup_compact_index&) = delete;
    dedup_compact_index& operator=(const dedup_compact_index&) = delete;
    dedup_compact_index(dedup_compact_index&&) noexcept = default;
    dedup_compact_index& operator=(dedup_compact_index&&) noexcept = default;
    ~dedup_compact_index() = default;

    size_t size() const noexcept { return _size; }
    bool empty() const noexcept { return _size == 0; }

    /// Position of \p identity, or npos.
    size_t find(const dedup_identity_digest& identity) const noexcept;

    /// Stages of a lookup's memory accesses, each one dependent on the one
    /// before: the segment the directory names, the slot the key hashes to in
    /// it, and the entry that slot holds.
    enum class prefetch_stage { segment, slot, entry };

    /// Start loading the memory find() reads for an identity at a stage, and
    /// return without waiting for it. In an index too large for the cache
    /// every stage of a lookup is a miss; issued for keys some lookups
    /// ahead, a stage at a time, those misses overlap the work in between.
    /// A hint only: it changes nothing, and is safe for any key at any time.
    void prefetch(const dedup_identity_digest&, prefetch_stage) const noexcept;

    /// The identity at \p pos, in dedup_canonical_digest() form.
    dedup_identity_digest identity_at(size_t pos) const noexcept {
        const auto& e = entry(pos);
        return {
          .hi = e.hi, .lo = (uint64_t{e.lo_hi} << 32) | uint64_t{e.lo_lo}};
    }

    /// The timestamp at \p pos, or `expired`.
    int64_t timestamp_at(size_t pos) const noexcept {
        return decode(_meta[pos >> block_shift].base, entry(pos).rel);
    }

    /// Overwrite the timestamp at \p pos. May re-base the entry's block.
    void set_timestamp_at(size_t pos, int64_t ts);

    /// Append \p identity, which must not be present, at position size().
    /// Returns false, changing nothing, if the lookup table cannot take it.
    bool push_back(const dedup_identity_digest& identity, int64_t ts);

    /// Erase the entry at \p pos. The last entry moves into its place.
    /// Requires fully_indexed() or \p pos >= indexed().
    void erase_at(size_t pos);

    /// \brief Entries can be kept without the lookup table.
    ///
    /// Positions [0, indexed()) are in the lookup table, and find() sees only
    /// them; those from indexed() on were appended by append_unindexed() and
    /// are only in the entry array, at 16 bytes an entry and no lookup cost.
    /// A replica that is not filtering needs nothing but the entries, and
    /// indexes them in bulk once it is: begin_bulk_index(), prepare_step()
    /// until it returns false, then index_next() up to size(). push_back()
    /// and every lookup need fully_indexed().
    size_t indexed() const noexcept { return _indexed; }
    bool fully_indexed() const noexcept {
        return _indexed == _size && !_preparing;
    }

    /// Append \p identity at position size() without indexing it. It may be
    /// present already, which index_next() reports when it gets to it.
    /// False, changing nothing, at max_size.
    bool append_unindexed(const dedup_identity_digest& identity, int64_t ts);

    /// Free the lookup table, leaving every entry unindexed.
    void drop_lookup() noexcept;

    /// With nothing indexed, start a lookup table sized for size() entries:
    /// its segments, which prepare_step() allocates, are what that many keys
    /// spread over, so index_next() does not grow them a doubling at a time,
    /// each doubling reading back every key. O(1).
    void begin_bulk_index();
    /// Allocate up to \p n more of the segments begin_bulk_index() sized.
    /// True while some are left; index_next() needs none to be.
    bool prepare_step(size_t n);

    enum class index_outcome {
        /// The entry at the old indexed() is in the lookup table now.
        indexed,
        /// An indexed entry has the same identity; the caller merges the two
        /// and erases this one, which is still at indexed().
        duplicate,
        /// The lookup table cannot take it, as push_back() can refuse; the
        /// caller erases it.
        refused,
    };
    /// Index the entry at indexed(). For a duplicate, \p duplicate_of is set
    /// to the indexed entry's position.
    index_outcome index_next(size_t& duplicate_of);

    /// Make room for \p n entries without growing the entry storage again.
    void reserve(size_t n);

    /// Bytes allocated for the table, excluding the object itself.
    size_t memory_usage() const noexcept;

    /// For tests: throws std::logic_error if the lookup table and the entries
    /// disagree, or a slot breaks the robin hood order. O(size()).
    void check_invariants() const;

    /// For tests and benchmarks.
    struct table_stats {
        size_t segments{0};
        uint32_t directory_depth{0};
        size_t slots{0};
        uint32_t longest_probe{0};
        size_t rebases{0};
        size_t refused_inserts{0};
        size_t segment_splits{0};
        /// Slots, entries and directory entries every call so far has
        /// touched. The per-call delta is what shows that no operation does
        /// work proportional to the index.
        size_t work{0};
    };
    /// O(segments + slots): for tests and benchmarks.
    table_stats stats() const noexcept;
    /// How many entries a block's re-base could not keep in its span, and so
    /// turned into ones that read back as expired.
    size_t expired_by_rebase() const noexcept { return _expired_by_rebase; }
    /// table_stats::work, in O(1).
    size_t work() const noexcept { return _work; }

private:
    // Packed to 2-byte alignment: 14 bytes, not 16 with padding. An entry
    // can straddle two cache lines, which a standalone benchmark put at a few
    // percent on a lookup.
#pragma pack(push, 2)
    struct packed_entry {
        uint32_t lo_lo;
        uint32_t lo_hi;
        uint16_t hi;
        /// 0: expired. Otherwise the timestamp is the block's base + rel - 1.
        uint32_t rel;
    };
#pragma pack(pop)
    static_assert(sizeof(packed_entry) == 14);

    struct block_meta {
        int64_t base{0};
        /// At least the newest live timestamp in the block: raised on every
        /// write, made exact by a re-base, never lowered by an erase.
        int64_t newest{expired};
        /// Writes since the last re-base. A write too old to keep beside
        /// `newest` is stored expired without a pass over the block, unless
        /// a block's worth of writes has gone by: `newest` can be stale, and
        /// only a re-base refreshes it, so this bounds how long a stale one
        /// costs coverage at one block pass per block of writes.
        uint32_t writes{0};
    };

    struct segment {
        std::unique_ptr<uint32_t[]> slots;
        uint32_t mask{0};
        uint32_t count{0};
        uint32_t depth{0};
        /// Bumped by every insert and erase, so the agreement below can tell
        /// it still describes the segment's keys. 64 bits: it never wraps.
        uint64_t changes{0};
        /// As of `agreement_at` changes: bits set in some key, and bits clear
        /// in some key. A split the keys refused once is refused again for an
        /// incoming key that agrees with them too, without reading them all
        /// back each time.
        uint64_t agreement_at{std::numeric_limits<uint64_t>::max()};
        uint64_t ones{0};
        uint64_t zeros{0};
    };

    static int64_t decode(int64_t base, uint32_t rel) noexcept {
        return rel == 0 ? expired : base + static_cast<int64_t>(rel - 1);
    }

    packed_entry& entry(size_t pos) noexcept {
        return _blocks[pos >> block_shift][pos & (block_entries - 1)];
    }
    const packed_entry& entry(size_t pos) const noexcept {
        return _blocks[pos >> block_shift][pos & (block_entries - 1)];
    }
    static uint64_t hash_of(const packed_entry& e) noexcept {
        return (uint64_t{e.lo_hi} << 32) | uint64_t{e.lo_lo};
    }

    /// Relative value for \p ts in block \p b, re-basing the block if it does
    /// not fit. The entry being written must already read as expired, so the
    /// re-base does not count what it is replacing.
    uint32_t encode(size_t b, int64_t ts);
    void rebase(size_t b, int64_t ts);
    /// Entries in block \p b.
    size_t block_size(size_t b) const noexcept;

    // Lookup table.
    size_t directory_index(uint64_t h) const noexcept {
        return _depth == 0 ? 0 : static_cast<size_t>(h >> (64 - _depth));
    }
    segment& segment_for(uint64_t h) noexcept {
        return *_directory[directory_index(h)];
    }
    const segment& segment_for(uint64_t h) const noexcept {
        return *_directory[directory_index(h)];
    }
    /// Robin hood insert of \p pos; false, changing nothing, on a probe
    /// sequence too long to record.
    bool slot_insert(segment&, uint64_t h, uint32_t pos) const noexcept;
    /// The slot index in \p seg holding \p pos, whose key hashes to \p h.
    uint32_t slot_of(const segment&, uint64_t h, uint32_t pos) const noexcept;
    void slot_erase(segment&, uint32_t slot) const noexcept;
    /// Make room in the segment \p h maps to: double it, or split it. False,
    /// with the table unchanged, if neither is possible.
    bool grow_for(uint64_t h);
    /// \p seg's keys: each one's hash and position.
    std::vector<std::pair<uint64_t, uint32_t>> keys_of(const segment&) const;
    segment make_segment(uint32_t slots, uint32_t depth) const;
    /// Allocate what the entry at position size() needs: all of what can
    /// throw in an append.
    void make_room_for_entry();
    /// Write \p key, in canonical form, at position size(), after
    /// make_room_for_entry(), without touching the lookup table.
    void write_entry(const dedup_identity_digest& key, int64_t ts);
    /// Put the entry at \p pos, hashing to \p h, in the lookup table,
    /// growing it as needed. False if it cannot.
    bool slot_index(uint64_t h, uint32_t pos);
    /// Whether the lookup table is usable: allocated, if begun in bulk.
    bool lookup_ready() const noexcept {
        return !_directory.empty() && !_preparing;
    }

    // Entries: _blocks[i] holds positions [i * block_entries, ...). The first
    // block grows by doubling to block_entries; later blocks are allocated
    // whole.
    std::vector<std::unique_ptr<packed_entry[]>> _blocks;
    std::vector<block_meta> _meta;
    size_t _first_block_capacity{0};
    size_t _size{0};

    std::vector<std::unique_ptr<segment>> _segments;
    std::vector<segment*> _directory;
    uint32_t _depth{0};
    // Entries [0, _indexed) are in the lookup table.
    size_t _indexed{0};
    // Between begin_bulk_index() and the prepare_step() that finishes: the
    // directory entries from _prepared on name no segment yet, and each gets
    // one of _bulk_slots slots.
    bool _preparing{false};
    size_t _prepared{0};
    uint32_t _bulk_slots{0};

    size_t _rebases{0};
    size_t _expired_by_rebase{0};
    size_t _refused_inserts{0};
    size_t _segment_splits{0};
    mutable size_t _work{0};
};

} // namespace cluster
