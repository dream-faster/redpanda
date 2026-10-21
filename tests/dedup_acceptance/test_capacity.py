"""How large the index may grow, and what happens at the limits.

The per-partition ceiling (dedup_max_entries_per_partition) bounds one index;
the per-shard limit (dedup_max_entries_per_shard) bounds all of a shard's
indices together, which is what keeps several busy partitions on one core
from exhausting its memory. Each test runs a dedicated single-node cluster so
every partition shares the one shard under test.
"""

from __future__ import annotations

import time

import pytest
from conftest import has_new_build_features
from harness import kafka
from harness.cluster import Cluster
from harness.kafka import Rec

pytestmark = pytest.mark.slow

SHARD_ENTRIES = "vectorized_dedup_shard_index_entries"
SHARD_MAX = "vectorized_dedup_shard_max_index_entries"
PART_ENTRIES = "vectorized_dedup_partition_index_entries"
TRUNCATED = "vectorized_dedup_partition_snapshot_entries_truncated"
UNINDEXED = "vectorized_dedup_partition_records_unindexed"
EVICTED = "vectorized_dedup_partition_capacity_evicted_entries"
RECLAIMED = "vectorized_dedup_partition_expired_entries_reclaimed"
KVSTORE_BYTES = "vectorized_storage_kvstore_cached_bytes"


def require_new_build(c: Cluster, pytestconfig) -> None:
    if not has_new_build_features(c):
        if pytestconfig.getoption("--require-new-build"):
            pytest.fail("image predates the dedup memory fixes")
        pytest.skip("image predates the dedup memory fixes")


def require_reclaim(c: Cluster, pytestconfig) -> None:
    """Builds that reclaim expired entries from a shard's other indices under
    shard pressure export a per-partition count of them. Call once a dedup
    partition exists."""
    if not c.any_node().metrics().has(RECLAIMED):
        msg = "image predates reclaiming expired entries under shard pressure"
        if pytestconfig.getoption("--require-new-build"):
            pytest.fail(msg)
        pytest.skip(msg)


def admitted_on_resend(c: Cluster, t: str, partition: int, recs: list[Rec]) -> int:
    """How many of `recs`, sent again as they are, the partition admits."""
    before = len(kafka.consume_all(c.brokers, t, partition))
    kafka.produce_ok(c.brokers, t, recs, partition=partition)
    return len(kafka.consume_all(c.brokers, t, partition)) - before


def spread_timestamps(n: int, window_s: int):
    """Timestamps spread evenly over most of the window, so the capacity
    cursor's histogram has distinct buckets to choose an oldest quarter from."""
    start = int(time.time() * 1000) - (window_s - 60) * 1000
    step = max(1, ((window_s - 120) * 1000) // n)
    return lambda i: start + i * step


def topic_on_one_node(
    c: Cluster, partitions: int, window_ms: int = 3_600_000, extra=None
) -> str:
    t = kafka.unique("cap")
    cfg = {kafka.DEDUP_WINDOW: str(window_ms)}
    cfg.update(extra or {})
    kafka.create_topic(c.brokers, t, partitions, 1, cfg)
    return t


def test_partition_ceiling_keeps_the_newest_identities(make_cluster):
    c = make_cluster(
        nodes=1, smp=1, cluster_config={"dedup_max_entries_per_partition": 500}
    )
    t = topic_on_one_node(c, 1)
    keys = [f"k{i}".encode() for i in range(3000)]
    ts = spread_timestamps(len(keys), 3600)
    kafka.produce_bulk(c.brokers, t, 0, keys, timestamp_ms=ts)
    m = c.node(0).metrics()
    assert m.sum(PART_ENTRIES, topic=t) <= 500
    assert m.sum(EVICTED, topic=t) + m.sum(UNINDEXED, topic=t) > 0
    # Every identity was distinct, so every record was admitted.
    assert len(kafka.consume_all(c.brokers, t)) == 3000
    # The newest are still remembered, the oldest were let go.
    now = int(time.time() * 1000)
    kafka.produce_ok(
        c.brokers, t, [Rec(k, b"dup", timestamp_ms=now) for k in keys[-100:]]
    )
    assert len(kafka.consume_all(c.brokers, t)) == 3000
    kafka.produce_ok(
        c.brokers, t, [Rec(k, b"old", timestamp_ms=now) for k in keys[:100]]
    )
    assert len(kafka.consume_all(c.brokers, t)) == 3100


def test_shard_limit_caps_the_shards_indices_together(make_cluster, pytestconfig):
    limit = 2000
    c = make_cluster(
        nodes=1, smp=1, cluster_config={"dedup_max_entries_per_shard": limit}
    )
    require_new_build(c, pytestconfig)
    t = topic_on_one_node(c, 4)
    for p in range(4):
        keys = [f"p{p}-{i}".encode() for i in range(1500)]
        kafka.produce_bulk(
            c.brokers, t, p, keys, timestamp_ms=spread_timestamps(1500, 3600)
        )
        m = c.node(0).metrics()
        assert m.sum(SHARD_ENTRIES) <= limit, f"after partition {p}"

    m = c.node(0).metrics()
    assert m.sum(SHARD_MAX) == limit
    assert m.sum(SHARD_ENTRIES) <= limit
    assert m.sum(SHARD_ENTRIES) == m.sum(PART_ENTRIES)
    # Each partition alone is far below its own (default) ceiling: the shard
    # is what bound them.
    for p in range(4):
        assert m.sum(PART_ENTRIES, topic=t, partition=str(p)) < 1_000_000
    # Produce stays available, and the most recently written identities are
    # still deduplicated.
    for p in range(4):
        assert len(kafka.consume_all(c.brokers, t, p)) == 1500
    # The last partition written got its room from the earlier ones, which
    # went idle holding the budget: its newest identities are (almost all)
    # remembered rather than admitted unindexed.
    now = int(time.time() * 1000)
    kafka.produce_ok(
        c.brokers,
        t,
        [Rec(f"p3-{i}".encode(), b"d", timestamp_ms=now) for i in range(1450, 1500)],
        partition=3,
    )
    readmitted = len(kafka.consume_all(c.brokers, t, 3)) - 1500
    assert readmitted <= 10, f"{readmitted} of the newest 50 were not remembered"


def test_shard_limit_spans_topics(make_cluster, pytestconfig):
    limit = 1000
    c = make_cluster(
        nodes=1, smp=1, cluster_config={"dedup_max_entries_per_shard": limit}
    )
    require_new_build(c, pytestconfig)
    topics = [topic_on_one_node(c, 1) for _ in range(3)]
    for t in topics:
        kafka.produce_bulk(
            c.brokers,
            t,
            0,
            [f"{t}-{i}".encode() for i in range(800)],
            timestamp_ms=spread_timestamps(800, 3600),
        )
    m = c.node(0).metrics()
    assert m.sum(SHARD_ENTRIES) <= limit
    assert m.sum(SHARD_ENTRIES) == sum(m.sum(PART_ENTRIES, topic=t) for t in topics)


def test_shard_limit_is_per_shard(make_cluster, pytestconfig):
    """Two shards, each with its own budget: filling one leaves the other's
    alone."""
    limit = 1000
    c = make_cluster(
        nodes=1, smp=2, cluster_config={"dedup_max_entries_per_shard": limit}
    )
    require_new_build(c, pytestconfig)
    t = topic_on_one_node(c, 4)
    shards = {p: c.local_core(t, p, 0) for p in range(4)}
    assert len(set(shards.values())) == 2, shards
    full_shard = shards[0]
    for p in [p for p, s in shards.items() if s == full_shard]:
        kafka.produce_bulk(
            c.brokers,
            t,
            p,
            [f"p{p}-{i}".encode() for i in range(1500)],
            timestamp_ms=spread_timestamps(1500, 3600),
        )
    m = c.node(0).metrics()
    per_shard = {int(l["shard"]): v for l, v in m.select(SHARD_ENTRIES)}
    assert per_shard[full_shard] <= limit
    assert per_shard[1 - full_shard] == 0


def _fill_busy_partition(c: Cluster, n: int, now: int) -> tuple[str, list[Rec]]:
    """Fill a fresh partition with `n` identities, 50 ms apart up to ten
    minutes ago, every one inside its hour-long window. Returns the topic and
    what it was sent."""
    busy = topic_on_one_node(c, 1)
    recs = [
        Rec(f"busy-{i}".encode(), timestamp_ms=now - 1_300_000 + i * 50)
        for i in range(n)
    ]
    kafka.produce_bulk(
        c.brokers,
        busy,
        0,
        (r.key for r in recs),
        timestamp_ms=lambda i: recs[i].timestamp_ms,
    )
    return busy, recs


def _assert_busy_kept_everything(c: Cluster, busy: str, recs: list[Rec]) -> None:
    m = c.node(0).metrics()
    assert m.sum(SHARD_ENTRIES) <= m.sum(SHARD_MAX)
    assert m.sum(EVICTED, topic=busy) == 0
    assert m.sum(UNINDEXED, topic=busy) == 0
    assert m.sum(PART_ENTRIES, topic=busy) == len(recs)
    # The oldest and the newest it was sent are both still caught.
    assert admitted_on_resend(c, busy, 0, recs[:500] + recs[-500:]) == 0


def test_expired_entries_elsewhere_go_before_live_ones(make_cluster, pytestconfig):
    """A partition whose entries have all expired, but whose own cursor has
    not reached them yet, holds two fifths of the shard while another
    partition fills the rest. The shard reclaims the expired entries rather
    than trim the busy partition's live ones: it keeps every identity it
    was sent."""
    limit = 20_000
    c = make_cluster(
        nodes=1, smp=1, cluster_config={"dedup_max_entries_per_shard": limit}
    )
    require_new_build(c, pytestconfig)
    stale = topic_on_one_node(c, 1, window_ms=60_000)
    now = int(time.time() * 1000)
    held = limit * 2 // 5
    kafka.produce_bulk(
        c.brokers,
        stale,
        0,
        (f"stale-{i}".encode() for i in range(held)),
        timestamp_ms=lambda i: now - 2_000_000 + i,
    )
    # One record ten minutes on puts every earlier one outside the window; the
    # partition's own cursor drops only a step's worth of them.
    kafka.produce_ok(
        c.brokers,
        stale,
        [Rec(b"stale-late", timestamp_ms=now - 1_400_000)],
        partition=0,
    )
    require_reclaim(c, pytestconfig)
    assert c.node(0).metrics().sum(PART_ENTRIES, topic=stale) > held * 9 // 10

    busy, recs = _fill_busy_partition(c, limit * 7 // 10, now)
    assert c.node(0).metrics().sum(RECLAIMED, topic=stale) > 0
    _assert_busy_kept_everything(c, busy, recs)


def test_a_quiet_partitions_entries_age_with_the_shard(make_cluster, pytestconfig):
    """A partition that went quiet holding two fifths of the shard: by its own
    watermark nothing in it ever expires, since nothing newer arrives. Its
    entries age with the shard's newest records instead, so once those are a
    window past them they are reclaimed, and the busy partition filling the
    rest of the shard keeps every identity it was sent."""
    limit = 20_000
    c = make_cluster(
        nodes=1, smp=1, cluster_config={"dedup_max_entries_per_shard": limit}
    )
    require_new_build(c, pytestconfig)
    quiet = topic_on_one_node(c, 1, window_ms=60_000)
    now = int(time.time() * 1000)
    held = limit * 2 // 5
    # Eight seconds from first to last, well inside the minute-long window.
    kafka.produce_bulk(
        c.brokers,
        quiet,
        0,
        (f"quiet-{i}".encode() for i in range(held)),
        timestamp_ms=lambda i: now - 1_600_000 + i,
    )
    require_reclaim(c, pytestconfig)
    assert c.node(0).metrics().sum(PART_ENTRIES, topic=quiet) == held

    # The busy partition's records start five minutes after the quiet one's
    # last: a window and more.
    busy, recs = _fill_busy_partition(c, limit * 7 // 10, now)
    assert c.node(0).metrics().sum(RECLAIMED, topic=quiet) > 0
    _assert_busy_kept_everything(c, busy, recs)


def test_a_partition_stamping_behind_its_neighbours_keeps_its_entries(
    make_cluster, pytestconfig
):
    """A partition whose producers stamp records half an hour behind its
    neighbour's, still receiving them: its entries are all inside its own
    window, so however far ahead the shard's newest records are, none of them
    is reclaimed. Only the shard's own trimming takes from it."""
    limit = 20_000
    c = make_cluster(
        nodes=1, smp=1, cluster_config={"dedup_max_entries_per_shard": limit}
    )
    require_new_build(c, pytestconfig)
    lagging = topic_on_one_node(c, 1, window_ms=60_000)
    busy = topic_on_one_node(c, 1)
    now = int(time.time() * 1000)
    chunk = 500
    p = kafka.producer(c.brokers)
    for r in range(30):
        # In turns, each flushed before the next, so the broker sees them in
        # this order: the lagging partition keeps receiving records as the
        # shard's newest move on.
        for i in range(chunk):
            n = r * chunk + i
            p.produce(
                lagging,
                key=f"lag-{n}".encode(),
                value=b"v",
                partition=0,
                timestamp=now - 3_000_000 + n,
            )
        assert p.flush(60) == 0
        for i in range(chunk):
            n = r * chunk + i
            p.produce(
                busy,
                key=f"busy-{n}".encode(),
                value=b"v",
                partition=0,
                timestamp=now - 1_200_000 + n * 10,
            )
        assert p.flush(60) == 0
        if r == 0:
            require_reclaim(c, pytestconfig)
    m = c.node(0).metrics()
    assert m.sum(SHARD_ENTRIES) <= limit
    assert m.sum(RECLAIMED, topic=lagging) == 0
    assert m.sum(RECLAIMED, topic=busy) == 0
    # The shard was over its limit all the same, and trimmed for it.
    assert (
        m.sum(EVICTED, topic=lagging)
        + m.sum(EVICTED, topic=busy)
        + m.sum(UNINDEXED, topic=lagging)
        + m.sum(UNINDEXED, topic=busy)
    ) > 0


def test_default_shard_limit_follows_shard_memory(make_cluster, pytestconfig):
    c = make_cluster(nodes=1, smp=2, memory="2G")
    require_new_build(c, pytestconfig)
    m = c.node(0).metrics()
    limits = [v for _, v in m.select(SHARD_MAX)]
    assert len(limits) == 2
    # 15% of each shard's ~1 GiB at 44 bytes an entry, give or take what
    # seastar keeps for itself.
    expect = (1 << 30) * 15 // 100 // 44
    for v in limits:
        assert 0.7 * expect <= v <= 1.05 * expect, (v, expect)


def test_lowering_the_shard_limit_takes_effect_on_restart(make_cluster, pytestconfig):
    """Restoring more than the new limit allows must not be how the node runs
    out of memory: restores are bounded by the shard's headroom."""
    c = make_cluster(
        nodes=1, smp=1, cluster_config={"dedup_max_entries_per_shard": 8000}
    )
    require_new_build(c, pytestconfig)
    t = topic_on_one_node(c, 2, extra={"segment.bytes": str(1024 * 1024)})
    for p in range(2):
        # ~3 MB a partition, so several segment rolls write local snapshots.
        kafka.produce_bulk(
            c.brokers,
            t,
            p,
            [f"p{p}-{i}".encode() for i in range(3000)],
            value=b"x" * 1000,
            timestamp_ms=spread_timestamps(3000, 3600),
        )
    before = c.node(0).metrics().sum(SHARD_ENTRIES)
    assert before == 6000

    c.set_config(dedup_max_entries_per_shard=1000)
    c.node(0).restart()
    m = c.node(0).metrics()
    assert m.sum(SHARD_MAX) == 1000
    assert m.sum(SHARD_ENTRIES) <= 1000
    assert m.sum(TRUNCATED, topic=t) > 0
    # Right after the restore the shard is at its limit and the cursor has
    # not finished a pass, so the first new identities may go unindexed; a
    # few hundred later, trimming has made room again.
    now = int(time.time() * 1000)
    kafka.produce_bulk(
        c.brokers,
        t,
        0,
        [f"warm-{i}".encode() for i in range(500)],
        timestamp_ms=lambda i: now + i,
    )
    later = now + 1000
    kafka.produce_ok(c.brokers, t, [Rec(b"after-restart", b"x", timestamp_ms=later)])
    kafka.produce_ok(
        c.brokers, t, [Rec(b"after-restart", b"y", timestamp_ms=later + 1)]
    )
    assert kafka.keys_in(c.brokers, t, 0).count(b"after-restart") == 1
    assert c.node(0).metrics().sum(SHARD_ENTRIES) <= 1000


# -- changing the limits without a restart -----------------------------------

LIVE_LIMITS = ("dedup_max_entries_per_partition", "dedup_max_entries_per_shard")


def require_live_limits(c: Cluster, pytestconfig) -> None:
    """Builds that apply both limits without a restart say so in the cluster
    config schema."""
    schema = c.config_schema()
    if any(schema[p]["needs_restart"] for p in LIVE_LIMITS):
        msg = "image needs a restart to change the dedup entry limits"
        if pytestconfig.getoption("--require-new-build"):
            pytest.fail(msg)
        pytest.skip(msg)


def set_live(c: Cluster, **values) -> None:
    """Change cluster properties and wait for every node to apply them,
    asserting none of them asks for a restart to do so."""
    status = c.wait_config_applied(c.set_config(**values))
    assert not any(s["restart"] for s in status), status


def readmitted(c: Cluster, t: str, partition: int, keys: list[bytes]) -> int:
    """How many of `keys`, sent again now, the partition admits: the ones its
    index no longer remembers."""
    now = int(time.time() * 1000)
    return admitted_on_resend(
        c, t, partition, [Rec(k, b"again", timestamp_ms=now) for k in keys]
    )


def recent(n: int, behind_s: int = 60):
    """Ascending timestamps ending `behind_s` seconds ago: newer than anything
    spread_timestamps() wrote, and never ahead of the broker's clock."""
    start = int(time.time() * 1000) - behind_s * 1000 - n
    return lambda i: start + i


def test_limits_change_without_a_restart(make_cluster, pytestconfig):
    c = make_cluster(
        nodes=1, smp=2, cluster_config={"dedup_max_entries_per_shard": 4000}
    )
    require_new_build(c, pytestconfig)
    require_live_limits(c, pytestconfig)
    started = c.node(0).metrics().sum("vectorized_application_uptime")
    assert [v for _, v in c.node(0).metrics().select(SHARD_MAX)] == [4000, 4000]

    set_live(c, dedup_max_entries_per_shard=9000, dedup_max_entries_per_partition=700)
    kafka.eventually(lambda: _assert_shard_max(c, [9000, 9000]), timeout_s=30)
    # 0 derives the limit from shard memory again, as it does at startup.
    set_live(c, dedup_max_entries_per_shard=0)
    kafka.eventually(lambda: _assert_derived_shard_max(c), timeout_s=30)
    # The same process throughout.
    assert c.node(0).metrics().sum("vectorized_application_uptime") >= started


def test_raising_the_shard_limit_live_indexes_new_identities(
    make_cluster, pytestconfig
):
    c = make_cluster(
        nodes=1, smp=1, cluster_config={"dedup_max_entries_per_shard": 1000}
    )
    require_new_build(c, pytestconfig)
    require_live_limits(c, pytestconfig)
    t = topic_on_one_node(c, 2)
    for p in range(2):
        kafka.produce_bulk(
            c.brokers,
            t,
            p,
            [f"p{p}-{i}".encode() for i in range(1500)],
            timestamp_ms=spread_timestamps(1500, 3600),
        )
    m = c.node(0).metrics()
    assert m.sum(SHARD_ENTRIES) <= 1000
    held = m.sum(SHARD_ENTRIES)

    set_live(c, dedup_max_entries_per_shard=20_000)
    kafka.eventually(lambda: _assert_shard_max(c, [20_000]), timeout_s=30)
    unindexed = c.node(0).metrics().sum(UNINDEXED, topic=t)
    fresh = [f"fresh-{i}".encode() for i in range(3000)]
    kafka.produce_bulk(c.brokers, t, 0, fresh, timestamp_ms=recent(len(fresh)))
    m = c.node(0).metrics()
    # Every new identity was indexed, on top of what the shard already held
    # (less the few of its oldest entries that may have aged out since).
    assert m.sum(UNINDEXED, topic=t) == unindexed
    assert m.sum(SHARD_ENTRIES) >= 0.9 * held + len(fresh)
    assert m.sum(SHARD_ENTRIES) <= 20_000
    assert readmitted(c, t, 0, fresh) == 0


def test_raising_the_partition_ceiling_live_indexes_new_identities(
    make_cluster, pytestconfig
):
    c = make_cluster(
        nodes=1, smp=1, cluster_config={"dedup_max_entries_per_partition": 500}
    )
    require_new_build(c, pytestconfig)
    require_live_limits(c, pytestconfig)
    t = topic_on_one_node(c, 1)
    keys = [f"k{i}".encode() for i in range(3000)]
    kafka.produce_bulk(
        c.brokers, t, 0, keys, timestamp_ms=spread_timestamps(len(keys), 3600)
    )
    m = c.node(0).metrics()
    assert m.sum(PART_ENTRIES, topic=t) <= 500
    held = m.sum(PART_ENTRIES, topic=t)

    set_live(c, dedup_max_entries_per_partition=10_000)
    unindexed = c.node(0).metrics().sum(UNINDEXED, topic=t)
    fresh = [f"fresh-{i}".encode() for i in range(3000)]
    kafka.produce_bulk(c.brokers, t, 0, fresh, timestamp_ms=recent(len(fresh)))
    m = c.node(0).metrics()
    assert m.sum(UNINDEXED, topic=t) == unindexed
    assert m.sum(PART_ENTRIES, topic=t) >= 0.9 * held + len(fresh)
    assert readmitted(c, t, 0, fresh) == 0


def test_lowering_the_partition_ceiling_live_keeps_the_newest(
    make_cluster, pytestconfig
):
    """Lowered below what an index holds, the ceiling drops nothing at once;
    the index shrinks back with its traffic, a bounded number of its oldest
    entries per new identity, and indexes the new identities meanwhile."""
    c = make_cluster(nodes=1, smp=1)
    require_new_build(c, pytestconfig)
    require_live_limits(c, pytestconfig)
    t = topic_on_one_node(c, 1)
    old = [f"old-{i}".encode() for i in range(5000)]
    kafka.produce_bulk(
        c.brokers, t, 0, old, timestamp_ms=spread_timestamps(len(old), 3600)
    )
    assert c.node(0).metrics().sum(PART_ENTRIES, topic=t) == 5000

    set_live(c, dedup_max_entries_per_partition=500)
    # Nothing is trimmed without traffic.
    time.sleep(2)
    m = c.node(0).metrics()
    assert m.sum(PART_ENTRIES, topic=t) == 5000
    unindexed = m.sum(UNINDEXED, topic=t)

    fresh = [f"fresh-{i}".encode() for i in range(300)]
    kafka.produce_bulk(c.brokers, t, 0, fresh, timestamp_ms=recent(len(fresh)))
    m = c.node(0).metrics()
    assert m.sum(PART_ENTRIES, topic=t) <= 500
    assert m.sum(EVICTED, topic=t) >= 4500
    # Shrinking took old entries and left the new identities indexed.
    assert m.sum(UNINDEXED, topic=t) - unindexed <= 20
    assert readmitted(c, t, 0, fresh[-100:]) <= 5
    # It stays under the new ceiling.
    more = [f"more-{i}".encode() for i in range(2000)]
    kafka.produce_bulk(c.brokers, t, 0, more, timestamp_ms=recent(len(more), 30))
    assert c.node(0).metrics().sum(PART_ENTRIES, topic=t) <= 500
    # The oldest identities were let go.
    assert readmitted(c, t, 0, old[:100]) >= 90


def test_lowering_the_shard_limit_live_shrinks_the_shard(make_cluster, pytestconfig):
    """Lowered below what the shard holds, the shard limit drops nothing at
    once. New identities anywhere on the shard then trim its largest indices,
    an idle one's included, until the shard is back under."""
    c = make_cluster(
        nodes=1, smp=1, cluster_config={"dedup_max_entries_per_shard": 8000}
    )
    require_new_build(c, pytestconfig)
    require_live_limits(c, pytestconfig)
    t = topic_on_one_node(c, 2)
    for p in range(2):
        kafka.produce_bulk(
            c.brokers,
            t,
            p,
            [f"p{p}-{i}".encode() for i in range(3000)],
            timestamp_ms=spread_timestamps(3000, 3600),
        )
    assert c.node(0).metrics().sum(SHARD_ENTRIES) == 6000

    set_live(c, dedup_max_entries_per_shard=1000)
    kafka.eventually(lambda: _assert_shard_max(c, [1000]), timeout_s=30)
    m = c.node(0).metrics()
    assert m.sum(SHARD_ENTRIES) == 6000
    unindexed = m.sum(UNINDEXED, topic=t)

    fresh = [f"fresh-{i}".encode() for i in range(500)]
    kafka.produce_bulk(c.brokers, t, 0, fresh, timestamp_ms=recent(len(fresh)))
    m = c.node(0).metrics()
    assert m.sum(SHARD_ENTRIES) <= 1000
    assert m.sum(SHARD_ENTRIES) == m.sum(PART_ENTRIES)
    # The idle partition gave up room too.
    assert m.sum(PART_ENTRIES, topic=t, partition="1") < 3000
    assert m.sum(UNINDEXED, topic=t) - unindexed <= 10
    assert readmitted(c, t, 0, fresh[-50:]) <= 5


def _assert_shard_max(c: Cluster, want: list[int]):
    got = sorted(v for _, v in c.node(0).metrics().select(SHARD_MAX))
    assert got == sorted(want), got


def _assert_derived_shard_max(c: Cluster):
    # The default limit is far above anything set explicitly above.
    got = [v for _, v in c.node(0).metrics().select(SHARD_MAX)]
    assert len(got) == 2 and all(v > 100_000 for v in got), got


def test_dedup_snapshots_are_files_and_the_kvstore_stays_small(
    make_cluster, pytestconfig
):
    """The memory regression itself: with large indices that snapshot often,
    the kvstore must not hold them. Before the fix it held every partition's
    latest dedup snapshot (~30 bytes an entry) for good."""
    c = make_cluster(nodes=1, smp=1)
    require_new_build(c, pytestconfig)
    t = topic_on_one_node(c, 4, extra={"segment.bytes": str(1024 * 1024)})
    for p in range(4):
        kafka.produce_bulk(
            c.brokers,
            t,
            p,
            (f"p{p}-{i}".encode() for i in range(50_000)),
            value=b"x" * 100,
        )
    m = c.node(0).metrics()
    assert m.sum(PART_ENTRIES, topic=t) == 200_000
    # Tens of KiB of raft and offset-translator state, not ~6 MB of indices.
    assert m.sum(KVSTORE_BYTES) < 256 * 1024, m.sum(KVSTORE_BYTES)
    for p in range(4):
        snap = c.node(0).partition_dir(t, p) / "dedup.snapshot"
        kafka.eventually(lambda snap=snap: _assert_exists(snap), timeout_s=60)
    # And restarting from those files restores the indices. Until a partition
    # leads a produce request its entries stay unindexed, and an identity the
    # snapshot holds from a batch committed but not yet applied when it was
    # taken is replayed into it a second time; the first request merges them.
    c.node(0).restart()
    m = c.node(0).metrics()
    kafka.eventually(lambda: _assert_entries_at_least(c, t, 200_000), timeout_s=60)
    assert m.sum(KVSTORE_BYTES) < 256 * 1024
    now = int(time.time() * 1000)
    kafka.produce_ok(
        c.brokers,
        t,
        [
            Rec(f"p1-{i}".encode(), b"d", timestamp_ms=now)
            for i in range(0, 50_000, 500)
        ],
        partition=1,
    )
    assert len(kafka.consume_all(c.brokers, t, 1)) == 50_000
    for p in (0, 2, 3):
        kafka.produce_ok(
            c.brokers, t, [Rec(f"p{p}-0".encode(), b"d", timestamp_ms=now)], partition=p
        )
    _assert_entries(c, t, 200_000)


def _assert_exists(path):
    assert path.exists(), path


def _assert_entries(c: Cluster, t: str, want: int):
    got = c.node(0).metrics().sum(PART_ENTRIES, topic=t)
    assert got == want, got


def _assert_entries_at_least(c: Cluster, t: str, want: int):
    got = c.node(0).metrics().sum(PART_ENTRIES, topic=t)
    assert got >= want, got
