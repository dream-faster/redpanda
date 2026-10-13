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
KVSTORE_BYTES = "vectorized_storage_kvstore_cached_bytes"


def require_new_build(c: Cluster, pytestconfig) -> None:
    if not has_new_build_features(c):
        if pytestconfig.getoption("--require-new-build"):
            pytest.fail("image predates the dedup memory fixes")
        pytest.skip("image predates the dedup memory fixes")


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


def test_default_shard_limit_follows_shard_memory(make_cluster, pytestconfig):
    c = make_cluster(nodes=1, smp=2, memory="2G")
    require_new_build(c, pytestconfig)
    m = c.node(0).metrics()
    limits = [v for _, v in m.select(SHARD_MAX)]
    assert len(limits) == 2
    # 15% of each shard's ~1 GiB at 80 bytes an entry, give or take what
    # seastar keeps for itself.
    expect = (1 << 30) * 15 // 100 // 80
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
    # And restarting from those files restores the indices.
    c.node(0).restart()
    m = c.node(0).metrics()
    kafka.eventually(lambda: _assert_entries(c, t, 200_000), timeout_s=60)
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


def _assert_exists(path):
    assert path.exists(), path


def _assert_entries(c: Cluster, t: str, want: int):
    got = c.node(0).metrics().sum(PART_ENTRIES, topic=t)
    assert got == want, got
