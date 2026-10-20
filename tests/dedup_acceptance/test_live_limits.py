"""Changing the dedup entry limits on a running cluster.

dedup_max_entries_per_partition and dedup_max_entries_per_shard apply without
a restart. test_capacity.py covers raising and lowering each on one node; this
module covers what a change has to survive beyond that: every replica applying
it, partitions created after it, a restart after it, and changes made back and
forth under live traffic.
"""

from __future__ import annotations

import threading
import time

import pytest
from harness import kafka
from harness.cluster import Cluster
from test_capacity import (
    PART_ENTRIES,
    SHARD_ENTRIES,
    SHARD_MAX,
    UNINDEXED,
    readmitted,
    recent,
    require_live_limits,
    require_new_build,
    set_live,
    spread_timestamps,
    topic_on_one_node,
)
from test_stress import assert_memory_healthy, assert_shards_within_budget

pytestmark = pytest.mark.slow


def part_entries(c: Cluster, node_id: int, t: str, partition: int = 0) -> float:
    return (
        c.node(node_id).metrics().sum(PART_ENTRIES, topic=t, partition=str(partition))
    )


def test_lowered_ceiling_applies_on_every_replica(make_cluster, pytestconfig):
    """Followers index what they apply under the same ceiling as the leader,
    so each replica shrinks back under a lowered ceiling with the partition's
    traffic, and keeps the newest identities: a follower that becomes leader
    still deduplicates them."""
    c = make_cluster(nodes=3, smp=1)
    require_new_build(c, pytestconfig)
    require_live_limits(c, pytestconfig)
    t = kafka.unique("live")
    kafka.create_topic(c.brokers, t, 1, 3, {kafka.DEDUP_WINDOW: "3600000"})
    old = [f"old-{i}".encode() for i in range(3000)]
    kafka.produce_bulk(
        c.brokers, t, 0, old, timestamp_ms=spread_timestamps(len(old), 3600)
    )
    for n in c.nodes:
        kafka.eventually(
            lambda n=n: _assert_part_entries(c, n.node_id, t, lambda v: v == 3000),
            timeout_s=60,
        )

    set_live(c, dedup_max_entries_per_partition=500)
    for n in c.nodes:
        assert part_entries(c, n.node_id, t) == 3000, n.node_id

    fresh = [f"fresh-{i}".encode() for i in range(300)]
    kafka.produce_bulk(c.brokers, t, 0, fresh, timestamp_ms=recent(len(fresh)))
    for n in c.nodes:
        kafka.eventually(
            lambda n=n: _assert_part_entries(c, n.node_id, t, lambda v: v <= 500),
            timeout_s=60,
        )
        m = c.node(n.node_id).metrics()
        assert m.sum(UNINDEXED, topic=t) <= 20, (n.node_id, m.sum(UNINDEXED, topic=t))

    leader = c.leader(t, 0)
    follower = next(n.node_id for n in c.nodes if n.node_id != leader)
    c.transfer_leadership(t, 0, follower)
    assert readmitted(c, t, 0, fresh[-100:]) <= 5


def test_partitions_created_after_a_change_get_the_new_ceiling(
    make_cluster, pytestconfig
):
    c = make_cluster(nodes=1, smp=1)
    require_new_build(c, pytestconfig)
    require_live_limits(c, pytestconfig)
    before = topic_on_one_node(c, 1)
    set_live(c, dedup_max_entries_per_partition=300)
    after = topic_on_one_node(c, 1)
    for t in (before, after):
        keys = [f"{t}-{i}".encode() for i in range(2000)]
        kafka.produce_bulk(
            c.brokers, t, 0, keys, timestamp_ms=spread_timestamps(len(keys), 3600)
        )
        got = part_entries(c, 0, t)
        assert 0 < got <= 300, (t, got)
        # The ceiling trims the oldest; the newest are still deduplicated.
        assert readmitted(c, t, 0, keys[-50:]) <= 5, t


def test_a_live_change_persists_across_restart(make_cluster, pytestconfig):
    """A limit changed without a restart is the limit a later restart comes up
    with, and restores are bounded by it."""
    c = make_cluster(nodes=1, smp=1)
    require_new_build(c, pytestconfig)
    require_live_limits(c, pytestconfig)
    t = topic_on_one_node(c, 2, extra={"segment.bytes": str(1024 * 1024)})
    for p in range(2):
        kafka.produce_bulk(
            c.brokers,
            t,
            p,
            [f"p{p}-{i}".encode() for i in range(3000)],
            value=b"x" * 1000,
            timestamp_ms=spread_timestamps(3000, 3600),
        )
    assert c.node(0).metrics().sum(SHARD_ENTRIES) == 6000

    set_live(c, dedup_max_entries_per_shard=1000, dedup_max_entries_per_partition=800)
    c.node(0).restart()
    m = c.node(0).metrics()
    assert m.sum(SHARD_MAX) == 1000
    assert m.sum(SHARD_ENTRIES) <= 1000
    for p in range(2):
        assert part_entries(c, 0, t, p) <= 800, p
    assert not any(s["restart"] for s in c.config_status())
    fresh = [f"fresh-{i}".encode() for i in range(600)]
    kafka.produce_bulk(c.brokers, t, 0, fresh, timestamp_ms=recent(len(fresh)))
    m = c.node(0).metrics()
    assert m.sum(SHARD_ENTRIES) <= 1000
    assert part_entries(c, 0, t, 0) <= 800
    assert readmitted(c, t, 0, fresh[-50:]) <= 5


def test_limits_change_back_and_forth_under_traffic(make_cluster, pytestconfig):
    """Producers keep writing new identities while both limits are raised and
    lowered repeatedly, each change landing in the middle of the traffic.
    Produce stays available, nothing acknowledged is lost or written twice,
    no shard ends over its limit, memory stays healthy, and once the limits
    are generous again the newest identities are deduplicated."""
    c = make_cluster(nodes=1, smp=2)
    require_new_build(c, pytestconfig)
    require_live_limits(c, pytestconfig)
    partitions = 4
    t = topic_on_one_node(c, partitions)
    stop = threading.Event()
    sent: list[list[bytes]] = [[] for _ in range(partitions)]
    errors: list = []

    def traffic():
        p = kafka.producer(c.brokers)

        def cb(err, _msg):
            if err is not None:
                errors.append(err)

        i = 0
        while not stop.is_set():
            part = i % partitions
            k = f"live-{i}".encode()
            while True:
                try:
                    p.produce(t, key=k, value=b"x", partition=part, on_delivery=cb)
                    break
                except BufferError:
                    p.poll(0.05)
            sent[part].append(k)
            i += 1
            if i % 200 == 0:
                p.poll(0)
                time.sleep(0.005)
        assert p.flush(120) == 0, "traffic did not drain"

    th = threading.Thread(target=traffic)
    th.start()
    steps = [
        {"dedup_max_entries_per_shard": 20_000},
        {"dedup_max_entries_per_shard": 2_000},
        {"dedup_max_entries_per_partition": 300},
        {"dedup_max_entries_per_shard": 50_000},
        {"dedup_max_entries_per_partition": 100_000},
        {"dedup_max_entries_per_shard": 1_000},
        {"dedup_max_entries_per_partition": 1_000},
        {
            "dedup_max_entries_per_shard": 0,
            "dedup_max_entries_per_partition": 1_000_000,
        },
    ]
    try:
        time.sleep(3)
        for step in steps:
            set_live(c, **step)
            time.sleep(3)
    finally:
        stop.set()
        th.join()
    assert not errors, f"{len(errors)} records failed, first: {errors[0]}"
    assert sum(len(s) for s in sent) > 10_000, "too little traffic to mean much"
    for part in range(partitions):
        keys = kafka.keys_in(c.brokers, t, part)
        assert sorted(keys) == sorted(sent[part]), part
    assert_shards_within_budget(c)
    assert_memory_healthy(c)
    # The last limits are generous: the newest identities are all remembered.
    for part in range(partitions):
        assert readmitted(c, t, part, sent[part][-100:]) == 0, part


def _assert_part_entries(c: Cluster, node_id: int, t: str, ok) -> None:
    got = part_entries(c, node_id, t)
    assert ok(got), (node_id, got)
