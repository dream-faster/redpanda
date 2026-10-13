"""A broker with large dedup indices must always be able to start.

The incident behind these tests: a shard whose dedup snapshots, held several
times over during a snapshot, exhausted its memory -- and then kept doing so
on every start, so the broker could not come back. These restart brokers in
the conditions that make startup expensive (full shards, little memory, many
partitions restoring at once, less memory than the indices were built with)
and require every start to succeed with the indices bounded.
"""

from __future__ import annotations

import time

import pytest
from harness import kafka
from harness.cluster import Cluster
from harness.kafka import Rec
from test_capacity import (
    KVSTORE_BYTES,
    PART_ENTRIES,
    SHARD_ENTRIES,
    SHARD_MAX,
    require_new_build,
    topic_on_one_node,
)

pytestmark = pytest.mark.slow


def fill(
    c: Cluster, t: str, partitions: int, per_partition: int, value: bytes = b"v"
) -> None:
    for p in range(partitions):
        kafka.produce_bulk(
            c.brokers,
            t,
            p,
            (f"p{p}-{i:08d}".encode() for i in range(per_partition)),
            value=value,
        )


def assert_serving(c: Cluster, t: str, partitions: int) -> None:
    """Produce works on every partition and a fresh identity is deduplicated."""
    for p in range(partitions):
        key = f"probe-{kafka.unique()}".encode()
        kafka.produce_ok(c.brokers, t, [Rec(key, b"1")], partition=p)
        kafka.produce_ok(c.brokers, t, [Rec(key, b"2")], partition=p)
    for p in range(partitions):
        probes = [k for k in kafka.keys_in(c.brokers, t, p) if k.startswith(b"probe-")]
        assert len(probes) == len(set(probes)), (
            f"partition {p} admitted a duplicate probe"
        )


def assert_bounded(c: Cluster, node: int = 0) -> dict[str, float]:
    m = c.node(node).metrics()
    shard_entries = {int(l["shard"]): v for l, v in m.select(SHARD_ENTRIES)}
    shard_max = {int(l["shard"]): v for l, v in m.select(SHARD_MAX)}
    for s, v in shard_entries.items():
        assert v <= shard_max[s], (
            f"shard {s}: {v} entries over its limit {shard_max[s]}"
        )
    assert m.sum(KVSTORE_BYTES) < 1024 * 1024, (
        f"kvstore holds {m.sum(KVSTORE_BYTES)} bytes"
    )
    return {"entries": m.sum(SHARD_ENTRIES), "max": m.sum(SHARD_MAX)}


def test_full_shard_restarts_in_little_memory(make_cluster, pytestconfig):
    """Four partitions filling a 512 MiB shard's budget, restarted cleanly
    twice and killed once: every start succeeds."""
    c = make_cluster(nodes=1, smp=1, memory="512M")
    require_new_build(c, pytestconfig)
    limit = c.node(0).metrics().sum(SHARD_MAX)
    t = topic_on_one_node(c, 4, extra={"segment.bytes": str(4 * 1024 * 1024)})
    per = int(limit * 0.4)  # 1.6x the budget in all
    fill(c, t, 4, per)
    state = assert_bounded(c)
    assert state["entries"] >= 0.8 * limit

    for how in ("restart", "restart", "kill"):
        if how == "kill":
            c.node(0).kill()
            c.node(0).start()
        else:
            c.node(0).restart()
        state = assert_bounded(c)
        assert_serving(c, t, 4)


def test_less_memory_than_the_indices_were_built_with(make_cluster, pytestconfig):
    """Indices built on a 1 GiB shard, restarted with half of that: the
    restore is cut to the smaller budget instead of exhausting it."""
    c = make_cluster(nodes=1, smp=1, memory="1G")
    require_new_build(c, pytestconfig)
    big_limit = c.node(0).metrics().sum(SHARD_MAX)
    t = topic_on_one_node(c, 4, extra={"segment.bytes": str(4 * 1024 * 1024)})
    fill(c, t, 4, int(big_limit * 0.22))
    assert c.node(0).metrics().sum(SHARD_ENTRIES) >= 0.8 * big_limit

    c.node(0).restart(memory="512M")
    state = assert_bounded(c)
    assert state["max"] < 0.6 * big_limit
    assert state["entries"] <= state["max"]
    assert_serving(c, t, 4)

    # And back up: nothing is stuck at the smaller size.
    c.node(0).restart(memory="1G")
    assert_bounded(c)
    assert_serving(c, t, 4)


def test_many_partitions_restore_at_once(make_cluster, pytestconfig):
    c = make_cluster(nodes=1, smp=1, memory="1G")
    require_new_build(c, pytestconfig)
    t = topic_on_one_node(c, 24, extra={"segment.bytes": str(1024 * 1024)})
    fill(c, t, 24, 40_000)
    before = c.node(0).metrics().sum(PART_ENTRIES, topic=t)
    assert before == 24 * 40_000
    c.node(0).restart()
    assert_bounded(c)
    kafka.eventually(
        lambda: _assert_entries(c, t, before), timeout_s=120, msg="indices not restored"
    )
    assert_serving(c, t, 24)


def _assert_entries(c: Cluster, t: str, want: float) -> None:
    got = c.node(0).metrics().sum(PART_ENTRIES, topic=t)
    assert got == want, (got, want)


def test_three_node_cluster_with_full_shards_restarts(make_cluster, pytestconfig):
    """Replicated: every replica, leader or follower, holds a full index, and
    the whole cluster restarts at once."""
    c = make_cluster(nodes=3, smp=1, memory="768M")
    require_new_build(c, pytestconfig)
    limit = c.node(0).metrics().sum(SHARD_MAX)
    t = kafka.unique("rep")
    kafka.create_topic(
        c.brokers,
        t,
        3,
        3,
        {kafka.DEDUP_WINDOW: "3600000", "segment.bytes": str(4 * 1024 * 1024)},
    )
    fill(c, t, 3, int(limit * 0.5))
    for n in c.nodes:
        assert_bounded(c, n.node_id)
    for n in c.nodes:
        n.stop()
    for n in c.nodes:
        n.start(wait=False)
    for n in c.nodes:
        n.wait_ready()
    c.wait_healthy()
    for n in c.nodes:
        assert_bounded(c, n.node_id)
    assert_serving(c, t, 3)


def test_restart_while_producing(make_cluster, pytestconfig):
    """Startup with produce traffic arriving the whole time: the index is
    rebuilt while it is also being added to."""
    import threading

    c = make_cluster(nodes=3, smp=1, memory="1G")
    require_new_build(c, pytestconfig)
    t = kafka.unique("live")
    kafka.create_topic(c.brokers, t, 2, 3, {kafka.DEDUP_WINDOW: "3600000"})
    fill(c, t, 2, 100_000)
    stop = threading.Event()
    sent: list[bytes] = []

    def traffic():
        p = kafka.producer(c.brokers, **{"message.timeout.ms": 120000})
        i = 0
        while not stop.is_set():
            k = f"live-{i}".encode()
            p.produce(t, key=k, value=b"x", partition=i % 2)
            sent.append(k)
            i += 1
            if i % 200 == 0:
                p.poll(0)
                time.sleep(0.01)
        p.flush(120)

    th = threading.Thread(target=traffic)
    th.start()
    try:
        for n in c.nodes:
            n.restart()
            c.wait_healthy()
    finally:
        stop.set()
        th.join()
    for n in c.nodes:
        assert_bounded(c, n.node_id)
    # Every live identity was produced once; whatever retries the restarts
    # caused were absorbed, so none appears twice.
    for p in range(2):
        live = [k for k in kafka.keys_in(c.brokers, t, p) if k.startswith(b"live-")]
        assert len(live) == len(set(live)), f"partition {p} holds a duplicate"
