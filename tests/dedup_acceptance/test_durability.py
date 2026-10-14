"""The index outlives the events a partition goes through: leadership changes,
restarts, crashes, catch-up from the log or from a raft snapshot, replica
moves between nodes and between cores, and segment churn.

Each test checks the same thing afterwards: identities written before the
event are still deduplicated, and new ones are still admitted.
"""

from __future__ import annotations

import time

import pytest
from harness import kafka
from harness.cluster import Cluster
from harness.kafka import Rec

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def dcluster(image, request, pytestconfig):
    c = Cluster(image, nodes=4, smp=2, memory="1G").start()
    yield c
    c.destroy(keep_on_failure=pytestconfig.getoption("--keep-clusters"))


@pytest.fixture(autouse=True)
def _logs(dcluster, request):
    yield
    rep = getattr(request.node, "rep_call", None)
    if rep is not None and rep.failed:
        for n in dcluster.nodes:
            print(f"\n===== node {n.node_id} log tail =====\n{n.logs(tail=200)}")


def make_topic(c: Cluster, partitions=1, replicas=3, window_ms=3_600_000, extra=None):
    t = kafka.unique("dur")
    cfg = {kafka.DEDUP_WINDOW: str(window_ms)}
    cfg.update(extra or {})
    kafka.create_topic(c.brokers, t, partitions, replicas, cfg)
    return t


def seed(c: Cluster, t: str, n: int = 200, partition: int = 0, prefix: str = "seed"):
    keys = [f"{prefix}-{i}".encode() for i in range(n)]
    kafka.produce_ok(
        c.brokers, t, [Rec(k, b"first") for k in keys], partition=partition
    )
    return keys


def assert_still_deduplicating(
    c: Cluster, t: str, seeded: list[bytes], partition: int = 0
):
    """Re-send everything seeded plus some new identities; only the new ones
    may be written."""
    before = kafka.keys_in(c.brokers, t, partition)
    fresh = [f"fresh-{kafka.unique()}".encode() for _ in range(20)]
    recs = [Rec(k, b"again") for k in seeded] + [Rec(k, b"new") for k in fresh]
    kafka.produce_ok(c.brokers, t, recs, partition=partition)
    after = kafka.keys_in(c.brokers, t, partition)
    assert after[: len(before)] == before
    assert after[len(before) :] == fresh, (
        f"{len(after) - len(before)} records admitted, wanted the {len(fresh)} new ones"
    )


def other_replica(c: Cluster, t: str, exclude: set[int]) -> int:
    for r in c.replicas(t, 0):
        if r["node_id"] not in exclude:
            return r["node_id"]
    raise AssertionError("no other replica")


def test_leadership_moves_to_every_replica(dcluster):
    c = dcluster
    t = make_topic(c)
    keys = seed(c, t)
    for r in c.replicas(t, 0):
        c.transfer_leadership(t, 0, r["node_id"])
        assert_still_deduplicating(c, t, keys)


def test_graceful_leader_restart(dcluster):
    c = dcluster
    t = make_topic(c)
    keys = seed(c, t)
    leader = c.leader(t, 0)
    c.node(leader).restart()
    c.wait_healthy()
    assert_still_deduplicating(c, t, keys)
    c.transfer_leadership(t, 0, leader)
    assert_still_deduplicating(c, t, keys)


def test_leader_crash(dcluster):
    c = dcluster
    t = make_topic(c)
    keys = seed(c, t)
    leader = c.leader(t, 0)
    c.node(leader).kill()
    # Another replica takes over while the old leader is down.
    kafka.eventually(lambda: _assert_leader_not(c, t, leader), timeout_s=60)
    assert_still_deduplicating(c, t, keys)
    c.node(leader).start()
    c.wait_healthy()
    c.transfer_leadership(t, 0, leader)
    assert_still_deduplicating(c, t, keys)


def _assert_leader_not(c: Cluster, t: str, node: int):
    assert c.leader(t, 0) != node


def test_whole_cluster_restart(dcluster):
    c = dcluster
    t = make_topic(c, partitions=3)
    seeded = {p: seed(c, t, partition=p) for p in range(3)}
    for n in c.nodes:
        n.stop()
    for n in c.nodes:
        n.start(wait=False)
    for n in c.nodes:
        n.wait_ready()
    c.wait_healthy()
    for p in range(3):
        assert_still_deduplicating(c, t, seeded[p], partition=p)


def test_whole_cluster_crash(dcluster):
    c = dcluster
    t = make_topic(c)
    keys = seed(c, t)
    for n in c.nodes:
        n.kill()
    for n in c.nodes:
        n.start(wait=False)
    for n in c.nodes:
        n.wait_ready()
    c.wait_healthy()
    assert_still_deduplicating(c, t, keys)


def test_follower_catches_up_from_the_log(dcluster):
    c = dcluster
    t = make_topic(c)
    leader = c.leader(t, 0)
    follower = other_replica(c, t, {leader})
    c.node(follower).stop()
    keys = seed(c, t, 500)
    c.node(follower).start()
    c.wait_healthy()
    c.transfer_leadership(t, 0, follower)
    assert_still_deduplicating(c, t, keys)


def test_follower_catches_up_from_a_raft_snapshot(dcluster):
    """The follower misses records that are then prefix-truncated away, so the
    leader has to send it a raft snapshot, and the follower's index comes from
    that snapshot rather than from the log."""
    c = dcluster
    t = make_topic(c, extra={"segment.bytes": str(1024 * 1024)})
    leader = c.leader(t, 0)
    follower = other_replica(c, t, {leader})
    c.node(follower).stop()
    keys = seed(c, t, 2000)
    # More data and rolled segments, then truncate everything written so far.
    kafka.produce_ok(
        c.brokers, t, [Rec(f"filler-{i}".encode(), b"x" * 2048) for i in range(1500)]
    )
    hw = len(kafka.consume_all(c.brokers, t))
    kafka.delete_records(c.brokers, t, 0, hw)
    c.node(follower).start()
    c.wait_healthy()
    c.transfer_leadership(t, 0, follower)
    # The seeded records are no longer in the log at all; only the snapshot
    # remembers them.
    assert kafka.consume_all(c.brokers, t) == []
    kafka.produce_ok(c.brokers, t, [Rec(k, b"again") for k in keys])
    assert kafka.consume_all(c.brokers, t) == []
    kafka.produce_ok(c.brokers, t, [Rec(b"brand-new", b"n")])
    assert kafka.keys_in(c.brokers, t) == [b"brand-new"]


def test_replica_moves_to_a_new_node(dcluster):
    c = dcluster
    t = make_topic(c)
    keys = seed(c, t)
    current = [(r["node_id"], r["core"]) for r in c.replicas(t, 0)]
    nodes_in_use = {n for n, _ in current}
    newcomer = next(n.node_id for n in c.nodes if n.node_id not in nodes_in_use)
    dropped = next(n for n, _ in current if n != c.leader(t, 0))
    target = [(n, core) for n, core in current if n != dropped] + [(newcomer, 0)]
    c.move_replicas(t, 0, target)
    c.transfer_leadership(t, 0, newcomer)
    assert_still_deduplicating(c, t, keys)


def test_replica_moves_to_a_new_node_after_truncation(dcluster):
    """As above, but the new replica can only get the index from a raft
    snapshot: the log no longer holds the identities."""
    c = dcluster
    t = make_topic(c, extra={"segment.bytes": str(1024 * 1024)})
    keys = seed(c, t, 1000)
    hw = len(kafka.consume_all(c.brokers, t))
    kafka.delete_records(c.brokers, t, 0, hw)
    current = [(r["node_id"], r["core"]) for r in c.replicas(t, 0)]
    nodes_in_use = {n for n, _ in current}
    newcomer = next(n.node_id for n in c.nodes if n.node_id not in nodes_in_use)
    dropped = next(n for n, _ in current if n != c.leader(t, 0))
    target = [(n, core) for n, core in current if n != dropped] + [(newcomer, 0)]
    c.move_replicas(t, 0, target)
    c.transfer_leadership(t, 0, newcomer)
    kafka.produce_ok(c.brokers, t, [Rec(k, b"again") for k in keys])
    assert kafka.consume_all(c.brokers, t) == []


def test_replica_moves_between_cores(dcluster):
    c = dcluster
    t = make_topic(c)
    keys = seed(c, t)
    replicas = [r["node_id"] for r in c.replicas(t, 0)]
    leader = c.leader(t, 0)
    c.move_core(t, 0, leader, 1 - c.local_core(t, 0, leader))
    c.transfer_leadership(t, 0, leader)
    assert_still_deduplicating(c, t, keys)
    # Every replica onto its other core, each leading in turn.
    for n in replicas:
        c.move_core(t, 0, n, 1 - c.local_core(t, 0, n))
    for n in replicas:
        c.transfer_leadership(t, 0, n)
        assert_still_deduplicating(c, t, keys)


def test_segment_churn_and_restart(dcluster):
    """Small segments make every roll take a local dedup snapshot. Many of them,
    then a restart that recovers from the latest."""
    c = dcluster
    t = make_topic(c, extra={"segment.bytes": str(1024 * 1024)})
    keys = []
    for round_ in range(8):
        batch = [Rec(f"r{round_}-{i}".encode(), b"y" * 1024) for i in range(1000)]
        kafka.produce_ok(c.brokers, t, batch)
        keys += [r.key for r in batch]
    for n in c.nodes:
        n.restart()
    c.wait_healthy()
    assert_still_deduplicating(c, t, keys)


def test_recovery_bounded_by_the_window(dcluster):
    """After a restart the index is rebuilt from the log only for the window:
    an identity whose record is older than the window is admitted again, one
    inside it is still dropped. Timestamps are explicit, so the test does not
    depend on how long the restart takes."""
    c = dcluster
    t = make_topic(c, window_ms=600_000)
    now = int(time.time() * 1000)
    kafka.produce_ok(c.brokers, t, [Rec(b"old", b"1", timestamp_ms=now - 3_600_000)])
    kafka.produce_ok(c.brokers, t, [Rec(b"recent", b"2", timestamp_ms=now)])
    for n in c.nodes:
        n.restart()
    c.wait_healthy()
    later = int(time.time() * 1000)
    kafka.produce_ok(
        c.brokers,
        t,
        [
            Rec(b"old", b"3", timestamp_ms=later),
            Rec(b"recent", b"4", timestamp_ms=later),
        ],
    )
    assert [r.value for r in kafka.consume_all(c.brokers, t)] == [b"1", b"2", b"3"]


def test_topic_recreated_with_the_same_name_starts_empty(dcluster):
    c = dcluster
    t = make_topic(c)
    seed(c, t, 50)
    kafka.delete_topic(c.brokers, t)
    kafka.create_topic(c.brokers, t, 1, 3, {kafka.DEDUP_WINDOW: "3600000"})
    kafka.produce_ok(c.brokers, t, [Rec(b"seed-0", b"x")])
    assert kafka.keys_in(c.brokers, t) == [b"seed-0"]


def test_partition_count_increase(dcluster):
    c = dcluster
    t = make_topic(c, partitions=1)
    keys = seed(c, t)
    a = kafka.admin(c.brokers)
    from confluent_kafka.admin import NewPartitions

    a.create_partitions([NewPartitions(t, 3)])[t].result()
    kafka.wait_for_leaders(c.brokers, t, 3)
    assert_still_deduplicating(c, t, keys, partition=0)
    kafka.produce_ok(c.brokers, t, [Rec(b"seed-0", b"x")], partition=2)
    assert kafka.keys_in(c.brokers, t, 2) == [b"seed-0"]
