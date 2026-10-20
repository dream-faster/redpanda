"""Upgrading from, downgrading to, and running mixed with an earlier dedup
build (--old-image), which kept dedup snapshots in the kvstore in the version
1 format.

What must hold across every step: brokers start, the kvstore's legacy dedup
snapshots are gone once a node runs the new build, and identities written
within the window are still deduplicated (the index is rebuilt from the log
where a snapshot could not be used).
"""

from __future__ import annotations

import pytest
from conftest import has_new_build_features
from harness import kafka
from harness.cluster import Cluster
from harness.kafka import Rec
from test_capacity import KVSTORE_BYTES, PART_ENTRIES
from test_startup import assert_serving

pytestmark = [pytest.mark.slow, pytest.mark.upgrade]


# Whether --image has the new build's features: probed once per run, not once
# per test, since it cannot change in between.
_image_is_new_build: bool | None = None


@pytest.fixture
def upgrade_cluster(old_image, image, make_cluster, pytestconfig):
    def make(**kw) -> Cluster:
        c = make_cluster(image_override=old_image, **kw)
        return c

    global _image_is_new_build
    if _image_is_new_build is None:
        probe = make_cluster(nodes=1, smp=1)
        _image_is_new_build = has_new_build_features(probe)
        probe.stop()
    if not _image_is_new_build:
        if pytestconfig.getoption("--require-new-build"):
            pytest.fail("--image predates the dedup memory fixes")
        pytest.skip("--image predates the dedup memory fixes")
    return make


def keys_for(p: int, n: int) -> list[bytes]:
    return [f"p{p}-{i:07d}".encode() for i in range(n)]


def resend_is_dropped(
    c: Cluster, t: str, p: int, keys: list[bytes], sample: int = 500
) -> None:
    before = len(kafka.consume_all(c.brokers, t, p))
    step = max(1, len(keys) // sample)
    kafka.produce_ok(
        c.brokers, t, [Rec(k, b"again") for k in keys[::step]], partition=p
    )
    after = len(kafka.consume_all(c.brokers, t, p))
    assert after == before, (
        f"partition {p}: {after - before} re-sent identities admitted"
    )


def test_upgrade_drops_legacy_kvstore_snapshots(upgrade_cluster, image):
    c = upgrade_cluster(nodes=1, smp=1, memory="1G")
    t = kafka.unique("up")
    kafka.create_topic(
        c.brokers,
        t,
        4,
        1,
        {kafka.DEDUP_WINDOW: "3600000", "segment.bytes": str(1024 * 1024)},
    )
    seeded = {p: keys_for(p, 150_000) for p in range(4)}
    for p, ks in seeded.items():
        kafka.produce_bulk(c.brokers, t, p, ks, value=b"x" * 64)

    # The old build keeps every partition's latest dedup snapshot in the
    # kvstore: ~30 bytes an entry. It writes them in the background, so the
    # last partitions' may land after the produce returns.
    def _old_kv_full() -> None:
        old_kv = c.node(0).metrics().sum(KVSTORE_BYTES)
        assert old_kv > 5 * 1024 * 1024, f"old build's kvstore held only {old_kv} bytes"

    kafka.eventually(_old_kv_full, timeout_s=60)

    c.node(0).restart(image=image)
    assert "longer kept in the kvstore" in c.node(0).logs()
    m = c.node(0).metrics()
    assert m.sum(KVSTORE_BYTES) < 256 * 1024, m.sum(KVSTORE_BYTES)
    kafka.eventually(
        lambda: _assert_entries(c, t, 600_000), timeout_s=180, msg="index not rebuilt"
    )
    for p, ks in seeded.items():
        resend_is_dropped(c, t, p, ks)

    # A second start reads the slimmed kvstore snapshot and the new dedup
    # snapshot files.
    kafka.produce_bulk(
        c.brokers, t, 0, (f"more-{i}".encode() for i in range(20_000)), value=b"y" * 200
    )
    c.node(0).restart()
    assert c.node(0).metrics().sum(KVSTORE_BYTES) < 256 * 1024
    for p, ks in seeded.items():
        resend_is_dropped(c, t, p, ks)


def _assert_entries(c: Cluster, t: str, want: int) -> None:
    got = c.node(0).metrics().sum(PART_ENTRIES, topic=t)
    assert got >= want, (got, want)


def test_upgrade_into_less_memory(upgrade_cluster, image):
    """The upgraded broker gets half the memory the old one built its state
    with: kvstore recovery must not need the legacy snapshots in memory, and
    the rebuilt indices must fit the smaller shard."""
    c = upgrade_cluster(nodes=1, smp=1, memory="1G")
    t = kafka.unique("upmem")
    kafka.create_topic(
        c.brokers,
        t,
        4,
        1,
        {kafka.DEDUP_WINDOW: "3600000", "segment.bytes": str(1024 * 1024)},
    )
    for p in range(4):
        kafka.produce_bulk(c.brokers, t, p, keys_for(p, 250_000), value=b"x" * 32)
    c.node(0).restart(image=image, memory="512M")
    m = c.node(0).metrics()
    assert m.sum(KVSTORE_BYTES) < 256 * 1024
    for labels, entries in m.select("vectorized_dedup_shard_index_entries"):
        limit = m.sum("vectorized_dedup_shard_max_index_entries", shard=labels["shard"])
        assert entries <= limit
    assert_serving(c, t, 4)


def test_rolling_upgrade(upgrade_cluster, image):
    c = upgrade_cluster(nodes=3, smp=1, memory="1G")
    t = kafka.unique("roll")
    kafka.create_topic(c.brokers, t, 3, 3, {kafka.DEDUP_WINDOW: "3600000"})
    seeded = {p: keys_for(p, 20_000) for p in range(3)}
    for p, ks in seeded.items():
        kafka.produce_bulk(c.brokers, t, p, ks)

    for node in (2, 1, 0):
        c.node(node).restart(image=image)
        c.wait_healthy()
        # Lead every partition from the node just upgraded, then from one
        # still on the old build (while there is one).
        for p in range(3):
            c.transfer_leadership(t, p, node)
            resend_is_dropped(c, t, p, seeded[p])
            old = next((n for n in (0, 1) if n < node), None)
            if old is not None:
                c.transfer_leadership(t, p, old)
                resend_is_dropped(c, t, p, seeded[p])
        # New identities written in the mixed cluster are deduplicated too.
        for p in range(3):
            fresh = [f"mixed-{node}-{p}-{i}".encode() for i in range(100)]
            kafka.produce_ok(c.brokers, t, [Rec(k) for k in fresh], partition=p)
            seeded[p] += fresh
            resend_is_dropped(c, t, p, fresh, sample=100)

    for n in c.nodes:
        assert n.metrics().sum(KVSTORE_BYTES) < 256 * 1024, n.node_id
    assert_serving(c, t, 3)


def test_raft_snapshot_from_old_leader_to_new_follower(upgrade_cluster, image):
    """A follower on the new build that needs a raft snapshot gets the old
    version 1 format, which it cannot read: it must discard it and keep
    serving, not fail the partition. Identities that existed only in that
    snapshot are lost on that replica -- the documented cost of the format
    change -- so the check is availability plus new identities."""
    c = upgrade_cluster(nodes=3, smp=1, memory="1G")
    t = kafka.unique("rsnap")
    kafka.create_topic(
        c.brokers,
        t,
        1,
        3,
        {kafka.DEDUP_WINDOW: "3600000", "segment.bytes": str(1024 * 1024)},
    )
    leader = c.leader(t, 0)
    follower = next(r["node_id"] for r in c.replicas(t, 0) if r["node_id"] != leader)
    c.node(follower).stop()
    kafka.produce_bulk(
        c.brokers, t, 0, (f"k{i}".encode() for i in range(5000)), value=b"x" * 1024
    )
    hw = len(kafka.consume_all(c.brokers, t))
    kafka.delete_records(c.brokers, t, 0, hw)
    c.node(follower).spec.image = image
    c.node(follower).start()
    c.wait_healthy()
    assert c.node(follower).is_running()
    c.transfer_leadership(t, 0, follower)
    kafka.produce_ok(c.brokers, t, [Rec(b"after", b"1")])
    kafka.produce_ok(c.brokers, t, [Rec(b"after", b"2")])
    assert kafka.keys_in(c.brokers, t) == [b"after"]


def test_raft_snapshot_from_new_leader_to_old_follower(upgrade_cluster, image):
    """The reverse: an old follower receives the new version 2 format. The
    old build discards a snapshot it cannot read; it must not crash."""
    c = upgrade_cluster(nodes=3, smp=1, memory="1G")
    t = kafka.unique("rsnap2")
    kafka.create_topic(
        c.brokers,
        t,
        1,
        3,
        {kafka.DEDUP_WINDOW: "3600000", "segment.bytes": str(1024 * 1024)},
    )
    leader = c.leader(t, 0)
    follower = next(r["node_id"] for r in c.replicas(t, 0) if r["node_id"] != leader)
    c.node(leader).restart(image=image)
    c.wait_healthy()
    c.transfer_leadership(t, 0, leader)
    c.node(follower).stop()
    kafka.produce_bulk(
        c.brokers, t, 0, (f"k{i}".encode() for i in range(5000)), value=b"x" * 1024
    )
    hw = len(kafka.consume_all(c.brokers, t))
    kafka.delete_records(c.brokers, t, 0, hw)
    c.node(follower).start()
    c.wait_healthy()
    assert c.node(follower).is_running()
    c.transfer_leadership(t, 0, follower)
    kafka.produce_ok(c.brokers, t, [Rec(b"after", b"1")])
    kafka.produce_ok(c.brokers, t, [Rec(b"after", b"2")])
    assert kafka.keys_in(c.brokers, t) == [b"after"]


def test_downgrade_and_upgrade_again(upgrade_cluster, image, old_image):
    c = upgrade_cluster(nodes=1, smp=1, memory="1G")
    t = kafka.unique("down")
    kafka.create_topic(
        c.brokers,
        t,
        2,
        1,
        {kafka.DEDUP_WINDOW: "3600000", "segment.bytes": str(1024 * 1024)},
    )
    seeded = {p: keys_for(p, 30_000) for p in range(2)}
    for p, ks in seeded.items():
        kafka.produce_bulk(c.brokers, t, p, ks, value=b"x" * 100)

    c.node(0).restart(image=image)
    for p, ks in seeded.items():
        resend_is_dropped(c, t, p, ks)
    # Back to the old build: it finds no dedup snapshot in the kvstore (the
    # new build removed them) and rebuilds from the log.
    c.node(0).restart(image=old_image)
    for p, ks in seeded.items():
        resend_is_dropped(c, t, p, ks)
    fresh = [f"old-again-{i}".encode() for i in range(100)]
    kafka.produce_ok(c.brokers, t, [Rec(k) for k in fresh], partition=0)
    # And forward once more.
    c.node(0).restart(image=image)
    resend_is_dropped(c, t, 0, fresh, sample=100)
    for p, ks in seeded.items():
        resend_is_dropped(c, t, p, ks)
    assert c.node(0).metrics().sum(KVSTORE_BYTES) < 256 * 1024
