"""Sustained and adversarial load on the dedup index.

Every scenario checks the dedup invariant it stresses -- no identity admitted
twice within the window, no acknowledged record lost -- and that the broker
stayed within its memory: no allocation failed, and the lowest available
memory any shard reached stayed above a floor. Allocation failures are what
took the staging broker down, so they are asserted directly rather than
inferred from the broker staying up.

DEDUP_STRESS_SCALE (default 1) multiplies record counts and durations; the
default keeps each scenario to a few minutes.
"""

from __future__ import annotations

import os
import random
import statistics
import threading
import time
from collections import Counter

import pytest
from confluent_kafka import Producer
from harness import kafka
from harness.cluster import Cluster
from harness.kafka import Rec
from test_capacity import (
    EVICTED,
    PART_ENTRIES,
    RECLAIMED,
    SHARD_ENTRIES,
    SHARD_MAX,
    UNINDEXED,
    admitted_on_resend,
    require_new_build,
    require_reclaim,
    topic_on_one_node,
)

pytestmark = [pytest.mark.slow, pytest.mark.stress]

SCALE = float(os.environ.get("DEDUP_STRESS_SCALE", "1"))

MALLOC_FAILED = "vectorized_memory_malloc_failed"
TOTAL_MEMORY = "vectorized_memory_total_memory"
LOW_WATER = "vectorized_memory_available_memory_low_water_mark"


def scaled(n: float) -> int:
    return max(1, int(n * SCALE))


def assert_memory_healthy(c: Cluster, min_available_fraction: float = 0.10) -> None:
    """No allocation has failed on any shard of any running node, and no
    shard's available memory has dipped below the floor since it started."""
    for node_id, m in c.metrics().items():
        failed = m.sum(MALLOC_FAILED)
        assert failed == 0, f"node {node_id}: {failed} allocations failed"
        for labels, total in m.select(TOTAL_MEMORY):
            low = m.sum(LOW_WATER, shard=labels["shard"])
            assert low >= min_available_fraction * total, (
                f"node {node_id} shard {labels['shard']}: available memory fell to "
                f"{low / total:.1%} of {total / 2**20:.0f} MiB"
            )


def assert_shards_within_budget(c: Cluster) -> None:
    """Where the build has a shard budget (older builds do not), no shard is
    over it."""
    for node_id, m in c.metrics().items():
        limits = {l["shard"]: v for l, v in m.select(SHARD_MAX)}
        for l, v in m.select(SHARD_ENTRIES):
            assert v <= limits[l["shard"]], (node_id, l["shard"], v, limits[l["shard"]])


def assert_each_key_once(
    c: Cluster, t: str, partitions: int, prefix: bytes = b""
) -> int:
    total = 0
    for p in range(partitions):
        keys = [
            k
            for k in kafka.keys_in(c.brokers, t, p, timeout_s=300)
            if k.startswith(prefix)
        ]
        dupes = [k for k, n in Counter(keys).items() if n > 1]
        assert not dupes, (
            f"partition {p}: {len(dupes)} identities admitted twice, e.g. {dupes[:3]}"
        )
        total += len(keys)
    return total


def run_threads(targets) -> None:
    errors: list[BaseException] = []

    def wrap(fn):
        def run():
            try:
                fn()
            except BaseException as e:  # noqa: BLE001 -- re-raised below
                errors.append(e)

        return run

    threads = [threading.Thread(target=wrap(fn)) for fn in targets]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    if errors:
        raise errors[0]


# ---------------------------------------------------------------------------


def test_sustained_duplicate_heavy_load(make_cluster, pytestconfig):
    """Four producers hammer eight replicated partitions with overlapping key
    ranges, so about half of everything sent is a duplicate of something
    another producer sent concurrently."""
    c = make_cluster(nodes=3, smp=2, memory="1G")
    t = kafka.unique("load")
    kafka.create_topic(c.brokers, t, 8, 3, {kafka.DEDUP_WINDOW: "3600000"})
    per_producer = scaled(120_000)
    keyspace = per_producer * 2  # 4 producers x per_producer over 2x keys

    def producer(seed: int):
        rng = random.Random(seed)
        start = rng.randrange(keyspace)
        p = kafka.producer(c.brokers, **{"linger.ms": 20})
        errors = []
        for i in range(per_producer):
            k = (start + i * 7) % keyspace
            while True:
                try:
                    p.produce(
                        t,
                        key=f"id-{k}".encode(),
                        value=b"x" * 64,
                        partition=k % 8,
                        on_delivery=lambda e, _m: e and errors.append(e),
                    )
                    break
                except BufferError:
                    # The client's own queue is full: the broker is behind
                    # this producer for a moment. Serve deliveries until
                    # there is room, then send the same record again.
                    p.poll(0.1)
            if i % 5000 == 0:
                p.poll(0)
        assert p.flush(300) == 0
        assert not errors, errors[:3]

    started = time.monotonic()
    run_threads([lambda s=s: producer(s) for s in range(4)])
    elapsed = time.monotonic() - started
    print(
        f"\n{4 * per_producer} records in {elapsed:.1f}s ({4 * per_producer / elapsed:.0f}/s)"
    )

    admitted = assert_each_key_once(c, t, 8)
    assert admitted <= keyspace
    assert_shards_within_budget(c)
    assert_memory_healthy(c)


def test_snapshot_storm_near_the_shard_limit(make_cluster, pytestconfig):
    """Tiny segments make every roll snapshot the index, on a shard filled
    past its budget, in little memory: the shape of the staging incident."""
    c = make_cluster(nodes=1, smp=1, memory="768M")
    require_new_build(c, pytestconfig)
    limit = int(c.node(0).metrics().sum(SHARD_MAX))
    t = topic_on_one_node(c, 8, extra={"segment.bytes": str(1024 * 1024)})
    per = int(limit * 1.5 / 8 * SCALE)
    run_threads(
        [
            lambda p=p: kafka.produce_bulk(
                c.brokers,
                t,
                p,
                (f"p{p}-{i}".encode() for i in range(per)),
                value=b"s" * 256,
            )
            for p in range(8)
        ]
    )
    assert_shards_within_budget(c)
    assert_memory_healthy(c, min_available_fraction=0.15)
    # The node survives a restart from all those snapshots too.
    c.node(0).restart()
    assert_shards_within_budget(c)
    assert_memory_healthy(c, min_available_fraction=0.15)


def test_chaos_loses_nothing_and_duplicates_nothing(make_cluster, pytestconfig):
    """Producers with retries run while nodes are killed, restarted, frozen
    and stripped of leadership. Every acknowledged record must be readable,
    and no identity may appear twice: a retry of a write that did land is
    exactly what dedup must absorb."""
    run_chaos(make_cluster, feeds=1)


def test_chaos_with_redundant_feeds(make_cluster, pytestconfig):
    """The chaos above with two producers sending the same identities in the
    same order, as redundant feeds of one stream do: about half of what
    arrives duplicates a record still being replicated, so requests depend on
    other requests' outcomes while leaders move and nodes die. Every record
    either feed had acknowledged must be readable, and none written twice."""
    run_chaos(make_cluster, feeds=2)


def run_chaos(make_cluster, feeds: int) -> None:
    # Producers run for as long as the chaos does, so the index grows with
    # SCALE; past the per-partition cap it would admit duplicates unindexed.
    c = make_cluster(
        nodes=3,
        smp=1,
        memory="1G",
        cluster_config={"dedup_max_entries_per_partition": scaled(2_000_000)},
    )
    t = kafka.unique("chaos")
    partitions = 3
    kafka.create_topic(c.brokers, t, partitions, 3, {kafka.DEDUP_WINDOW: "3600000"})
    stop = threading.Event()
    acked: list[bytes] = []
    failed: list = []
    lock = threading.Lock()

    def producer():
        p = Producer(
            {
                "bootstrap.servers": ",".join(n.kafka_addr for n in c.nodes),
                "enable.idempotence": False,
                "acks": "all",
                "retries": 1_000_000,
                "retry.backoff.ms": 250,
                "message.timeout.ms": 300_000,
                "request.timeout.ms": 10_000,
                "linger.ms": 5,
            }
        )

        def cb(err, msg):
            with lock:
                if err is None:
                    acked.append(msg.key())
                else:
                    failed.append(err)

        i = 0
        while not stop.is_set():
            k = f"c-{i}".encode()
            try:
                p.produce(
                    t, key=k, value=b"v", partition=i % partitions, on_delivery=cb
                )
            except BufferError:
                # The cluster is down or electing: the client's queue is full
                # of records waiting for a leader. Back off, keep the key.
                p.poll(0.5)
                continue
            i += 1
            if i % 100 == 0:
                p.poll(0)
                time.sleep(0.005)
        assert p.flush(300) == 0

    def chaos():
        rng = random.Random(1234)
        for _ in range(scaled(8)):
            time.sleep(rng.uniform(2, 6))
            victim = c.node(rng.randrange(3))
            action = rng.choice(["kill", "restart", "pause", "transfer"])
            if action == "kill":
                victim.kill()
                time.sleep(rng.uniform(1, 4))
                victim.start()
            elif action == "restart":
                victim.restart()
            elif action == "pause":
                victim.pause()
                time.sleep(rng.uniform(3, 8))
                victim.unpause()
            else:
                p = rng.randrange(partitions)
                try:
                    c.transfer_leadership(t, p, victim.node_id)
                except AssertionError:
                    pass  # the target may be catching up; chaos goes on
            c.wait_healthy(timeout_s=180)
        stop.set()

    run_threads([producer] * feeds + [chaos])
    assert not failed, f"{len(failed)} records failed for good: {failed[:3]}"
    assert len(acked) > 1000
    present = set()
    for p in range(partitions):
        keys = kafka.keys_in(c.brokers, t, p, timeout_s=300)
        dupes = [k for k, n in Counter(keys).items() if n > 1]
        assert not dupes, (
            f"partition {p}: {len(dupes)} identities written twice, e.g. {dupes[:3]}"
        )
        present.update(keys)
    lost = [k for k in acked if k not in present]
    assert not lost, f"{len(lost)} acknowledged records missing, e.g. {lost[:3]}"
    assert_memory_healthy(c)


def test_redundant_feeds_at_full_speed(make_cluster, pytestconfig):
    """Three producers send the same identities in the same order as fast as
    they can -- two with acks=all, one with acks=1 -- while leadership moves
    between nodes. Nearly every duplicate arrives while the record it repeats
    is still being replicated. Each identity must be written exactly once,
    and every one an acks=all producer had acknowledged must be readable."""
    c = make_cluster(nodes=3, smp=2, memory="1G")
    t = kafka.unique("feeds")
    partitions = 8
    kafka.create_topic(c.brokers, t, partitions, 3, {kafka.DEDUP_WINDOW: "3600000"})
    kafka.wait_for_leaders(c.brokers, t, partitions)
    n = scaled(150_000)
    acked_all: list[bytes] = []
    failed: list = []
    lock = threading.Lock()
    done = threading.Event()

    def feed(acks: str):
        p = kafka.producer(
            c.brokers,
            acks=acks,
            **{
                "linger.ms": 2,
                "retries": 1_000_000,
                "message.timeout.ms": 300_000,
                "queue.buffering.max.messages": 200_000,
            },
        )

        def cb(err, msg):
            with lock:
                if err is not None:
                    failed.append(err)
                elif acks == "all":
                    acked_all.append(msg.key())

        for i in range(n):
            while True:
                try:
                    p.produce(
                        t,
                        key=f"f-{i}".encode(),
                        value=b"v" * 64,
                        partition=i % partitions,
                        on_delivery=cb,
                    )
                    break
                except BufferError:
                    p.poll(0.05)
            if i % 2000 == 0:
                p.poll(0)
        assert p.flush(300) == 0

    def feeds():
        try:
            run_threads([lambda: feed("all"), lambda: feed("all"), lambda: feed("1")])
        finally:
            done.set()

    def move_leaders():
        rng = random.Random(7)
        while not done.wait(rng.uniform(1, 3)):
            part = rng.randrange(partitions)
            try:
                c.transfer_leadership(t, part, rng.randrange(3))
            except AssertionError:
                pass  # already the leader, or the target is catching up

    run_threads([feeds, move_leaders])
    assert not failed, f"{len(failed)} records failed for good: {failed[:3]}"
    admitted = assert_each_key_once(c, t, partitions, prefix=b"f-")
    present = set()
    for part in range(partitions):
        present.update(kafka.keys_in(c.brokers, t, part, timeout_s=300))
    lost = {k for k in acked_all if k not in present}
    assert not lost, f"{len(lost)} acknowledged records missing, e.g. {list(lost)[:3]}"
    assert admitted == n, f"{admitted} identities written, {n} sent"
    assert_memory_healthy(c)


def test_many_partitions_far_over_the_shard_limit(make_cluster, pytestconfig):
    """32 partitions sending twelve times what the shard may index: the shard
    stays at its limit, produce stays available, and every partition that is
    still busy keeps coverage for its newest identities."""
    limit = 50_000
    c = make_cluster(
        nodes=1,
        smp=1,
        memory="1G",
        cluster_config={"dedup_max_entries_per_shard": limit},
    )
    require_new_build(c, pytestconfig)
    t = topic_on_one_node(c, 32)
    per = scaled(limit * 12 // 32)
    now = int(time.time() * 1000)
    span = 3_000_000
    run_threads(
        [
            lambda p=p: kafka.produce_bulk(
                c.brokers,
                t,
                p,
                (f"p{p}-{i}".encode() for i in range(per)),
                timestamp_ms=lambda i: now - span + i * span // per,
            )
            for p in range(32)
        ]
    )
    m = c.node(0).metrics()
    assert m.sum(SHARD_ENTRIES) <= limit
    for p in range(32):
        assert m.sum(PART_ENTRIES, topic=t, partition=str(p)) > 0, (
            f"partition {p} has no index"
        )
    # Newest identities of every partition: (almost all) still deduplicated.
    later = int(time.time() * 1000)
    readmitted = 0
    for p in range(32):
        before = len(kafka.consume_all(c.brokers, t, p))
        newest = [
            Rec(f"p{p}-{i}".encode(), b"d", timestamp_ms=later)
            for i in range(per - 20, per)
        ]
        kafka.produce_ok(c.brokers, t, newest, partition=p)
        readmitted += len(kafka.consume_all(c.brokers, t, p)) - before
    assert readmitted <= 32 * 20 * 0.1, (
        f"{readmitted} of the newest {32 * 20} were forgotten"
    )
    assert_memory_healthy(c)


def test_crash_restart_loop_with_a_full_index(make_cluster, pytestconfig):
    c = make_cluster(nodes=1, smp=1, memory="512M")
    require_new_build(c, pytestconfig)
    limit = int(c.node(0).metrics().sum(SHARD_MAX))
    t = topic_on_one_node(c, 4, extra={"segment.bytes": str(2 * 1024 * 1024)})
    for p in range(4):
        kafka.produce_bulk(
            c.brokers, t, p, (f"p{p}-{i}".encode() for i in range(limit // 3))
        )
    for cycle in range(scaled(8)):
        started = time.monotonic()
        if cycle % 2 == 0:
            c.node(0).kill()
            c.node(0).start()
        else:
            c.node(0).restart()
        took = time.monotonic() - started
        assert took < 120, f"cycle {cycle}: start took {took:.0f}s"
        assert_shards_within_budget(c)
        assert_memory_healthy(c, min_available_fraction=0.15)
        fresh = [Rec(f"cycle{cycle}-{i}".encode()) for i in range(1000)]
        kafka.produce_ok(c.brokers, t, fresh, partition=cycle % 4)
        kafka.produce_ok(c.brokers, t, fresh, partition=cycle % 4)
    for p in range(4):
        keys = [k for k in kafka.keys_in(c.brokers, t, p) if k.startswith(b"cycle")]
        assert len(keys) == len(set(keys)), f"partition {p}"


def test_frozen_follower_catches_up_in_a_burst(make_cluster, pytestconfig):
    """A follower frozen while hundreds of thousands of identities arrive
    applies them all at once when it thaws; it must stay within its memory
    and end up with the leader's index."""
    c = make_cluster(nodes=3, smp=1, memory="768M")
    t = kafka.unique("thaw")
    kafka.create_topic(c.brokers, t, 2, 3, {kafka.DEDUP_WINDOW: "3600000"})
    leader = c.leader(t, 0)
    follower = next(r["node_id"] for r in c.replicas(t, 0) if r["node_id"] != leader)
    c.node(follower).pause()
    try:
        per = scaled(250_000)
        run_threads(
            [
                lambda p=p: kafka.produce_bulk(
                    c.brokers,
                    t,
                    p,
                    (f"p{p}-{i}".encode() for i in range(per)),
                    value=b"b" * 32,
                )
                for p in range(2)
            ]
        )
    finally:
        c.node(follower).unpause()
    c.wait_healthy(timeout_s=300)

    def caught_up():
        lm = c.node(leader).metrics().sum(PART_ENTRIES, topic=t)
        fm = c.node(follower).metrics().sum(PART_ENTRIES, topic=t)
        assert fm == lm, (fm, lm)

    kafka.eventually(caught_up, timeout_s=300)
    assert_memory_healthy(c)
    for p in range(2):
        c.transfer_leadership(t, p, follower)
        before = len(kafka.consume_all(c.brokers, t, p))
        kafka.produce_ok(
            c.brokers,
            t,
            [Rec(f"p{p}-{i}".encode()) for i in range(0, per, per // 200)],
            partition=p,
        )
        assert len(kafka.consume_all(c.brokers, t, p)) == before


def test_mixed_workloads_on_one_shard(make_cluster, pytestconfig):
    """Key-mode and header-mode dedup topics, a plain topic, an idempotent
    producer and a transactional producer, all at once on one shard: each
    keeps its own semantics."""
    c = make_cluster(nodes=1, smp=1, memory="1G")
    by_key = topic_on_one_node(c, 2)
    by_header = topic_on_one_node(c, 2)
    kafka.set_topic_config(c.brokers, by_header, {kafka.DEDUP_KEY_HEADER: "x-id"})
    plain = kafka.unique("plain")
    kafka.create_topic(c.brokers, plain, 2, 1)
    n = scaled(40_000)

    def key_mode():
        for _ in range(2):
            kafka.produce_bulk(
                c.brokers, by_key, 0, (f"k{i}".encode() for i in range(n))
            )

    def header_mode():
        p = kafka.producer(c.brokers)
        for _ in range(2):
            for i in range(n):
                p.produce(
                    by_header,
                    key=b"same",
                    value=b"h",
                    headers=[("x-id", f"h{i}".encode())],
                    partition=0,
                )
                if i % 5000 == 0:
                    p.poll(0)
        assert p.flush(300) == 0

    def plain_mode():
        for _ in range(2):
            kafka.produce_bulk(
                c.brokers, plain, 0, (f"k{i}".encode() for i in range(n))
            )

    def idempotent():
        for _ in range(2):
            kafka.produce_bulk(
                c.brokers,
                by_key,
                1,
                (f"i{i}".encode() for i in range(n)),
                **{"enable.idempotence": True},
            )

    def transactional():
        p = Producer(
            {"bootstrap.servers": c.brokers, "transactional.id": kafka.unique("tx")}
        )
        p.init_transactions(30)
        for round_ in range(4):
            p.begin_transaction()
            for i in range(n // 4):
                p.produce(
                    by_header,
                    key=b"tx",
                    value=b"t",
                    headers=[("x-id", b"t")],
                    partition=1,
                )
                if i % 5000 == 0:
                    p.poll(0)
            if round_ == 3:
                p.abort_transaction(60)
            else:
                p.commit_transaction(60)

    run_threads([key_mode, header_mode, plain_mode, idempotent, transactional])
    assert len(kafka.consume_all(c.brokers, by_key, 0)) == n
    assert len(kafka.consume_all(c.brokers, by_header, 0)) == n
    assert len(kafka.consume_all(c.brokers, plain, 0)) == 2 * n
    # Idempotent produce bypasses dedup: both rounds are there.
    assert len(kafka.consume_all(c.brokers, by_key, 1)) == 2 * n
    committed = kafka.consume_all(c.brokers, by_header, 1, isolation="read_committed")
    assert len(committed) == 3 * (n // 4)
    assert_shards_within_budget(c)
    assert_memory_healthy(c)


def test_topic_churn_returns_the_budget(make_cluster, pytestconfig):
    """Dedup topics created, filled and deleted over and over: every deleted
    partition gives its entries back, so the shard's count returns to what
    the surviving topics hold and the budget cannot leak.

    Allocated memory is judged by how much it grows a round once the broker
    has warmed up: its own caches grow by a couple of hundred MiB in one step,
    with or without dedup, sometimes only after the warm-up rounds. A leaked
    index would instead grow every round, by several MiB at this size, so
    the median round's growth catches it while one step does not."""
    c = make_cluster(nodes=1, smp=1, memory="1G")
    require_new_build(c, pytestconfig)
    keeper = topic_on_one_node(c, 1)
    kafka.produce_bulk(
        c.brokers, keeper, 0, (f"keep-{i}".encode() for i in range(5000))
    )
    per = 50_000
    warmup = scaled(20)
    measured = scaled(8)
    allocs: list[float] = []
    for round_ in range(warmup + measured):
        t = topic_on_one_node(c, 2)
        for p in range(2):
            kafka.produce_bulk(
                c.brokers, t, p, (f"r{round_}-{i}".encode() for i in range(per))
            )
        assert c.node(0).metrics().sum(SHARD_ENTRIES) >= 2 * per
        kafka.delete_topic(c.brokers, t)

        def released():
            got = c.node(0).metrics().sum(SHARD_ENTRIES)
            assert got == 5000, got

        kafka.eventually(released, timeout_s=60)
        if round_ >= warmup - 1:
            allocs.append(c.node(0).metrics().sum("vectorized_memory_allocated_memory"))
    growth = [b - a for a, b in zip(allocs, allocs[1:])]
    assert statistics.median(growth) < 2**20, (
        f"allocated memory grew every round: {[int(g) for g in growth]} bytes"
    )
    assert_memory_healthy(c)


def test_huge_header_identities_cost_the_same_as_small_ones(make_cluster, pytestconfig):
    """The index stores a digest, so 4 KiB identities must not make the index
    any larger than 8-byte ones."""
    # The batch cache would hold the 4 KiB identities themselves, as much of
    # them as memory allows, so with it on the growth says nothing about the
    # index.
    c = make_cluster(
        nodes=1, smp=1, memory="1G", cluster_config={"disable_batch_cache": True}
    )
    t = topic_on_one_node(c, 1, extra={"max.message.bytes": str(8 * 1024 * 1024)})
    kafka.set_topic_config(c.brokers, t, {kafka.DEDUP_KEY_HEADER: "x-id"})
    n = scaled(60_000)
    before = c.node(0).metrics().sum("vectorized_memory_allocated_memory")
    pad = b"z" * 4096
    p = kafka.producer(c.brokers, **{"message.max.bytes": 8_000_000})
    for round_ in range(2):
        for i in range(n):
            while True:
                try:
                    p.produce(
                        t,
                        key=None,
                        value=b"v",
                        headers=[("x-id", pad + str(i).encode())],
                        partition=0,
                    )
                    break
                except BufferError:
                    # 4 KiB a record fills the client's queue well before the
                    # broker has taken a round; drain and go on.
                    p.poll(0.1)
            if i % 2000 == 0:
                p.poll(0)
    assert p.flush(300) == 0
    assert len(kafka.consume_all(c.brokers, t)) == n
    m = c.node(0).metrics()
    assert m.sum(PART_ENTRIES, topic=t) == n
    # The index is well under 200 bytes an entry however large the identity;
    # keeping the identities themselves would cost over 4 KiB an entry.
    grown = m.sum("vectorized_memory_allocated_memory") - before
    assert grown < n * 200 + 32 * 2**20, grown
    assert_memory_healthy(c)


def test_idle_partitions_holding_the_budget_do_not_starve_a_busy_one(
    make_cluster, pytestconfig
):
    limit = 20_000
    c = make_cluster(
        nodes=1, smp=1, cluster_config={"dedup_max_entries_per_shard": limit}
    )
    require_new_build(c, pytestconfig)
    t = topic_on_one_node(c, 5)
    now = int(time.time() * 1000)
    for p in range(4):
        kafka.produce_bulk(
            c.brokers,
            t,
            p,
            (f"idle{p}-{i}".encode() for i in range(limit // 4)),
            timestamp_ms=lambda i: now - 3_000_000 + i * 500,
        )
    # Shard pressure starts trimming at 90% of the limit, so four equal
    # partitions settle just short of it rather than exactly on it.
    filled = c.node(0).metrics().sum(SHARD_ENTRIES)
    assert limit * 9 // 10 <= filled <= limit
    busy = scaled(50_000)
    kafka.produce_bulk(
        c.brokers,
        t,
        4,
        (f"busy-{i}".encode() for i in range(busy)),
        timestamp_ms=lambda i: now - 1_000_000 + i * (1_000_000 // busy),
    )
    m = c.node(0).metrics()
    assert m.sum(SHARD_ENTRIES) <= limit
    assert m.sum(PART_ENTRIES, topic=t, partition="4") > limit // 10
    before = len(kafka.consume_all(c.brokers, t, 4))
    later = int(time.time() * 1000)
    kafka.produce_ok(
        c.brokers,
        t,
        [
            Rec(f"busy-{i}".encode(), timestamp_ms=later)
            for i in range(busy - 1000, busy)
        ],
        partition=4,
    )
    readmitted = len(kafka.consume_all(c.brokers, t, 4)) - before
    assert readmitted <= 100, (
        f"{readmitted} of the busy partition's newest 1000 forgotten"
    )


def _readmitted(c: Cluster, t: str, partition: int, keys: list[bytes]) -> int:
    """How many of `keys`, sent again now, the partition admits."""
    before = len(kafka.consume_all(c.brokers, t, partition))
    later = int(time.time() * 1000)
    kafka.produce_ok(
        c.brokers, t, [Rec(k, timestamp_ms=later) for k in keys], partition=partition
    )
    return len(kafka.consume_all(c.brokers, t, partition)) - before


def test_many_small_idle_partitions_do_not_starve_a_busy_one(
    make_cluster, pytestconfig
):
    """Twenty idle partitions fill the shard between them, none holding more
    than a twentieth of it. A partition that then turns busy must take room
    from them and grow, rather than count as one of the largest because they
    are all small and so only ever trim its own few entries."""
    limit = 20_000
    idle = 20
    c = make_cluster(
        nodes=1, smp=1, cluster_config={"dedup_max_entries_per_shard": limit}
    )
    require_new_build(c, pytestconfig)
    t = topic_on_one_node(c, idle + 1)
    now = int(time.time() * 1000)
    for p in range(idle):
        kafka.produce_bulk(
            c.brokers,
            t,
            p,
            (f"idle{p}-{i}".encode() for i in range(limit // idle)),
            timestamp_ms=lambda i, p=p: now - 3_000_000 + p * 10_000 + i,
        )
    assert c.node(0).metrics().sum(SHARD_ENTRIES) >= limit * 9 // 10
    busy = scaled(20_000)
    kafka.produce_bulk(
        c.brokers,
        t,
        idle,
        (f"busy-{i}".encode() for i in range(busy)),
        timestamp_ms=lambda i: now - 1_000_000 + i * (1_000_000 // busy),
    )
    m = c.node(0).metrics()
    assert m.sum(SHARD_ENTRIES) <= limit
    # A twentieth of the shard is what each idle partition held. The busy one
    # takes room from them until it is within a tenth of their size, where it
    # counts as one of the largest and trims itself instead: parity with the
    # idle partitions, less that margin, is the designed steady state. Whether
    # it is among the largest is decided once per cursor step, so the step
    # that crosses the margin can trim up to a step's worth below it -- at
    # most the 64 entries of the step taken at the ceiling
    # (dedup_window_filter::capacity_emergency_budget).
    share = limit // idle
    step = 64
    assert (
        m.sum(PART_ENTRIES, topic=t, partition=str(idle)) >= share - share // 10 - step
    )
    newest = [f"busy-{i}".encode() for i in range(busy - 500, busy)]
    assert _readmitted(c, t, idle, newest) == 0


def test_full_shard_keeps_the_newest_identities(make_cluster, pytestconfig):
    """A partition producing far more identities than the shard can hold, next
    to an idle one holding half of it: the shard stays at its limit the whole
    run, and the newest identities are always the ones kept -- none of them is
    admitted unindexed, and resending them admits nothing."""
    limit = 20_000
    c = make_cluster(
        nodes=1, smp=1, cluster_config={"dedup_max_entries_per_shard": limit}
    )
    require_new_build(c, pytestconfig)
    t = topic_on_one_node(c, 2)
    now = int(time.time() * 1000)
    kafka.produce_bulk(
        c.brokers,
        t,
        0,
        (f"idle-{i}".encode() for i in range(limit // 2)),
        timestamp_ms=lambda i: now - 3_000_000 + i,
    )
    total = scaled(200_000)
    kafka.produce_bulk(
        c.brokers,
        t,
        1,
        (f"busy-{i}".encode() for i in range(total)),
        timestamp_ms=lambda i: now - 2_000_000 + i * (2_000_000 // total),
    )
    m = c.node(0).metrics()
    assert m.sum(SHARD_ENTRIES) <= limit
    assert m.sum(UNINDEXED, topic=t, partition="1") == 0
    newest = [f"busy-{i}".encode() for i in range(total - 2_000, total)]
    assert _readmitted(c, t, 1, newest) == 0


def test_a_burst_gone_quiet_does_not_cost_busy_partitions_their_window(
    make_cluster, pytestconfig
):
    """The staging shard: one large busy feed, two smaller ones, and a burst
    that fills a partition for a minute and then goes quiet, together over
    the shard's soft limit for as long as the burst counts. Ten minutes of
    traffic, two-minute windows.

    The burst's entries stop ageing by their own partition's clock the moment
    it goes quiet; they age with the shard's newest records instead, so they
    are reclaimed once a window has passed. From then on the busy feeds alone
    stay under the soft limit and nothing of theirs is trimmed: every
    identity they were sent in the last window is still caught. Without
    that, the burst held the shard over its soft limit for good and the
    largest feed lost the oldest part of its window."""
    limit = 46_000
    c = make_cluster(
        nodes=1, smp=1, cluster_config={"dedup_max_entries_per_shard": limit}
    )
    require_new_build(c, pytestconfig)
    window_s = 120
    feeds = {
        "quotes": (topic_on_one_node(c, 1, window_ms=window_s * 1000), 200),
        "trades": (topic_on_one_node(c, 1, window_ms=window_s * 1000), 40),
        "book": (topic_on_one_node(c, 1, window_ms=window_s * 1000), 40),
    }
    burst = topic_on_one_node(c, 1, window_ms=window_s * 1000)
    burst_rate, burst_s = 200, 60
    run_s = 600
    # Record time: ten minutes ending a minute ago, so no record is clamped.
    start = int(time.time() * 1000) - (run_s + 60) * 1000
    sent: dict[str, list[Rec]] = {name: [] for name in feeds}
    p = kafka.producer(c.brokers, **{"queue.buffering.max.messages": 1_000_000})
    # At each checkpoint: what the feeds had lost to trimming, and what the
    # shard and the burst held.
    checkpoints: dict[int, dict[str, float]] = {}

    def checkpoint(second: int) -> None:
        m = c.node(0).metrics()
        checkpoints[second] = {
            "evicted": sum(m.sum(EVICTED, topic=t) for t, _ in feeds.values()),
            "shard": m.sum(SHARD_ENTRIES),
            "burst": m.sum(PART_ENTRIES, topic=burst),
            "burst reclaimed": m.sum(RECLAIMED, topic=burst),
        }

    def produce(topic: str, key: bytes, ts: int) -> None:
        while True:
            try:
                p.produce(topic, key=key, value=b"v", partition=0, timestamp=ts)
                return
            except BufferError:
                p.poll(0.1)

    for second in range(run_s):
        for name, (t, rate) in feeds.items():
            for i in range(rate):
                ts = start + second * 1000 + i * 1000 // rate
                key = f"{name}-{second}-{i}".encode()
                sent[name].append(Rec(key, timestamp_ms=ts))
                produce(t, key, ts)
        if second < burst_s:
            for i in range(burst_rate):
                produce(
                    burst,
                    f"burst-{second}-{i}".encode(),
                    start + second * 1000 + i * 1000 // burst_rate,
                )
        if second == 0:
            assert p.flush(60) == 0
            require_reclaim(c, pytestconfig)
        if second in (burst_s, run_s // 2, run_s - 1):
            # Flushed per second of record time otherwise only in batches, so
            # the broker sees the feeds roughly in step with each other.
            assert p.flush(120) == 0
            checkpoint(second)
        elif second % 10 == 9:
            assert p.flush(120) == 0
    m = c.node(0).metrics()
    unindexed = {name: m.sum(UNINDEXED, topic=t) for name, (t, _) in feeds.items()}
    # Everything each feed was sent in the last window, less five seconds at
    # its old edge, sent again as it was: what a feed trims is its oldest.
    forgotten = {
        name: admitted_on_resend(c, t, 0, sent[name][-(window_s - 5) * rate :])
        for name, (t, rate) in feeds.items()
    }
    report = f"checkpoints={checkpoints} unindexed={unindexed} forgotten={forgotten}"
    print(report)
    assert m.sum(SHARD_ENTRIES) <= limit, report
    # The burst held the shard over its soft limit while it was live, and was
    # reclaimed once it had aged a window.
    assert checkpoints[run_s - 1]["burst reclaimed"] > 0, report
    # From halfway through, long after the burst's last entry was a window
    # old, no feed lost anything more to trimming.
    assert checkpoints[run_s - 1]["evicted"] == checkpoints[run_s // 2]["evicted"], (
        report
    )
    assert not any(unindexed.values()), report
    assert not any(forgotten.values()), report
    assert_memory_healthy(c)


def test_leadership_moves_with_a_large_index_under_load(make_cluster, pytestconfig):
    """Leadership of a partition holding a large index moves to every replica
    in turn while a producer keeps writing, half of it identities indexed
    before the moves. Followers keep their entries unindexed, and a replica
    taking over indexes them all before it filters (see
    dedup_window_filter::defer_lookup()): after every move, everything
    indexed earlier still drops, new identities are admitted once each, and
    the producer waits no longer than a leadership move costs anyway."""
    c = make_cluster(nodes=3, smp=1, memory="2G")
    t = kafka.unique("moves")
    kafka.create_topic(c.brokers, t, 1, 3, {kafka.DEDUP_WINDOW: "3600000"})
    kafka.wait_for_leaders(c.brokers, t, 1)
    # Below the partition's entry ceiling with room for what the producer
    # adds: at the ceiling the oldest entries are trimmed, and their resends
    # rightly admitted.
    n = min(scaled(600_000), 800_000)
    kafka.produce_bulk(
        c.brokers, t, 0, (f"big-{i}".encode() for i in range(n)), value=b"v" * 32
    )
    assert kafka.high_watermark(c.brokers, t, 0) == n

    stop = threading.Event()
    lock = threading.Lock()
    latencies: list[float] = []
    errors: list = []
    fresh_sent = 0

    def steady():
        nonlocal fresh_sent
        p = kafka.producer(
            c.brokers,
            **{
                "linger.ms": 0,
                "retries": 1_000_000,
                "retry.backoff.ms": 100,
                "message.timeout.ms": 120_000,
            },
        )

        def delivered(sent: float):
            def cb(err, _msg):
                with lock:
                    if err is None:
                        latencies.append(time.monotonic() - sent)
                    else:
                        errors.append(err)

            return cb

        i = 0
        while not stop.is_set():
            sent = time.monotonic()
            old = f"big-{(i * 7919) % n}".encode()
            p.produce(
                t, key=old, value=b"again", partition=0, on_delivery=delivered(sent)
            )
            p.produce(
                t,
                key=f"fresh-{i}".encode(),
                value=b"v",
                partition=0,
                on_delivery=delivered(sent),
            )
            i += 1
            p.poll(0.005)
        assert p.flush(120) == 0
        fresh_sent = i

    moves = 0

    def mover():
        nonlocal moves
        time.sleep(3)
        for _ in range(2):
            for target in range(3):
                if c.leader(t, 0) == target:
                    continue
                c.transfer_leadership(t, 0, target)
                moves += 1
                time.sleep(4)
        stop.set()

    before = {i: c.node(i).metrics() for i in range(3)}
    run_threads([steady, mover])
    assert not errors, f"{len(errors)} records failed, e.g. {errors[:3]}"
    assert moves >= 3

    # Every identity from before the moves was dropped when sent again, and
    # every fresh one was written once: only the fresh ones moved the log.
    written = kafka.high_watermark(c.brokers, t, 0)
    if written != n + fresh_sent:
        counts = Counter(kafka.keys_in(c.brokers, t, 0, timeout_s=600))
        twice = [k for k, m in counts.items() if m > 1]
        old_twice = [k for k in twice if k.startswith(b"big-")]
        missing = [i for i in range(fresh_sent) if f"fresh-{i}".encode() not in counts]
        raise AssertionError(
            f"{written} written, {n + fresh_sent} expected: {len(twice)} identities "
            f"twice ({len(old_twice)} from before the moves), e.g. {twice[:5]}; "
            f"{len(missing)} fresh missing"
        )

    latencies.sort()
    worst_stall_ms = (
        max(c.node(i).metrics().longest_stall_us(before[i]) for i in range(3)) / 1000
    )
    report = (
        f"{len(latencies)} acks over {moves} moves: p50 "
        f"{latencies[len(latencies) // 2] * 1000:.1f} ms, p99 "
        f"{latencies[len(latencies) * 99 // 100] * 1000:.1f} ms, max "
        f"{latencies[-1] * 1000:.0f} ms; longest reactor stall {worst_stall_ms:.0f} ms"
    )
    print("\n" + report)
    # A move costs an election's worth of retries whatever the index; what
    # indexing several hundred thousand entries adds must not show on top.
    assert latencies[-1] < 10, report
    assert worst_stall_ms < 500, report
    assert_memory_healthy(c)
