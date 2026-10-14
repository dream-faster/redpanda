"""What a client sees: which records dedup admits and which it drops.

All of these run against one shared three-node cluster, each on its own topic.
"""

from __future__ import annotations

import threading
import time

import pytest
from confluent_kafka import Producer
from harness import kafka
from harness.kafka import Rec, keyed

pytestmark = pytest.mark.usefixtures("dump_logs_on_failure")


def values(brokers, topic, partition=0):
    return [r.value for r in kafka.consume_all(brokers, topic, partition)]


# --- first wins ------------------------------------------------------------


def test_duplicate_across_requests_is_dropped(cluster, dedup_topic):
    t = dedup_topic()
    kafka.produce_ok(cluster.brokers, t, [Rec(b"a", b"1")])
    kafka.produce_ok(cluster.brokers, t, [Rec(b"b", b"2")])
    kafka.produce_ok(cluster.brokers, t, [Rec(b"a", b"3")])
    assert values(cluster.brokers, t) == [b"1", b"2"]


def test_duplicate_within_one_request_keeps_the_first(cluster, dedup_topic):
    t = dedup_topic()
    kafka.produce_ok(
        cluster.brokers,
        t,
        [
            Rec(b"a", b"1"),
            Rec(b"b", b"2"),
            Rec(b"a", b"3"),
            Rec(b"c", b"4"),
            Rec(b"b", b"5"),
        ],
    )
    got = kafka.consume_all(cluster.brokers, t)
    assert [(r.key, r.value) for r in got] == [(b"a", b"1"), (b"b", b"2"), (b"c", b"4")]


def test_all_duplicate_request_is_acknowledged(cluster, dedup_topic):
    """A request whose every record is a duplicate succeeds with nothing
    written, rather than failing the producer."""
    t = dedup_topic()
    kafka.produce_ok(cluster.brokers, t, keyed("a", "b"))
    res = kafka.produce_ok(cluster.brokers, t, keyed("a", "b", "a"))
    assert all(d.error is None for d in res)
    assert kafka.keys_in(cluster.brokers, t) == [b"a", b"b"]


def test_admitted_records_are_contiguous_and_ordered(cluster, dedup_topic):
    t = dedup_topic()
    first = [Rec(f"k{i}".encode(), f"v{i}".encode()) for i in range(50)]
    kafka.produce_ok(cluster.brokers, t, first)
    # Half repeats, half new, interleaved, in one request.
    mixed = []
    for i in range(50):
        mixed.append(Rec(f"k{i}".encode(), b"dup"))
        mixed.append(Rec(f"n{i}".encode(), f"w{i}".encode()))
    kafka.produce_ok(cluster.brokers, t, mixed)
    got = kafka.consume_all(cluster.brokers, t)
    assert [r.offset for r in got] == list(range(100))
    assert [r.value for r in got] == [f"v{i}".encode() for i in range(50)] + [
        f"w{i}".encode() for i in range(50)
    ]


def test_distinct_keys_are_all_admitted(cluster, dedup_topic):
    t = dedup_topic()
    recs = [Rec(f"key-{i}".encode(), str(i).encode()) for i in range(2000)]
    kafka.produce_ok(cluster.brokers, t, recs)
    assert len(kafka.consume_all(cluster.brokers, t)) == 2000


def test_retried_produce_is_absorbed(cluster, dedup_topic):
    """The case dedup exists for: a client that does not know whether its
    first attempt landed sends the same records again."""
    t = dedup_topic()
    batch = [Rec(f"order-{i}".encode(), f"payload-{i}".encode()) for i in range(100)]
    for _ in range(3):
        kafka.produce_ok(cluster.brokers, t, batch)
    assert len(kafka.consume_all(cluster.brokers, t)) == 100


@pytest.mark.parametrize("acks", ["1", "all"])
def test_acks_levels(cluster, dedup_topic, acks):
    t = dedup_topic()
    kafka.produce_ok(cluster.brokers, t, keyed("a", "b", "a"), acks=acks)
    kafka.produce_ok(cluster.brokers, t, keyed("b", "c"), acks=acks)
    assert kafka.keys_in(cluster.brokers, t) == [b"a", b"b", b"c"]


def test_acks_zero(cluster, dedup_topic):
    t = dedup_topic()
    kafka.produce(cluster.brokers, t, keyed("a", "b", "a"), acks="0")
    kafka.produce(cluster.brokers, t, keyed("a", "c"), acks="0")
    kafka.eventually(lambda: _assert_keys(cluster, t, [b"a", b"b", b"c"]))


def _assert_keys(cluster, t, want, partition=0):
    got = kafka.keys_in(cluster.brokers, t, partition)
    assert got == want, got


# --- the window -------------------------------------------------------------


def test_identity_is_admitted_again_after_the_window(cluster, dedup_topic):
    t = dedup_topic(window_ms=3000)
    kafka.produce_ok(cluster.brokers, t, [Rec(b"a", b"1")])
    kafka.produce_ok(cluster.brokers, t, [Rec(b"a", b"2")])
    time.sleep(4.5)
    kafka.produce_ok(cluster.brokers, t, [Rec(b"a", b"3")])
    kafka.produce_ok(cluster.brokers, t, [Rec(b"a", b"4")])
    assert values(cluster.brokers, t) == [b"1", b"3"]


def test_window_is_measured_on_record_timestamps(cluster, dedup_topic):
    """Two records with explicit CreateTimes further apart than the window are
    both admitted, however close together they are sent."""
    t = dedup_topic(window_ms=10_000)
    now = int(time.time() * 1000)
    kafka.produce_ok(cluster.brokers, t, [Rec(b"a", b"old", timestamp_ms=now - 60_000)])
    kafka.produce_ok(cluster.brokers, t, [Rec(b"a", b"new", timestamp_ms=now)])
    kafka.produce_ok(cluster.brokers, t, [Rec(b"a", b"dup", timestamp_ms=now + 1)])
    assert values(cluster.brokers, t) == [b"old", b"new"]


def test_future_timestamp_does_not_disable_dedup(cluster, dedup_topic):
    """A record dated ahead of the broker clock (legal: up to
    message.timestamp.after.max.ms) must not push the eviction cutoff past
    every entry in the index."""
    t = dedup_topic(window_ms=60_000)
    now = int(time.time() * 1000)
    kafka.produce_ok(cluster.brokers, t, keyed("a", "b"))
    kafka.produce_ok(
        cluster.brokers, t, [Rec(b"future", b"f", timestamp_ms=now + 30 * 60_000)]
    )
    kafka.produce_ok(cluster.brokers, t, keyed("a", "b", "c"))
    assert kafka.keys_in(cluster.brokers, t) == [b"a", b"b", b"future", b"c"]


def test_changing_the_window_keeps_existing_state(cluster, dedup_topic):
    t = dedup_topic(window_ms=3_600_000)
    kafka.produce_ok(cluster.brokers, t, keyed("a"))
    kafka.set_topic_config(cluster.brokers, t, {kafka.DEDUP_WINDOW: "7200000"})
    time.sleep(2)
    kafka.produce_ok(cluster.brokers, t, keyed("a", "b"))
    assert kafka.keys_in(cluster.brokers, t) == [b"a", b"b"]


# --- enabling and disabling ------------------------------------------------


def test_topic_without_dedup_admits_duplicates(cluster):
    t = kafka.unique("plain")
    kafka.create_topic(cluster.brokers, t, 1, 3)
    kafka.produce_ok(cluster.brokers, t, keyed("a", "a", "a"))
    assert kafka.keys_in(cluster.brokers, t) == [b"a", b"a", b"a"]


def test_disable_admits_duplicates_and_reenable_starts_fresh(cluster, dedup_topic):
    t = dedup_topic()
    kafka.produce_ok(cluster.brokers, t, keyed("a"))
    kafka.produce_ok(cluster.brokers, t, keyed("a"))
    assert kafka.keys_in(cluster.brokers, t) == [b"a"]

    kafka.set_topic_config(cluster.brokers, t, {kafka.DEDUP_WINDOW: "-1"})
    time.sleep(2)
    kafka.produce_ok(cluster.brokers, t, keyed("a"))
    assert kafka.keys_in(cluster.brokers, t) == [b"a", b"a"]

    # Re-enabling starts from an empty index: what was seen before the
    # disable no longer counts.
    kafka.set_topic_config(cluster.brokers, t, {kafka.DEDUP_WINDOW: "3600000"})
    time.sleep(2)
    kafka.produce_ok(cluster.brokers, t, keyed("a"))
    kafka.produce_ok(cluster.brokers, t, keyed("a"))
    assert kafka.keys_in(cluster.brokers, t) == [b"a", b"a", b"a"]


def test_enable_on_an_existing_topic(cluster):
    t = kafka.unique("late")
    kafka.create_topic(cluster.brokers, t, 1, 3)
    kafka.produce_ok(cluster.brokers, t, keyed("a", "a"))
    kafka.set_topic_config(cluster.brokers, t, {kafka.DEDUP_WINDOW: "3600000"})
    time.sleep(2)
    kafka.produce_ok(cluster.brokers, t, keyed("b", "b"))
    assert kafka.keys_in(cluster.brokers, t) == [b"a", b"a", b"b"]


def test_dedup_config_is_reported(cluster, dedup_topic):
    t = dedup_topic(window_ms=123_000, key_header="x-id")
    cfg = kafka.topic_config(cluster.brokers, t)
    assert cfg[kafka.DEDUP_WINDOW] == "123000"
    assert cfg[kafka.DEDUP_KEY_HEADER] == "x-id"


# --- identity sources -------------------------------------------------------


def test_null_keys_are_never_deduplicated(cluster, dedup_topic):
    t = dedup_topic()
    kafka.produce_ok(
        cluster.brokers, t, [Rec(None, b"1"), Rec(None, b"2"), Rec(None, b"1")]
    )
    assert values(cluster.brokers, t) == [b"1", b"2", b"1"]


def test_empty_key_is_an_identity(cluster, dedup_topic):
    t = dedup_topic()
    kafka.produce_ok(cluster.brokers, t, [Rec(b"", b"1"), Rec(b"", b"2")])
    assert values(cluster.brokers, t) == [b"1"]


def test_header_mode_deduplicates_on_the_header_value(cluster, dedup_topic):
    t = dedup_topic(key_header="x-id")
    kafka.produce_ok(
        cluster.brokers,
        t,
        [
            Rec(b"k1", b"1", [("x-id", b"A")]),
            Rec(b"k2", b"2", [("x-id", b"A")]),  # same header, different key
            Rec(b"k1", b"3", [("x-id", b"B")]),  # same key, different header
        ],
    )
    assert values(cluster.brokers, t) == [b"1", b"3"]


def test_header_mode_uses_the_first_occurrence(cluster, dedup_topic):
    t = dedup_topic(key_header="x-id")
    kafka.produce_ok(
        cluster.brokers,
        t,
        [
            Rec(b"k", b"1", [("x-id", b"A"), ("x-id", b"Z")]),
            Rec(b"k", b"2", [("other", b"A"), ("x-id", b"A")]),
            Rec(b"k", b"3", [("x-id", b"Z")]),
        ],
    )
    assert values(cluster.brokers, t) == [b"1", b"3"]


def test_header_mode_rejects_a_record_without_the_header(cluster, dedup_topic):
    t = dedup_topic(key_header="x-id")
    res = kafka.produce(
        cluster.brokers,
        t,
        [Rec(b"k", b"1", [("x-id", b"A")]), Rec(b"k", b"2", [("other", b"B")])],
        retries=0,
    )
    assert all(d.error is not None for d in res), res
    # The whole request is rejected: nothing from it was written.
    assert kafka.consume_all(cluster.brokers, t) == []
    # And the rejected request did not index its identities either.
    kafka.produce_ok(cluster.brokers, t, [Rec(b"k", b"3", [("x-id", b"A")])])
    assert values(cluster.brokers, t) == [b"3"]


def test_header_mode_rejects_a_null_header_value(cluster, dedup_topic, new_build):
    """A null header value is not an identity: digesting it would give every
    such record the identity of the empty buffer. (Not in builds before the
    memory fixes either, hence new_build.)"""
    t = dedup_topic(key_header="x-id")
    res = kafka.produce(
        cluster.brokers, t, [Rec(b"k", b"1", [("x-id", None)])], retries=0
    )
    assert res[0].error is not None
    assert kafka.consume_all(cluster.brokers, t) == []


def test_switching_identity_source_starts_fresh(cluster, dedup_topic):
    t = dedup_topic()
    kafka.produce_ok(cluster.brokers, t, [Rec(b"k", b"1", [("x-id", b"A")])])
    kafka.set_topic_config(cluster.brokers, t, {kafka.DEDUP_KEY_HEADER: "x-id"})
    time.sleep(2)
    # The index was keyed by Kafka key; under header mode it starts over, so
    # A is new even though k was seen.
    kafka.produce_ok(cluster.brokers, t, [Rec(b"k", b"2", [("x-id", b"A")])])
    kafka.produce_ok(cluster.brokers, t, [Rec(b"j", b"3", [("x-id", b"A")])])
    assert values(cluster.brokers, t) == [b"1", b"2"]

    # And back to keys.
    kafka.set_topic_config(cluster.brokers, t, {kafka.DEDUP_KEY_HEADER: None})
    time.sleep(2)
    kafka.produce_ok(cluster.brokers, t, [Rec(b"k", b"4"), Rec(b"k", b"5")])
    assert values(cluster.brokers, t) == [b"1", b"2", b"4"]


def test_large_identities(cluster, dedup_topic):
    t = dedup_topic(extra={"max.message.bytes": str(4 * 1024 * 1024)})
    big = b"x" * (512 * 1024)
    kafka.produce_ok(
        cluster.brokers, t, [Rec(big, b"1")], **{"message.max.bytes": 4_000_000}
    )
    kafka.produce_ok(
        cluster.brokers, t, [Rec(big, b"2")], **{"message.max.bytes": 4_000_000}
    )
    kafka.produce_ok(
        cluster.brokers,
        t,
        [Rec(big[:-1] + b"y", b"3")],
        **{"message.max.bytes": 4_000_000},
    )
    assert values(cluster.brokers, t) == [b"1", b"3"]


def test_binary_identities(cluster, dedup_topic):
    t = dedup_topic()
    ids = [bytes([i, 0, 255 - i]) for i in range(64)]
    kafka.produce_ok(cluster.brokers, t, [Rec(i, b"x") for i in ids])
    kafka.produce_ok(cluster.brokers, t, [Rec(i, b"y") for i in reversed(ids)])
    assert len(kafka.consume_all(cluster.brokers, t)) == 64


# --- producers that bypass dedup -------------------------------------------


def test_idempotent_producer_bypasses_dedup(cluster, dedup_topic):
    t = dedup_topic()
    kafka.produce_ok(
        cluster.brokers, t, keyed("a", "a"), **{"enable.idempotence": True}
    )
    assert kafka.keys_in(cluster.brokers, t) == [b"a", b"a"]
    # And it did not index anything: a plain produce of a is still new.
    kafka.produce_ok(cluster.brokers, t, keyed("a"))
    assert kafka.keys_in(cluster.brokers, t) == [b"a", b"a", b"a"]


def test_transactional_producer_bypasses_dedup(cluster, dedup_topic):
    t = dedup_topic()
    p = Producer(
        {
            "bootstrap.servers": cluster.brokers,
            "transactional.id": kafka.unique("tx"),
        }
    )
    p.init_transactions(30)
    p.begin_transaction()
    for v in (b"1", b"2"):
        p.produce(t, key=b"a", value=v, partition=0)
    p.commit_transaction(30)
    assert [
        r.value
        for r in kafka.consume_all(cluster.brokers, t, isolation="read_committed")
    ] == [
        b"1",
        b"2",
    ]


def test_aborted_transaction_does_not_poison_the_index(cluster, dedup_topic):
    t = dedup_topic()
    p = Producer(
        {"bootstrap.servers": cluster.brokers, "transactional.id": kafka.unique("tx")}
    )
    p.init_transactions(30)
    p.begin_transaction()
    p.produce(t, key=b"a", value=b"aborted", partition=0)
    p.flush(30)
    p.abort_transaction(30)
    kafka.produce_ok(cluster.brokers, t, [Rec(b"a", b"plain")])
    got = kafka.consume_all(cluster.brokers, t, isolation="read_committed")
    assert [r.value for r in got] == [b"plain"]


# --- batches and partitions -------------------------------------------------


@pytest.mark.parametrize("codec", ["none", "gzip", "snappy", "lz4", "zstd"])
def test_compressed_batches_with_partial_drops(cluster, dedup_topic, codec):
    t = dedup_topic()
    first = [Rec(f"k{i}".encode(), f"a{i}".encode() * 20) for i in range(0, 200, 2)]
    kafka.produce_ok(cluster.brokers, t, first, **{"compression.type": codec})
    second = [Rec(f"k{i}".encode(), f"b{i}".encode() * 20) for i in range(200)]
    kafka.produce_ok(cluster.brokers, t, second, **{"compression.type": codec})
    got = kafka.consume_all(cluster.brokers, t)
    assert len(got) == 200
    by_key = {r.key: r.value for r in got}
    for i in range(200):
        want = (f"a{i}" if i % 2 == 0 else f"b{i}").encode() * 20
        assert by_key[f"k{i}".encode()] == want


def test_partitions_are_independent(cluster, dedup_topic):
    t = dedup_topic(partitions=4)
    for p in range(4):
        kafka.produce_ok(cluster.brokers, t, keyed("same", "same"), partition=p)
    for p in range(4):
        assert kafka.keys_in(cluster.brokers, t, p) == [b"same"]


def test_topics_are_independent(cluster, dedup_topic):
    a, b = dedup_topic(), dedup_topic()
    kafka.produce_ok(cluster.brokers, a, keyed("k"))
    kafka.produce_ok(cluster.brokers, b, keyed("k"))
    assert kafka.keys_in(cluster.brokers, a) == [b"k"]
    assert kafka.keys_in(cluster.brokers, b) == [b"k"]


def test_concurrent_producers_admit_each_identity_once(cluster, dedup_topic):
    t = dedup_topic()
    keys = [f"id-{i}".encode() for i in range(300)]
    errors: list = []

    def worker(seed: int):
        try:
            order = keys[seed:] + keys[:seed]
            kafka.produce_ok(
                cluster.brokers, t, [Rec(k, str(seed).encode()) for k in order]
            )
        except Exception as e:  # noqa: BLE001 -- reported below
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(s * 37,)) for s in range(6)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    assert not errors, errors
    got = kafka.keys_in(cluster.brokers, t)
    assert sorted(got) == sorted(keys)


# --- metrics ----------------------------------------------------------------


def test_partition_metrics_count_drops_and_entries(cluster, dedup_topic):
    t = dedup_topic()
    kafka.produce_ok(cluster.brokers, t, keyed("a", "b", "c"))
    kafka.produce_ok(cluster.brokers, t, keyed("a", "b", "d"))
    leader = cluster.leader(t, 0)

    def check():
        m = cluster.node(leader).metrics()
        assert m.sum("vectorized_dedup_partition_records_dropped", topic=t) == 2
        assert m.sum("vectorized_dedup_partition_index_entries", topic=t) == 4
        assert m.sum("vectorized_dedup_partition_requests_rebuilt", topic=t) == 1

    kafka.eventually(check)

    # Followers index the same identities from the log.
    def followers():
        for n in cluster.nodes:
            m = n.metrics()
            assert m.sum("vectorized_dedup_partition_index_entries", topic=t) == 4, (
                n.node_id
            )

    kafka.eventually(followers)
