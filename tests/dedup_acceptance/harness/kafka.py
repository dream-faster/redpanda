"""Kafka client helpers for the dedup acceptance tests.

Dedup applies only to plain produce: idempotent and transactional producers
bypass it by design. So the default producer here has idempotence off --
librdkafka would otherwise enable it implicitly for acks=all.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewTopic,
)

DEDUP_WINDOW = "redpanda.dedup.window.ms"
DEDUP_KEY_HEADER = "redpanda.dedup.key.header"


@dataclass
class Rec:
    key: bytes | None
    value: bytes | None = b"v"
    headers: list[tuple[str, bytes | None]] = field(default_factory=list)
    timestamp_ms: int | None = None


@dataclass
class Delivery:
    key: bytes | None
    error: KafkaError | None
    offset: int | None


@dataclass
class Consumed:
    offset: int
    key: bytes | None
    value: bytes | None
    headers: list[tuple[str, bytes | None]]
    timestamp_ms: int


def admin(brokers: str) -> AdminClient:
    return AdminClient({"bootstrap.servers": brokers})


def create_topic(
    brokers: str,
    name: str,
    partitions: int = 1,
    replicas: int = 3,
    config: dict[str, str] | None = None,
) -> None:
    a = admin(brokers)
    fut = a.create_topics(
        [NewTopic(name, partitions, replicas, config=config or {})],
        operation_timeout=30,
    )
    fut[name].result()
    wait_for_leaders(brokers, name, partitions)


def delete_topic(brokers: str, name: str) -> None:
    a = admin(brokers)
    a.delete_topics([name], operation_timeout=30)[name].result()
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        md = admin(brokers).list_topics(timeout=10)
        if name not in md.topics:
            return
        time.sleep(0.5)
    raise AssertionError(f"topic {name} still listed after delete")


def set_topic_config(brokers: str, name: str, config: dict[str, str | None]) -> None:
    entries = []
    for prop, v in config.items():
        if v is None:
            entries.append(
                ConfigEntry(prop, "", incremental_operation=AlterConfigOpType.DELETE)
            )
        else:
            entries.append(
                ConfigEntry(prop, str(v), incremental_operation=AlterConfigOpType.SET)
            )
    res = ConfigResource(ConfigResource.Type.TOPIC, name, incremental_configs=entries)
    a = admin(brokers)
    a.incremental_alter_configs([res])[res].result()


def topic_config(brokers: str, name: str) -> dict[str, str]:
    res = ConfigResource(ConfigResource.Type.TOPIC, name)
    a = admin(brokers)
    cfg = a.describe_configs([res])[res].result()
    return {k: v.value for k, v in cfg.items()}


def wait_for_leaders(
    brokers: str, topic: str, partitions: int, timeout_s: float = 60
) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        md = admin(brokers).list_topics(topic, timeout=10)
        t = md.topics.get(topic)
        if (
            t is not None
            and t.error is None
            and len(t.partitions) == partitions
            and all(p.leader >= 0 for p in t.partitions.values())
        ):
            return
        time.sleep(0.5)
    raise AssertionError(f"{topic} has no leader for every partition")


def delete_records(brokers: str, topic: str, partition: int, before_offset: int) -> int:
    tp = TopicPartition(topic, partition, before_offset)
    a = admin(brokers)
    fut = a.delete_records([tp], operation_timeout=30)
    res = fut[tp].result()
    return res.low_watermark


def producer(brokers: str, acks: str = "all", **extra) -> Producer:
    conf = {
        "bootstrap.servers": brokers,
        "enable.idempotence": False,
        "acks": acks,
        "linger.ms": 20,
        "batch.num.messages": 100000,
        "message.timeout.ms": 60000,
        "retries": 5,
        "retry.backoff.ms": 200,
    }
    conf.update(extra)
    return Producer(conf)


def produce(
    brokers: str,
    topic: str,
    records: list[Rec],
    partition: int = 0,
    acks: str = "all",
    one_request: bool = True,
    p: Producer | None = None,
    **producer_conf,
) -> list[Delivery]:
    """Produce records and wait for every delivery report.

    one_request=True queues all records before flushing, so librdkafka sends
    them as one batch (one produce request); False flushes after each record.
    """
    p = p or producer(brokers, acks=acks, **producer_conf)
    results: list[Delivery] = []

    def make_cb(key):
        def cb(err, msg):
            results.append(
                Delivery(key, err, msg.offset() if msg is not None else None)
            )

        return cb

    for r in records:
        kwargs = {}
        if r.timestamp_ms is not None:
            kwargs["timestamp"] = r.timestamp_ms
        p.produce(
            topic,
            key=r.key,
            value=r.value,
            headers=r.headers or None,
            partition=partition,
            on_delivery=make_cb(r.key),
            **kwargs,
        )
        if not one_request:
            assert p.flush(60) == 0, "produce did not complete"
    assert p.flush(60) == 0, "produce did not complete"
    if acks != "0":
        assert len(results) == len(records), (len(results), len(records))
    return results


def produce_ok(brokers: str, topic: str, records: list[Rec], **kw) -> list[Delivery]:
    res = produce(brokers, topic, records, **kw)
    errors = [d for d in res if d.error is not None]
    assert not errors, f"produce failed: {errors[:5]}"
    return res


def keyed(*keys: str | bytes, value: bytes = b"v") -> list[Rec]:
    return [Rec(k.encode() if isinstance(k, str) else k, value) for k in keys]


_RETRIABLE = {
    KafkaError.NOT_LEADER_FOR_PARTITION,
    KafkaError.LEADER_NOT_AVAILABLE,
    KafkaError.UNKNOWN_TOPIC_OR_PART,
    KafkaError.REQUEST_TIMED_OUT,
    KafkaError._TRANSPORT,
    KafkaError._TIMED_OUT,
}


def consume_all(
    brokers: str,
    topic: str,
    partition: int = 0,
    isolation: str = "read_uncommitted",
    timeout_s: float = 60,
) -> list[Consumed]:
    """Every record in the partition, from the log start to the high watermark
    (or, for read_committed, the last stable offset). Retried from scratch on
    the transient errors a leadership change or restart produces."""
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            return _consume_all_once(brokers, topic, partition, isolation, deadline)
        except KafkaException as e:
            err = e.args[0] if e.args else None
            code = err.code() if isinstance(err, KafkaError) else None
            if code not in _RETRIABLE or time.monotonic() >= deadline:
                raise
            time.sleep(1)


def _consume_all_once(brokers, topic, partition, isolation, deadline) -> list[Consumed]:
    c = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": f"dedup-acc-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "isolation.level": isolation,
            "enable.partition.eof": True,
        }
    )
    try:
        low, high = c.get_watermark_offsets(
            TopicPartition(topic, partition), timeout=10
        )
        out: list[Consumed] = []
        if high <= low:
            return out
        c.assign([TopicPartition(topic, partition, low)])
        while time.monotonic() < deadline:
            m = c.poll(0.5)
            if m is None:
                continue
            if m.error():
                if m.error().code() == KafkaError._PARTITION_EOF:
                    return out
                raise KafkaException(m.error())
            out.append(
                Consumed(
                    m.offset(),
                    m.key(),
                    m.value(),
                    list(m.headers() or []),
                    m.timestamp()[1],
                )
            )
            if m.offset() >= high - 1:
                return out
        raise AssertionError(
            f"timed out reading {topic}/{partition}: got {len(out)} records"
        )
    finally:
        c.close()


def keys_in(brokers: str, topic: str, partition: int = 0, **kw) -> list[bytes | None]:
    return [r.key for r in consume_all(brokers, topic, partition, **kw)]


def eventually(fn, timeout_s: float = 30, interval_s: float = 0.5, msg: str = ""):
    """Retry fn until it stops raising AssertionError or the timeout expires."""
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            return fn()
        except AssertionError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(interval_s)


def unique(prefix: str = "t") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def produce_bulk(
    brokers: str,
    topic: str,
    partition: int,
    keys,
    value: bytes = b"v",
    timestamp_ms=None,
    **producer_conf,
) -> int:
    """Produce many records fast: no per-record delivery bookkeeping, only an
    error count. `keys` is any iterable of bytes; `timestamp_ms`, if given, is
    called with the record's index. Returns the number produced."""
    p = producer(
        brokers,
        **{"linger.ms": 50, "queue.buffering.max.messages": 1_000_000, **producer_conf},
    )
    errors: list = []

    def cb(err, _msg):
        if err is not None:
            errors.append(err)

    n = 0
    for i, k in enumerate(keys):
        kwargs = {}
        if timestamp_ms is not None:
            kwargs["timestamp"] = timestamp_ms(i)
        while True:
            try:
                p.produce(
                    topic,
                    key=k,
                    value=value,
                    partition=partition,
                    on_delivery=cb,
                    **kwargs,
                )
                break
            except BufferError:
                p.poll(0.1)
        n += 1
        if n % 10_000 == 0:
            p.poll(0)
    assert p.flush(300) == 0, "bulk produce did not complete"
    assert not errors, f"{len(errors)} records failed, first: {errors[0]}"
    return n
