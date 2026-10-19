"""Throughput cost of produce-time deduplication.

Not part of the default run: pass --bench. Each scenario creates a topic on a
single-node, single-core broker -- or, for the rf3 scenarios, on three
single-core brokers with every partition replicated three times -- produces
the same offered load from several producer processes, and reports

- records/s the client managed to produce (offered load, duplicates included),
- broker CPU per million records (reactor CPU time summed over every broker,
  from their own metrics -- the number that does not depend on how fast the
  client is; with three replicas it includes the followers' apply path),
- reactor stalls during the run, and the longest of them (from the
  broker's stall histogram, so an upper bound),
- records actually written, so the duplicates dropped are visible,
- produce latency once the index is loaded: after the throughput run, one
  producer sends the same kind of records at a fixed rate into the same
  topic, and each record's acknowledgement latency (acks=all) is taken from
  the client.

The redundant-producer scenarios send every identity from two producers at
about the same time, the way two redundant feeds of the same stream do: half
of all records are duplicates of one still being replicated when it arrives.
Their latency phase runs two such producers in step.

Every dedup scenario is compared against the same load on a topic without
dedup. Results go to stdout and, with --bench-out, to a Markdown file.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import threading
import time
from dataclasses import dataclass, field

import pytest
from confluent_kafka import Producer
from harness import kafka
from harness.cluster import Cluster, Metrics

pytestmark = pytest.mark.bench

CPU_MS = "vectorized_reactor_cpu_used_time_ms"
STALLS = "vectorized_reactor_stalls_count"
SHARD_ENTRIES = "vectorized_dedup_shard_index_entries"

RECORDS = int(os.environ.get("DEDUP_BENCH_RECORDS", "2000000"))
PRODUCERS = int(os.environ.get("DEDUP_BENCH_PRODUCERS", "2"))
REPEATS = int(os.environ.get("DEDUP_BENCH_REPEATS", "3"))
LATENCY_RATE = int(os.environ.get("DEDUP_BENCH_LATENCY_RATE", "20000"))
LATENCY_SECONDS = float(os.environ.get("DEDUP_BENCH_LATENCY_SECONDS", "10"))
# One-way delay added between the rf3 brokers' RPC servers (see
# harness/delay_proxy.py), so a replication round trip costs what a network
# hop and an fsync do in production instead of a loopback hop. 0: none.
RPC_DELAY_MS = float(os.environ.get("DEDUP_BENCH_RPC_DELAY_MS", "0"))
# Scenario names to run, separated by semicolons (the names contain
# commas), for a longer A/B of a few of them; all of them when unset.
ONLY = {
    n.strip()
    for n in os.environ.get("DEDUP_BENCH_SCENARIOS", "").split(";")
    if n.strip()
}
PARTITIONS = 4
VALUE = b"v" * 100
HEADER = "dedup-id"


@dataclass
class Scenario:
    name: str
    dedup: bool
    duplicate_every: int = 0  # 0: all unique; 2: every key sent twice
    # Producers send in pairs, both of a pair the same identities in the same
    # order, so each identity arrives twice at about the same time.
    redundant: bool = False
    # How many records the second producer of a pair runs ahead of the
    # first. At 0 a request from either holds only identities the other
    # sent in the same tick; ahead, every request holds some the other has
    # in flight and some it has not sent yet.
    lead: int = 0
    header: bool = False
    compression: str = "none"
    # 1: one broker; 3: three brokers, every partition on all of them.
    replicas: int = 1
    cluster_config: dict = field(default_factory=dict)
    note: str = ""

    @property
    def baseline(self) -> str:
        name = "rf3 baseline" if self.replicas == 3 else "baseline"
        return f"{name}, lz4" if self.compression == "lz4" else name


SCENARIOS = [
    Scenario("baseline", dedup=False, note="no dedup"),
    Scenario("key, unique", dedup=True, note="every record a new identity"),
    Scenario(
        "key, 50% duplicates",
        dedup=True,
        duplicate_every=2,
        note="each key sent twice; the second is dropped",
    ),
    Scenario("header, unique", dedup=True, header=True, note="identity in a header"),
    Scenario("baseline, lz4", dedup=False, compression="lz4"),
    Scenario(
        "key, unique, lz4",
        dedup=True,
        compression="lz4",
        note="batches are decompressed to read identities",
    ),
    Scenario(
        "key, unique, shard full",
        dedup=True,
        cluster_config={"dedup_max_entries_per_shard": 200_000},
        note="index at the shard limit the whole run: every insert trims",
    ),
    Scenario("rf3 baseline", dedup=False, replicas=3, note="no dedup"),
    Scenario(
        "rf3 key, 50% duplicates",
        dedup=True,
        duplicate_every=2,
        replicas=3,
        note="each key sent twice in a row; the second is dropped",
    ),
    Scenario(
        "rf3 key, redundant producers",
        dedup=True,
        redundant=True,
        replicas=3,
        note="two producers send every key at about the same time",
    ),
    Scenario(
        "rf3 key, redundant producers, one ahead",
        dedup=True,
        redundant=True,
        lead=1,
        replicas=3,
        note="as above, one producer a record ahead: every request partly "
        "duplicates one in flight",
    ),
    Scenario("rf3 baseline, lz4", dedup=False, replicas=3, compression="lz4"),
    Scenario(
        "rf3 key, redundant producers, lz4",
        dedup=True,
        redundant=True,
        replicas=3,
        compression="lz4",
    ),
]


def _produce_worker(args) -> tuple[float, int]:
    brokers, topic, worker, n, dup_every, redundant, lead, header, compression = args
    # A redundant pair shares its identities: both send stream worker // 2.
    stream = worker // 2 if redundant else worker
    shift = lead if redundant and worker % 2 else 0
    p = Producer(
        {
            "bootstrap.servers": brokers,
            "enable.idempotence": False,
            "acks": "all",
            "linger.ms": 5,
            "batch.size": 1_000_000,
            "batch.num.messages": 100_000,
            "queue.buffering.max.messages": 2_000_000,
            "queue.buffering.max.kbytes": 2_000_000,
            "compression.type": compression,
            "message.timeout.ms": 120_000,
        }
    )
    errors = 0

    def cb(err, _msg):
        nonlocal errors
        if err is not None:
            errors += 1

    start = time.monotonic()
    for i in range(n):
        ident = (i // dup_every if dup_every else i) + shift
        key = f"w{stream}-{ident:012d}".encode()
        kw = {"headers": [(HEADER, key)]} if header else {}
        while True:
            try:
                p.produce(topic, value=VALUE, key=key, on_delivery=cb, **kw)
                break
            except BufferError:
                p.poll(0.05)
        if i % 10_000 == 0:
            p.poll(0)
    p.flush(300)
    return time.monotonic() - start, errors


def _latency(brokers: str, topic: str, s: Scenario) -> dict[str, float]:
    """Acknowledgement latency, in ms, of records produced at LATENCY_RATE for
    LATENCY_SECONDS into a topic whose index the throughput run filled. Sent
    in 1 ms ticks with linger.ms=0, so what is measured is the broker's time
    to append and replicate a small batch, not client batching. A redundant
    scenario runs two producers sending the same records in step, each at
    LATENCY_RATE, and takes both producers' latencies."""
    latencies: list[float] = []
    errors = 0
    lock = threading.Lock()

    def make_producer() -> Producer:
        p = Producer(
            {
                "bootstrap.servers": brokers,
                "enable.idempotence": False,
                "acks": "all",
                "linger.ms": 0,
                "compression.type": s.compression,
                "message.timeout.ms": 120_000,
            }
        )
        # Metadata and connections up front, so neither producer's start is
        # held back by them.
        p.list_topics(topic, timeout=30)
        return p

    def run(p: Producer, start: float, shift: int) -> None:
        nonlocal errors
        mine: list[float] = []
        failed = 0

        def cb(err, msg):
            nonlocal failed
            if err is not None:
                failed += 1
            else:
                mine.append(msg.latency())

        total = int(LATENCY_RATE * LATENCY_SECONDS)
        per_tick = max(1, LATENCY_RATE // 1000)
        sent = 0
        while sent < total:
            due = start + sent / LATENCY_RATE
            now = time.monotonic()
            if due > now:
                time.sleep(due - now)
            for _ in range(min(per_tick, total - sent)):
                ident = sent // s.duplicate_every if s.duplicate_every else sent
                key = f"lat-{ident + shift:012d}".encode()
                kw = {"headers": [(HEADER, key)]} if s.header else {}
                p.produce(topic, value=VALUE, key=key, on_delivery=cb, **kw)
                sent += 1
            p.poll(0)
        p.flush(120)
        with lock:
            latencies.extend(mine)
            errors += failed

    # In step: the redundant pair shares one schedule, so each identity is
    # sent by both within the same millisecond tick and the second copy
    # arrives while the first is still being replicated.
    producers = [make_producer() for _ in range(2 if s.redundant else 1)]
    start = time.monotonic() + 0.5
    threads = [
        threading.Thread(target=run, args=(p, start, s.lead * i))
        for i, p in enumerate(producers)
    ]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    latencies.sort()

    def pct(q: float) -> float:
        return latencies[min(len(latencies) - 1, int(q * len(latencies)))] * 1e3

    return {
        "p50 ms": pct(0.50),
        "p99 ms": pct(0.99),
        "p99.9 ms": pct(0.999),
        "max ms": latencies[-1] * 1e3,
        "latency errors": errors,
    }


def _written(brokers: str, topic: str) -> int:
    return sum(kafka.high_watermark(brokers, topic, p) for p in range(PARTITIONS))


def _metrics(c: Cluster) -> list[Metrics]:
    return [c.node(i).metrics() for i in range(len(c.nodes))]


def _sum(ms: list[Metrics], name: str) -> float:
    return sum(m.sum(name) for m in ms)


def _run(c: Cluster, s: Scenario) -> dict:
    topic = kafka.unique("bench")
    cfg = {}
    if s.dedup:
        cfg[kafka.DEDUP_WINDOW] = str(3_600_000)
        if s.header:
            cfg[kafka.DEDUP_KEY_HEADER] = HEADER
    kafka.create_topic(c.brokers, topic, PARTITIONS, s.replicas, cfg)
    kafka.wait_for_leaders(c.brokers, topic, PARTITIONS)
    per = RECORDS // PRODUCERS
    before = _metrics(c)
    t0 = time.monotonic()
    with mp.get_context("spawn").Pool(PRODUCERS) as pool:
        results = pool.map(
            _produce_worker,
            [
                (
                    c.brokers,
                    topic,
                    w,
                    per,
                    s.duplicate_every,
                    s.redundant,
                    s.lead,
                    s.header,
                    s.compression,
                )
                for w in range(PRODUCERS)
            ],
        )
    wall = time.monotonic() - t0
    after = _metrics(c)
    errors = sum(e for _, e in results)
    offered = per * PRODUCERS
    cpu_ms = _sum(after, CPU_MS) - _sum(before, CPU_MS)
    out = {
        "scenario": s.name,
        "baseline": s.baseline,
        "note": s.note,
        "records/s": offered / wall,
        "cpu ms per 1M records": cpu_ms / offered * 1e6,
        "stalls": _sum(after, STALLS) - _sum(before, STALLS),
        "longest stall ms": max(a.longest_stall_us(b) for a, b in zip(after, before))
        / 1000,
        "offered": offered,
        "written": _written(c.brokers, topic),
        "errors": errors,
        "index entries": _sum(after, SHARD_ENTRIES),
    }
    stalls_before = _sum(_metrics(c), STALLS)
    out.update(_latency(c.brokers, topic, s))
    out["latency stalls"] = _sum(_metrics(c), STALLS) - stalls_before
    kafka.delete_topic(c.brokers, topic)
    return out


def _table(rows: list[dict], image: str) -> str:
    base = {r["scenario"]: r for r in rows}
    lines = [
        f"Image `{image}`, {RECORDS:,} records of 100 bytes per scenario from "
        f"{PRODUCERS} producer processes, one broker with one core (rf3: three "
        f"brokers with one core each, every partition on all three"
        + (f", {RPC_DELAY_MS:g} ms added each way between them" if RPC_DELAY_MS else "")
        + "), 4 "
        f"partitions, acks=all; median of {REPEATS} interleaved runs. "
        f"Latency: {LATENCY_RATE:,} records/s for {LATENCY_SECONDS:g} s from "
        "one producer (linger.ms=0; two in step for the redundant scenarios) "
        "after each throughput run, into the same topic with its index loaded.",
        "",
        "| scenario | records/s | vs baseline | broker CPU ms / 1M records "
        "| vs baseline | written | stalls | longest stall ms | p50 ms | p99 ms "
        "| p99.9 ms | max ms | note |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for r in rows:
        ref = base.get(r["baseline"])
        rel_rate = r["records/s"] / ref["records/s"] - 1 if ref else 0.0
        rel_cpu = (
            r["cpu ms per 1M records"] / ref["cpu ms per 1M records"] - 1
            if ref
            else 0.0
        )
        lines.append(
            f"| {r['scenario']} | {r['records/s']:,.0f} | {rel_rate:+.0%} "
            f"| {r['cpu ms per 1M records']:,.0f} | {rel_cpu:+.0%} "
            f"| {r['written']:,} | {r['stalls']:,.0f} "
            f"| {r['longest stall ms']:.1f} "
            f"| {r['p50 ms']:.2f} | {r['p99 ms']:.2f} | {r['p99.9 ms']:.2f} "
            f"| {r['max ms']:.1f} | {r['note']} |"
        )
    return "\n".join(lines) + "\n"


def _median(rows: list[dict]) -> dict:
    """Per-metric median over the repeats of one scenario."""
    out = dict(rows[0])
    for k in (
        "records/s",
        "cpu ms per 1M records",
        "stalls",
        "longest stall ms",
        "p50 ms",
        "p99 ms",
        "p99.9 ms",
        "max ms",
    ):
        vals = sorted(r[k] for r in rows)
        out[k] = vals[len(vals) // 2]
    out["errors"] = sum(r["errors"] + r["latency errors"] for r in rows)
    return out


def test_dedup_throughput(make_cluster, image, pytestconfig):
    runs: dict[str, list[dict]] = {}
    by_config: dict[tuple, list[Scenario]] = {}
    for s in SCENARIOS:
        if ONLY and s.name not in ONLY:
            continue
        by_config.setdefault(
            (s.replicas, tuple(sorted(s.cluster_config.items()))), []
        ).append(s)
    for (replicas, config), scenarios in by_config.items():
        c = make_cluster(
            nodes=replicas,
            smp=1,
            memory="2G" if replicas == 1 else "1536M",
            cluster_config=dict(config),
            rpc_delay_ms=RPC_DELAY_MS if replicas > 1 else 0,
        )
        if config and not c.node(0).metrics().has(SHARD_ENTRIES):
            continue  # the shard budget does not exist in this build
        # Warm up the broker (allocator, page cache, connections) so the first
        # scenario is not charged for it.
        _run(c, Scenario("warm-up", dedup=False, replicas=replicas))
        # Interleaved rather than back to back, so drift over the run spreads
        # across scenarios instead of landing on one.
        for _ in range(REPEATS):
            for s in scenarios:
                r = _run(c, s)
                print(r, flush=True)
                runs.setdefault(s.name, []).append(r)
    rows = [_median(runs[s.name]) for s in SCENARIOS if s.name in runs]
    table = _table(rows, image)
    print("\n" + table)
    out = pytestconfig.getoption("--bench-out")
    if out:
        with open(out, "a") as f:
            f.write(table + "\n")
    for r in rows:
        assert r["errors"] == 0, r
