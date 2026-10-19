"""Throughput cost of produce-time deduplication.

Not part of the default run: pass --bench. Each scenario creates a topic on a
single-node, single-core broker, produces the same offered load from several
producer processes, and reports

- records/s the client managed to produce (offered load, duplicates included),
- broker CPU per million records (reactor CPU time, from the broker's own
  metrics -- the number that does not depend on how fast the client is),
- reactor stalls during the run, and the longest of them (from the
  broker's stall histogram, so an upper bound),
- records actually written, so the duplicates dropped are visible,
- produce latency once the index is loaded: after the throughput run, one
  producer sends the same kind of records at a fixed rate into the same
  topic, and each record's acknowledgement latency (acks=all) is taken from
  the client.

Every dedup scenario is compared against the same load on a topic without
dedup. Results go to stdout and, with --bench-out, to a Markdown file.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import time
from dataclasses import dataclass, field

import pytest
from confluent_kafka import Producer
from harness import kafka
from harness.cluster import Cluster

pytestmark = pytest.mark.bench

CPU_MS = "vectorized_reactor_cpu_used_time_ms"
STALLS = "vectorized_reactor_stalls_count"
SHARD_ENTRIES = "vectorized_dedup_shard_index_entries"

RECORDS = int(os.environ.get("DEDUP_BENCH_RECORDS", "2000000"))
PRODUCERS = int(os.environ.get("DEDUP_BENCH_PRODUCERS", "2"))
REPEATS = int(os.environ.get("DEDUP_BENCH_REPEATS", "3"))
LATENCY_RATE = int(os.environ.get("DEDUP_BENCH_LATENCY_RATE", "20000"))
LATENCY_SECONDS = float(os.environ.get("DEDUP_BENCH_LATENCY_SECONDS", "10"))
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
    header: bool = False
    compression: str = "none"
    cluster_config: dict = field(default_factory=dict)
    note: str = ""


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
]


def _produce_worker(args) -> tuple[float, int]:
    brokers, topic, worker, n, dup_every, header, compression = args
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
        ident = i // dup_every if dup_every else i
        key = f"w{worker}-{ident:012d}".encode()
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
    to append and replicate a small batch, not client batching."""
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
    latencies: list[float] = []
    errors = 0

    def cb(err, msg):
        nonlocal errors
        if err is not None:
            errors += 1
        else:
            latencies.append(msg.latency())

    total = int(LATENCY_RATE * LATENCY_SECONDS)
    per_tick = max(1, LATENCY_RATE // 1000)
    start = time.monotonic()
    sent = 0
    while sent < total:
        due = start + sent / LATENCY_RATE
        now = time.monotonic()
        if due > now:
            time.sleep(due - now)
        for _ in range(min(per_tick, total - sent)):
            ident = sent // s.duplicate_every if s.duplicate_every else sent
            key = f"lat-{ident:012d}".encode()
            kw = {"headers": [(HEADER, key)]} if s.header else {}
            p.produce(topic, value=VALUE, key=key, on_delivery=cb, **kw)
            sent += 1
        p.poll(0)
    p.flush(120)
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


def _run(c: Cluster, s: Scenario) -> dict:
    topic = kafka.unique("bench")
    cfg = {}
    if s.dedup:
        cfg[kafka.DEDUP_WINDOW] = str(3_600_000)
        if s.header:
            cfg[kafka.DEDUP_KEY_HEADER] = HEADER
    kafka.create_topic(c.brokers, topic, PARTITIONS, 1, cfg)
    per = RECORDS // PRODUCERS
    before = c.node(0).metrics()
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
                    s.header,
                    s.compression,
                )
                for w in range(PRODUCERS)
            ],
        )
    wall = time.monotonic() - t0
    after = c.node(0).metrics()
    errors = sum(e for _, e in results)
    offered = per * PRODUCERS
    cpu_ms = after.sum(CPU_MS) - before.sum(CPU_MS)
    out = {
        "scenario": s.name,
        "note": s.note,
        "records/s": offered / wall,
        "cpu ms per 1M records": cpu_ms / offered * 1e6,
        "stalls": after.sum(STALLS) - before.sum(STALLS),
        "longest stall ms": after.longest_stall_us(before) / 1000,
        "offered": offered,
        "written": _written(c.brokers, topic),
        "errors": errors,
        "index entries": after.sum(SHARD_ENTRIES),
    }
    stalls_before = c.node(0).metrics().sum(STALLS)
    out.update(_latency(c.brokers, topic, s))
    out["latency stalls"] = c.node(0).metrics().sum(STALLS) - stalls_before
    kafka.delete_topic(c.brokers, topic)
    return out


def _table(rows: list[dict], image: str) -> str:
    base = {r["scenario"]: r for r in rows}
    lines = [
        f"Image `{image}`, {RECORDS:,} records of 100 bytes per scenario from "
        f"{PRODUCERS} producer processes, one broker with one core, 4 "
        f"partitions, acks=all; median of {REPEATS} interleaved runs. "
        f"Latency: {LATENCY_RATE:,} records/s for {LATENCY_SECONDS:g} s from "
        "one producer (linger.ms=0) after each throughput run, into the same "
        "topic with its index loaded.",
        "",
        "| scenario | records/s | vs baseline | broker CPU ms / 1M records "
        "| vs baseline | written | stalls | longest stall ms | p50 ms | p99 ms "
        "| p99.9 ms | max ms | note |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for r in rows:
        ref = base.get("baseline, lz4" if "lz4" in r["scenario"] else "baseline")
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
        by_config.setdefault(tuple(sorted(s.cluster_config.items())), []).append(s)
    for config, scenarios in by_config.items():
        c = make_cluster(nodes=1, smp=1, memory="2G", cluster_config=dict(config))
        if config and not c.node(0).metrics().has(SHARD_ENTRIES):
            continue  # the shard budget does not exist in this build
        # Warm up the broker (allocator, page cache, connections) so the first
        # scenario is not charged for it.
        _run(c, Scenario("warm-up", dedup=False))
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
