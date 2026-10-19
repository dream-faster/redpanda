"""Fixtures for the dedup acceptance suite.

    pytest tests/dedup_acceptance --image <image> [--old-image <image>]
                                  [--require-new-build]

--image is the build under test. --old-image is an earlier dedup build the
upgrade and mixed-version tests roll from and back to; without it they skip.

Some tests cover behaviour only the current build has (file-backed dedup
snapshots, the per-shard entry limit, kvstore recovery that drops legacy dedup
snapshots). They detect it from the image and skip on an older one, so the
suite can be pointed at the deployed build to compare; --require-new-build
turns those skips into failures, which is what CI uses, so a missing feature
can never pass as a skip.
"""

from __future__ import annotations

import os
import re
import time

import pytest
from harness import kafka
from harness.cluster import Cluster


def pytest_addoption(parser):
    parser.addoption("--image", default=os.environ.get("DEDUP_IMAGE"))
    parser.addoption("--old-image", default=os.environ.get("DEDUP_OLD_IMAGE"))
    parser.addoption(
        "--require-new-build",
        action="store_true",
        default=os.environ.get("DEDUP_REQUIRE_NEW_BUILD") == "1",
    )
    parser.addoption(
        "--bench",
        action="store_true",
        default=os.environ.get("DEDUP_BENCH") == "1",
        help="run the throughput benchmarks (test_bench.py)",
    )
    parser.addoption(
        "--bench-out",
        default=os.environ.get("DEDUP_BENCH_OUT"),
        help="append the benchmark results table to this Markdown file",
    )
    parser.addoption(
        "--stall-report",
        default=os.environ.get("DEDUP_STALL_REPORT"),
        help="append a tab-separated line per test that made its own clusters: "
        "test, outcome, reactor stalls counted, longest stall logged (ms), "
        "duration (s)",
    )
    parser.addoption(
        "--keep-clusters",
        action="store_true",
        default=os.environ.get("DEDUP_KEEP_CLUSTERS") == "1",
        help="leave failed clusters' data directories in place",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "new_build: needs behaviour only the build under test has"
    )
    config.addinivalue_line("markers", "upgrade: needs --old-image")
    config.addinivalue_line("markers", "slow: takes minutes rather than seconds")
    config.addinivalue_line("markers", "stress: sustained or adversarial load")
    config.addinivalue_line("markers", "bench: throughput benchmark, needs --bench")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--bench"):
        return
    skip = pytest.mark.skip(reason="benchmark: pass --bench to run")
    for item in items:
        if "bench" in item.keywords:
            item.add_marker(skip)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    rep = outcome.get_result()
    setattr(item, f"rep_{rep.when}", rep)


@pytest.fixture(scope="session")
def image(pytestconfig) -> str:
    img = pytestconfig.getoption("--image")
    if not img:
        pytest.exit("--image (or DEDUP_IMAGE) is required", returncode=2)
    return img


@pytest.fixture(scope="session")
def old_image(pytestconfig) -> str:
    img = pytestconfig.getoption("--old-image")
    if not img:
        pytest.skip("needs --old-image (or DEDUP_OLD_IMAGE)")
    return img


def _report_stalls(path: str, request, clusters: list[Cluster], took_s: float):
    """Reactor stalls the test's clusters counted, summed over their running
    nodes' shards, and the longest one any node logged. A node restarted
    during the test counts from its restart."""
    stalls = 0.0
    longest_ms = 0
    for c in clusters:
        try:
            for m in c.metrics().values():
                stalls += m.sum("vectorized_reactor_stalls_count")
        except Exception:
            # A node the test left frozen or half-stopped; report what the
            # others counted rather than fail the teardown.
            pass
        for n in c.nodes:
            for ms in _STALL_LOG.findall(n.logs()):
                longest_ms = max(longest_ms, int(ms))
    rep = getattr(request.node, "rep_call", None)
    outcome = rep.outcome if rep is not None else "error"
    with open(path, "a") as f:
        f.write(
            f"{request.node.nodeid}\t{outcome}\t{stalls:.0f}\t{longest_ms}"
            f"\t{took_s:.1f}\n"
        )


_STALL_LOG = re.compile(r"Reactor stalled for (\d+) ms")


def _dump_logs(cluster: Cluster, request) -> None:
    rep = getattr(request.node, "rep_call", None)
    if rep is not None and rep.failed:
        for n in cluster.nodes:
            print(f"\n===== node {n.node_id} ({n.spec.image}) log tail =====")
            print(n.logs(tail=200))


@pytest.fixture(scope="session")
def cluster(image, pytestconfig):
    """Three nodes, two shards each, shared by the tests that only need
    topics of their own."""
    c = Cluster(image, nodes=3, smp=2, memory="1G").start()
    yield c
    c.destroy(keep_on_failure=pytestconfig.getoption("--keep-clusters"))


@pytest.fixture
def dump_logs_on_failure(cluster, request):
    yield
    _dump_logs(cluster, request)


@pytest.fixture
def make_cluster(image, request, pytestconfig):
    """Dedicated clusters, for tests that restart nodes, change cluster-wide
    settings, or need particular memory limits."""
    made: list[Cluster] = []
    started = time.monotonic()

    def make(image_override: str | None = None, **kw) -> Cluster:
        c = Cluster(image_override or image, **kw)
        made.append(c)
        return c.start()

    yield make
    report = pytestconfig.getoption("--stall-report")
    if report:
        _report_stalls(report, request, made, time.monotonic() - started)
    for c in made:
        _dump_logs(c, request)
        failed = getattr(request.node, "rep_call", None)
        keep = (
            pytestconfig.getoption("--keep-clusters")
            and failed is not None
            and failed.failed
        )
        c.destroy(keep_on_failure=keep)


def has_new_build_features(c: Cluster) -> bool:
    """The per-shard dedup gauges exist only in builds with the shard budget,
    file-backed snapshots and the kvstore recovery discard -- they shipped
    together."""
    return c.any_node().metrics().has("vectorized_dedup_shard_max_index_entries")


@pytest.fixture
def new_build(cluster, pytestconfig):
    if not has_new_build_features(cluster):
        msg = "image predates the dedup memory fixes"
        if pytestconfig.getoption("--require-new-build"):
            pytest.fail(msg)
        pytest.skip(msg)


@pytest.fixture
def dedup_topic(cluster, dump_logs_on_failure):
    """Factory: a fresh dedup topic on the shared cluster."""

    def make(
        partitions: int = 1,
        replicas: int = 3,
        window_ms: int = 3_600_000,
        key_header: str | None = None,
        extra: dict[str, str] | None = None,
    ) -> str:
        name = kafka.unique("dd")
        cfg = {kafka.DEDUP_WINDOW: str(window_ms)}
        if key_header is not None:
            cfg[kafka.DEDUP_KEY_HEADER] = key_header
        cfg.update(extra or {})
        kafka.create_topic(cluster.brokers, name, partitions, replicas, cfg)
        return name

    return make
