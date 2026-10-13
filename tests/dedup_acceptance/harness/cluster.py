"""A Redpanda cluster of docker containers, for black-box acceptance tests.

Every node runs the image's redpanda binary directly, on the host network,
with its own config and data directory on the host. That keeps the data
directory inspectable (snapshot files, kvstore) and lets a node be stopped,
killed, restarted, or swapped to another image on the same data, which is
what upgrade and crash scenarios need.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

import requests
import yaml

READY_TIMEOUT_S = float(os.environ.get("DEDUP_ACCEPTANCE_READY_TIMEOUT", "120"))


def _run(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, check=check, capture_output=True, text=True)


@dataclass
class NodeSpec:
    image: str
    smp: int = 1
    memory: str = "1G"


@dataclass
class Node:
    cluster: Cluster
    node_id: int
    spec: NodeSpec
    root: Path
    container: str = field(init=False)

    def __post_init__(self) -> None:
        self.container = f"{self.cluster.name}-n{self.node_id}"
        (self.root / "data").mkdir(parents=True, exist_ok=True)
        (self.root / "conf").mkdir(parents=True, exist_ok=True)

    # -- addresses ---------------------------------------------------------

    @property
    def kafka_port(self) -> int:
        return self.cluster.port_base + 10 * self.node_id + 2

    @property
    def admin_port(self) -> int:
        return self.cluster.port_base + 10 * self.node_id + 4

    @property
    def rpc_port(self) -> int:
        return self.cluster.port_base + 10 * self.node_id + 5

    @property
    def kafka_addr(self) -> str:
        return f"127.0.0.1:{self.kafka_port}"

    @property
    def admin_url(self) -> str:
        return f"http://127.0.0.1:{self.admin_port}"

    @property
    def data_dir(self) -> Path:
        return self.root / "data"

    # -- lifecycle ---------------------------------------------------------

    def write_config(self) -> None:
        seed = self.cluster.nodes[0]
        cfg = {
            "redpanda": {
                "data_directory": "/var/lib/redpanda/data",
                "node_id": self.node_id,
                "developer_mode": True,
                "empty_seed_starts_cluster": False,
                "rpc_server": {"address": "127.0.0.1", "port": self.rpc_port},
                "advertised_rpc_api": {"address": "127.0.0.1", "port": self.rpc_port},
                "kafka_api": [{"address": "127.0.0.1", "port": self.kafka_port}],
                "advertised_kafka_api": [
                    {"address": "127.0.0.1", "port": self.kafka_port}
                ],
                "admin": [{"address": "127.0.0.1", "port": self.admin_port}],
                "seed_servers": [
                    {"host": {"address": "127.0.0.1", "port": seed.rpc_port}}
                ],
            }
        }
        (self.root / "conf" / "redpanda.yaml").write_text(yaml.safe_dump(cfg))
        bootstrap = dict(self.cluster.cluster_config)
        (self.root / "conf" / ".bootstrap.yaml").write_text(yaml.safe_dump(bootstrap))

    def start(self, wait: bool = True) -> None:
        self.write_config()
        _run("docker", "rm", "-f", self.container, check=False)
        _run(
            "docker",
            "run",
            "-d",
            "--name",
            self.container,
            "--network",
            "host",
            "--user",
            "0",
            "--label",
            f"dedup-acceptance={self.cluster.name}",
            "-v",
            f"{self.data_dir}:/var/lib/redpanda/data",
            "-v",
            f"{self.root / 'conf'}:/etc/redpanda",
            "--entrypoint",
            "/opt/redpanda/bin/redpanda",
            self.spec.image,
            "--redpanda-cfg",
            "/etc/redpanda/redpanda.yaml",
            "--smp",
            str(self.spec.smp),
            "--memory",
            self.spec.memory,
            "--reserve-memory",
            "0M",
            "--overprovisioned",
            "--unsafe-bypass-fsync=1",
            # Seastar reserves this many AIO control blocks per shard for
            # networking (10000 by default); several nodes on one host would
            # exhaust fs.aio-max-nr (65536 by default, also on CI runners).
            "--max-networking-io-control-blocks=2000",
            "--default-log-level=info",
        )
        if wait:
            self.wait_ready()

    def stop(self, timeout_s: int = 60) -> None:
        """Graceful shutdown (SIGTERM), then remove the container."""
        _run("docker", "stop", "-t", str(timeout_s), self.container, check=False)
        self._save_logs()
        _run("docker", "rm", "-f", self.container, check=False)

    def kill(self) -> None:
        """SIGKILL: no clean shutdown, no final flush."""
        _run("docker", "kill", "-s", "KILL", self.container, check=False)
        self._save_logs()
        _run("docker", "rm", "-f", self.container, check=False)

    def restart(self, image: str | None = None, memory: str | None = None) -> None:
        self.stop()
        if image is not None:
            self.spec.image = image
        if memory is not None:
            self.spec.memory = memory
        self.start()

    def pause(self) -> None:
        """Freeze every process in the node (SIGSTOP via the cgroup freezer):
        to its peers it looks like a node that stopped answering mid-flight,
        not one that shut down."""
        _run("docker", "pause", self.container)

    def unpause(self) -> None:
        _run("docker", "unpause", self.container)

    def is_running(self) -> bool:
        out = _run(
            "docker", "inspect", "-f", "{{.State.Running}}", self.container, check=False
        )
        return out.returncode == 0 and out.stdout.strip() == "true"

    def exit_info(self) -> str:
        out = _run(
            "docker",
            "inspect",
            "-f",
            "{{.State.ExitCode}} oom={{.State.OOMKilled}} {{.State.Error}}",
            self.container,
            check=False,
        )
        return out.stdout.strip()

    def wait_ready(self, timeout_s: float = READY_TIMEOUT_S) -> None:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if not self.is_running():
                raise AssertionError(
                    f"node {self.node_id} exited during startup "
                    f"({self.exit_info()}):\n{self.logs(tail=150)}"
                )
            try:
                r = requests.get(f"{self.admin_url}/v1/status/ready", timeout=2)
                if r.ok and r.json().get("status") == "ready":
                    return
            except requests.RequestException:
                pass
            time.sleep(0.5)
        raise AssertionError(
            f"node {self.node_id} not ready after {timeout_s}s:\n{self.logs(tail=150)}"
        )

    def logs(self, tail: int | None = None) -> str:
        args = ["docker", "logs"]
        if tail is not None:
            args += ["--tail", str(tail)]
        out = _run(*args, self.container, check=False)
        if out.returncode != 0:
            saved = self.root / "last.log"
            text = saved.read_text() if saved.exists() else ""
            return "\n".join(text.splitlines()[-(tail or 0) :]) if tail else text
        return out.stdout + out.stderr

    def _save_logs(self) -> None:
        out = _run("docker", "logs", self.container, check=False)
        if out.returncode == 0:
            with open(self.root / "all.log", "a") as f:
                f.write(out.stdout + out.stderr)
            (self.root / "last.log").write_text(out.stdout + out.stderr)

    # -- introspection -----------------------------------------------------

    def admin(self, method: str, path: str, **kwargs) -> requests.Response:
        """An admin API call. Redirects (to the controller or partition leader)
        are followed by hand: requests re-sends a redirected POST without its
        body, and loops when the leader is moving."""
        kwargs.pop("allow_redirects", None)
        node = self
        for _ in range(10):
            r = requests.request(
                method,
                f"{node.admin_url}{path}",
                timeout=30,
                allow_redirects=False,
                **kwargs,
            )
            if r.status_code not in (301, 302, 307, 308):
                return r
            location = r.headers.get("Location", "")
            m = re.match(r"https?://[^/:]+:(\d+)", location)
            target = None
            if m:
                port = int(m.group(1))
                target = next(
                    (n for n in self.cluster.nodes if n.admin_port == port), None
                )
            if target is None:
                return r
            node = target
            time.sleep(0.5)
        return r

    def metrics(self) -> Metrics:
        r = requests.get(f"{self.admin_url}/metrics", timeout=30)
        r.raise_for_status()
        return Metrics.parse(r.text)

    def partition_dir(self, topic: str, partition: int) -> Path:
        base = self.data_dir / "kafka" / topic
        matches = sorted(base.glob(f"{partition}_*"))
        if not matches:
            raise FileNotFoundError(
                f"no directory for {topic}/{partition} under {base}"
            )
        return matches[-1]


class Metrics:
    """Prometheus text exposition, enough to look samples up by name and labels."""

    _line = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{([^}]*)\})?\s+(\S+)")
    _label = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')

    def __init__(self, samples: list[tuple[str, dict[str, str], float]]):
        self.samples = samples

    @classmethod
    def parse(cls, text: str) -> Metrics:
        samples = []
        for line in text.splitlines():
            if not line or line.startswith("#"):
                continue
            m = cls._line.match(line)
            if not m:
                continue
            labels = dict(cls._label.findall(m.group(3) or ""))
            try:
                value = float(m.group(4))
            except ValueError:
                continue
            samples.append((m.group(1), labels, value))
        return cls(samples)

    def has(self, name: str) -> bool:
        return any(n == name for n, _, _ in self.samples)

    def select(self, name: str, **labels: str) -> list[tuple[dict[str, str], float]]:
        return [
            (l, v)
            for n, l, v in self.samples
            if n == name and all(l.get(k) == str(want) for k, want in labels.items())
        ]

    def sum(self, name: str, **labels: str) -> float:
        return sum(v for _, v in self.select(name, **labels))


class Cluster:
    """Nodes 0..n-1; node 0 is the seed."""

    _counter = 0

    def __init__(
        self,
        image: str,
        nodes: int = 3,
        smp: int = 1,
        memory: str = "1G",
        cluster_config: dict | None = None,
        port_base: int | None = None,
        workdir: Path | None = None,
    ):
        Cluster._counter += 1
        self.name = f"dedup-acc-{os.getpid()}-{Cluster._counter}"
        self.port_base = port_base or (21000 + 200 * (Cluster._counter % 40))
        self.workdir = Path(workdir or tempfile.mkdtemp(prefix=f"{self.name}-"))
        self.cluster_config = {
            "enable_leader_balancer": False,
            "auto_create_topics_enabled": False,
            "partition_autobalancing_mode": "off",
            "log_segment_size_min": 1024 * 1024,
            "storage_min_free_bytes": 0,
            **(cluster_config or {}),
        }
        self.nodes = [
            Node(
                self,
                i,
                NodeSpec(image=image, smp=smp, memory=memory),
                self.workdir / f"n{i}",
            )
            for i in range(nodes)
        ]

    @property
    def brokers(self) -> str:
        return ",".join(n.kafka_addr for n in self.nodes if n.is_running())

    def start(self) -> Cluster:
        for n in self.nodes:
            n.start(wait=False)
        for n in self.nodes:
            n.wait_ready()
        self.wait_healthy()
        return self

    def stop(self) -> None:
        for n in self.nodes:
            n.stop(timeout_s=10)

    def destroy(self, keep_on_failure: bool = False) -> None:
        self.stop()
        if not keep_on_failure:
            # Data was written as root inside the container; the test runner
            # may not be root, so clean up from a container too.
            _run(
                "docker",
                "run",
                "--rm",
                "-v",
                f"{self.workdir}:/w",
                "--entrypoint",
                "/bin/sh",
                self.nodes[0].spec.image,
                "-c",
                "rm -rf /w/*",
                check=False,
            )
            shutil.rmtree(self.workdir, ignore_errors=True)

    def node(self, node_id: int) -> Node:
        return self.nodes[node_id]

    def any_node(self) -> Node:
        for n in self.nodes:
            if n.is_running():
                return n
        raise AssertionError("no node is running")

    def wait_healthy(self, timeout_s: float = 90) -> None:
        deadline = time.monotonic() + timeout_s
        last = None
        while time.monotonic() < deadline:
            try:
                r = self.any_node().admin("GET", "/v1/cluster/health_overview")
                if r.ok:
                    last = r.json()
                    if last.get("is_healthy"):
                        return
            except requests.RequestException:
                pass
            time.sleep(1)
        raise AssertionError(f"cluster not healthy after {timeout_s}s: {last}")

    def set_config(self, **values) -> None:
        r = self.any_node().admin(
            "PUT", "/v1/cluster_config", json={"upsert": values, "remove": []}
        )
        r.raise_for_status()

    # -- partitions --------------------------------------------------------

    def partition(self, topic: str, partition: int) -> dict:
        r = self.any_node().admin("GET", f"/v1/partitions/kafka/{topic}/{partition}")
        r.raise_for_status()
        return r.json()

    def leader(self, topic: str, partition: int, timeout_s: float = 30) -> int:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                leader = self.partition(topic, partition).get("leader_id", -1)
                if leader is not None and leader >= 0:
                    return int(leader)
            except requests.RequestException:
                pass
            time.sleep(0.5)
        raise AssertionError(f"no leader for {topic}/{partition}")

    def controller_leader(self) -> int:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                r = self.any_node().admin("GET", "/v1/partitions/redpanda/controller/0")
                if r.ok and r.json().get("leader_id", -1) >= 0:
                    return int(r.json()["leader_id"])
            except requests.RequestException:
                pass
            time.sleep(0.5)
        raise AssertionError("no controller leader")

    def replicas(self, topic: str, partition: int) -> list[dict]:
        return self.partition(topic, partition)["replicas"]

    def transfer_leadership(self, topic: str, partition: int, target: int) -> None:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if self.leader(topic, partition) == target:
                return
            leader = self.leader(topic, partition)
            r = self.node(leader).admin(
                "POST",
                f"/v1/partitions/kafka/{topic}/{partition}/transfer_leadership",
                params={"target": target},
            )
            if r.ok:
                time.sleep(1)
                if self.leader(topic, partition) == target:
                    return
            time.sleep(1)
        raise AssertionError(
            f"could not move leadership of {topic}/{partition} to {target}"
        )

    def move_replicas(
        self,
        topic: str,
        partition: int,
        replicas: list[tuple[int, int]],
        timeout_s: float = 120,
    ) -> None:
        body = [{"node_id": n, "core": c} for n, c in replicas]
        # Sent to the controller leader directly: other nodes answer with a
        # redirect, which requests cannot follow for a POST with a body.
        r = self.node(self.controller_leader()).admin(
            "POST",
            f"/v1/partitions/kafka/{topic}/{partition}/replicas",
            json=body,
            allow_redirects=False,
        )
        r.raise_for_status()
        # Cores are assigned node-locally, so a cross-node move is complete
        # when the node set matches; move_core() moves between cores.
        want = sorted(n for n, _ in replicas)
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            p = self.partition(topic, partition)
            got = sorted(r["node_id"] for r in p["replicas"])
            if (
                got == want
                and p.get("status") == "done"
                and self._reconfigured(topic, partition)
            ):
                return
            time.sleep(1)
        raise AssertionError(
            f"move of {topic}/{partition} to nodes {want} did not finish"
        )

    def _reconfigured(self, topic: str, partition: int) -> bool:
        r = self.any_node().admin("GET", "/v1/partitions/reconfigurations")
        if not r.ok:
            return True
        return not any(
            x.get("topic") == topic and x.get("partition") == partition
            for x in r.json()
        )

    def local_core(self, topic: str, partition: int, node_id: int) -> int | None:
        """The shard a replica actually runs on. The admin API's `core` field
        is the controller's view and can be stale once cores are assigned
        node-locally; the dedup STM's metrics carry the real shard label."""
        m = self.node(node_id).metrics()
        rows = m.select(
            "vectorized_dedup_partition_index_entries",
            topic=topic,
            partition=str(partition),
        )
        return int(rows[0][0]["shard"]) if rows else None

    def move_core(
        self,
        topic: str,
        partition: int,
        node_id: int,
        core: int,
        timeout_s: float = 120,
    ) -> None:
        """Move one replica to another core of the same node."""
        r = self.node(node_id).admin(
            "POST",
            f"/v1/partitions/kafka/{topic}/{partition}/replicas/{node_id}",
            json={"core": core},
        )
        r.raise_for_status()
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if self.local_core(topic, partition, node_id) == core:
                return
            time.sleep(1)
        raise AssertionError(
            f"{topic}/{partition} on node {node_id} never moved to core {core}"
        )

    def metrics(self) -> dict[int, Metrics]:
        return {n.node_id: n.metrics() for n in self.nodes if n.is_running()}
