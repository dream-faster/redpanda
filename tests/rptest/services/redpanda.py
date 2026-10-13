# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import concurrent.futures
from contextlib import contextmanager
import copy
import dataclasses
import json
import os
import pathlib
import random
import re
import shlex
import shutil
import signal
import socket
import tempfile
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from logging import Logger
from typing import (
    Any,
    Callable,
    Collection,
    Iterable,
    List,
    Mapping,
    NamedTuple,
    Protocol,
    Set,
    Tuple,
    TypedDict,
    TypeVar,
    cast,
)

from signal import SIGKILL, SIGTERM, Signals

import requests
import yaml
from ducktape.cluster.cluster import ClusterNode
from ducktape.cluster.remoteaccount import RemoteAccount, RemoteCommandError
from ducktape.errors import TimeoutError
from ducktape.services.service import Service
from ducktape.tests.test import TestContext
from ducktape.utils.local_filesystem_utils import mkdir_p
from ducktape.utils.util import wait_until
from prometheus_client.metrics_core import Metric
from prometheus_client.parser import text_string_to_metric_families

from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.python_librdkafka import PythonLibrdkafka
from rptest.clients.rpk import RpkTool
from rptest.clients.rpk_remote import RpkRemoteTool
from rptest.services import redpanda_types, tls
from rptest.services.admin import Admin
from rptest.services.redpanda_installer import (
    VERSION_RE as RI_VERSION_RE,
)
from rptest.services.redpanda_installer import (
    RedpandaInstaller,
    RedpandaVersionTriple,
)
from rptest.services.redpanda_installer import (
    int_tuple as ri_int_tuple,
)
from rptest.services.redpanda_types import (
    CompiledLogAllowElem,
    CompiledLogAllowList,
    KafkaClientSecurity,
    LogAllowList,
    LogAllowListElem,
)
from rptest.services.rolling_restarter import RollingRestarter
from rptest.services.storage import ClusterStorage, NodeStorage
from rptest.services.storage_failure_injection import FailureInjectionConfig
from rptest.services.utils import LogSearchLocal, NodeCrash
from rptest.util import (
    inject_remote_script,
    wait_until_result,
    wait_until_with_progress_check,
    debounce,
    get_fips_mode,
)
from rptest.utils.mode_checks import in_fips_environment

T = TypeVar("T")
U = TypeVar("U")


class Partition(NamedTuple):
    topic: str
    index: int  # type: ignore existing name clash, fix later
    leader: ClusterNode | None
    replicas: list[ClusterNode]


class MetricSample(NamedTuple):
    sample: str
    """The metric name, i.e., the part before the `{` in Prometheus exposition format."""
    node: Any
    """The node the metric was queried from."""
    value: float
    """The value of the metric sample."""
    labels: dict[str, str]
    """The labels attached to the metric sample."""


@dataclass
class UsageStats:
    disk_bytes_read: int = 0
    disk_bytes_written: int = 0
    batches_read: int = 0
    batches_written: int = 0
    internal_rpc_bytes_recv: int = 0
    internal_rpc_bytes_sent: int = 0


class NodeNotFoundError(Exception):
    pass


SaslCredentials = redpanda_types.SaslCredentials

# Map of path -> (checksum, size)
FileToChecksumSize = dict[str, Tuple[str, int]]

# Map of node -> (path -> (arbitrary dictionary))
NodeConfigOverridesT = dict[ClusterNode, dict[str, Any]]

DEFAULT_LOG_ALLOW_LIST: list[CompiledLogAllowElem] = [
    # Tests may be run on workstations that do not have XFS filesystem volumes
    # for containers.
    # Pre-23.2 version of the message
    re.compile("not on XFS. This is a non-supported setup."),
    # >= 23.2 version of the message
    re.compile("not on XFS or ext4. This is a non-supported"),
    # >= 23.2.22, 23.3 version of the message
    re.compile("not XFS or ext4. This is a unsupported"),
    # This is expected when tests are intentionally run on low memory configurations
    re.compile(r"Memory: '\d+' below recommended"),
    # A client disconnecting is not bad behaviour on redpanda's part
    re.compile(r"kafka rpc protocol.*(Connection reset by peer|Broken pipe)"),
    # Redpanda upgrade tests will use the default location of rpk (/usr/bin/rpk)
    # which is not present in typical CI runs, which results in the following
    # error message from the debug bundle service
    re.compile(r"Current specified RPK location"),
    re.compile(r"Configuring topic .* with id .*SEGV.*"),
]

# Log errors that are expected in tests that restart nodes mid-test
RESTART_LOG_ALLOW_LIST = [
    re.compile(
        "(raft|rpc) - .*(disconnected_endpoint|Broken pipe|Connection reset by peer)"
    ),
    re.compile("raft - .*recovery append entries error.*client_request_timeout"),
    # cluster - rm_stm.cc:550 - Error "raft::errc:19" on replicating pid:{producer_identity: id=1, epoch=0} commit batch
    # raft::errc:19 is the shutdown error code, the transaction subsystem encounters this and logs at error level
    re.compile('Error "raft::errc:19" on replicating'),
]

# Log errors that are expected in chaos-style tests that e.g.
# stop redpanda nodes uncleanly
CHAOS_LOG_ALLOW_LIST = [
    # Unclean connection shutdown
    re.compile(
        "(raft|rpc) - .*(client_request_timeout|disconnected_endpoint|Broken pipe|Connection reset by peer)"
    ),
    # Failure to progress STMs promptly
    re.compile("raft::offset_monitor::wait_timed_out"),
    # storage - log_manager.cc:415 - Leftover staging file found, removing: /var/lib/redpanda/data/kafka/__consumer_offsets/15_320/0-1-v1.log.staging
    re.compile("storage - .*Leftover staging file"),
    # e.g. cluster - controller_backend.cc:466 - exception while executing partition operation: {type: update_finished, ntp: {kafka/test-topic-1944-1639161306808363/1}, offset: 413, new_assignment: { id: 1, group_id: 65, replicas: {{node_id: 3, shard: 2}, {node_id: 4, shard: 2}, {node_id: 1, shard: 0}} }, previous_assignment: {nullopt}} - std::__1::__fs::filesystem::filesystem_error (error system:39, filesystem error: remove failed: Directory not empty [/var/lib/redpanda/data/kafka/test-topic-1944-1639161306808363])
    re.compile("cluster - .*Directory not empty"),
    re.compile("r/heartbeat - .*cannot find consensus group"),
    re.compile(
        r"cluster - .*exception while executing partition operation:.*std::exception \(std::exception\)"
    ),
    # Failure to handle an internal RPC because the RPC server already handles connections but doesn't yet handle this method. This can happen while the node is still starting up/restarting.
    # e.g. "admin_api_server - server.cc:655 - [_anonymous] exception intercepted - url: [http://ip-172-31-9-208:9644/v1/brokers/7/decommission] http_return_status[500] reason - seastar::httpd::server_error_exception (Unexpected error: rpc::errc::method_not_found)"
    re.compile("admin_api_server - .*Unexpected error: rpc::errc::method_not_found"),
]

# Log errors are used in node_operation_fuzzy_test and partition_movement_test
PREV_VERSION_LOG_ALLOW_LIST = [
    # e.g. cluster - controller_backend.cc:400 - Error while reconciling topics - seastar::abort_requested_exception (abort requested)
    "cluster - .*Error while reconciling topic.*",
    # Typo fixed in recent versions.
    # e.g.  raft - [follower: {id: {1}, revision: {10}}] [group_id:3, {kafka/topic/2}] - recovery_stm.cc:422 - recovery append entries error: raft group does not exists on target broker
    "raft - .*raft group does not exists on target broker",
    # e.g. rpc - Service handler thrown an exception - seastar::gate_closed_exception (gate closed)
    "rpc - .*gate_closed_exception.*",
    # Tests on mixed versions will start out with an unclean restart before
    # starting a workload.
    "(raft|rpc) - .*(disconnected_endpoint|Broken pipe|Connection reset by peer)",
    # e.g.  raft - [group_id:3, {kafka/topic/2}] consensus.cc:2317 - unable to replicate updated configuration: raft::errc::replicated_entry_truncated
    "raft - .*unable to replicate updated configuration: .*",
    # e.g. recovery_stm.cc:432 - recovery append entries error: rpc::errc::client_request_timeout"
    "raft - .*recovery append entries error.*client_request_timeout",
    # We added a condition to log these storage parser errors at `DEBUG` during
    # recovery here: https://github.com/redpanda-data/redpanda/pull/27287
    # However, old versions will still log at `ERROR` when there are not enough bytes
    # left due to unclean shutdown in a segment being recovered. Ignore these
    # in a mixed version test.
    "storage - .*parser::consume_records error: parser_errc::input_stream_not_enough_bytes .* storage::checksumming_consumer",
    # Failure to handle Schema Registry requests due to Redpanda being shutdown (fix is in https://github.com/redpanda-data/redpanda/pull/26909)
    "schemaregistry - .* - exception_reply: .*seastar::sleep_aborted",
]

AUDIT_LOG_ALLOW_LIST = RESTART_LOG_ALLOW_LIST + [
    re.compile(".*Failed to audit authentication.*"),
    re.compile(".*Failed to append authz event to audit log.*"),
    re.compile(".*Failed to append authentication event to audit log.*"),
    re.compile(".*Failed to audit authorization request for endpoint.*"),
    re.compile(
        ".*Request to authorize user to modify or view cluster configuration was not audited.*"
    ),
]

# Path to the LSAN suppressions file
LSAN_SUPPRESSIONS_FILE = "/opt/lsan_suppressions.txt"

# Path to the UBSAN suppressions file
UBSAN_SUPPRESSIONS_FILE = "/opt/ubsan_suppressions.txt"

FAILURE_INJECTION_LOG_ALLOW_LIST = [
    re.compile(
        "Assert failure: .* filesystem error: Injected Failure: Input/output error"
    ),
    re.compile("assert - Backtrace:"),
    re.compile("finject - .* flush called concurrently with other operations"),
    re.compile("crash reason to crash file.*(vassert)"),
]


class RemoteClusterNode(Protocol):
    account: RemoteAccount

    @property
    def name(self) -> str: ...


@dataclass
class MetricSamples:
    samples: list[MetricSample]

    def label_filter(self, labels: Mapping[str, str]) -> "MetricSamples":
        def f(sample: MetricSample) -> bool:
            for key, value in labels.items():
                assert key in sample.labels
                return sample.labels[key] == value
            # TODO: above logic looks wrong, it only matches the first label in labels
            assert False, "unreachable"

        return MetricSamples([s for s in filter(f, self.samples)])


class MetricsEndpoint(Enum):
    METRICS = "metrics"
    PUBLIC_METRICS = "public_metrics"


def prepare_allow_list(allow_list: LogAllowList) -> CompiledLogAllowList:
    def maybe_compile(a: LogAllowListElem) -> CompiledLogAllowElem:
        return re.compile(a) if isinstance(a, str) else a

    return DEFAULT_LOG_ALLOW_LIST + list(map(maybe_compile, allow_list))


def one_or_many(value: Any) -> Any:
    """
    Helper for reading `one_or_many_property` configs when
    we only care about getting one value out
    """
    if isinstance(value, list):
        return cast(Any, value[0])
    else:
        return value


def get_cloud_provider() -> str:
    """
    Returns the cloud provider in use.  If one is not set then return 'docker'
    """
    return os.getenv("CLOUD_PROVIDER", "docker")


class ResourceSettings:
    """
    Control CPU+memory footprint of Redpanda instances.  Pass one
    of these into your RedpandaTest constructor if you want to e.g.
    create low-memory situations.

    This class also contains defaults for redpanda CPU and memory
    sizing in tests.  If `dedicated_node` is true, even these limits
    are ignored, and Redpanda is allowed to behave in its default
    way of taking all the CPU and memory on the machine.  The
    `dedicated_node` mode is appropriate when using e.g. whole EC2 instances
    as test nodes.
    """

    DEFAULT_NUM_CPUS = 2
    DEFAULT_MEMORY_MB = 2048

    def __init__(
        self,
        *,
        num_cpus: int | None = None,
        memory_mb: int | None = None,
        bypass_fsync: bool | None = None,
        nfiles: int | None = None,
        reactor_stall_threshold: int | None = None,
        core_dump_limit: str | None = "unlimited",
    ) -> None:
        self._num_cpus = num_cpus
        self._memory_mb = memory_mb

        if bypass_fsync is None:
            self._bypass_fsync = False
        else:
            self._bypass_fsync = bypass_fsync

        self._nfiles = nfiles
        self._reactor_stall_threshold = reactor_stall_threshold
        self._core_dump_limit = core_dump_limit

    @property
    def memory_mb(self) -> int | None:
        return self._memory_mb

    @property
    def num_cpus(self) -> int | None:
        return self._num_cpus

    @property
    def core_dump_limit(self) -> str | None:
        return self._core_dump_limit

    @core_dump_limit.setter
    def core_dump_limit(self, value: str | None) -> None:
        self._core_dump_limit = value

    def to_cli(self, *, dedicated_node: bool) -> Tuple[str, str]:
        """

        Generate Redpanda CLI flags based on the settings passed in at construction
        time.

        :return: 2 tuple of strings, first goes before the binary, second goes after it
        """
        preamble = f"ulimit -Sc {self._core_dump_limit}"
        preamble += f" -Sn {self._nfiles}; " if self._nfiles else "; "

        if self._num_cpus is None and not dedicated_node:
            num_cpus = self.DEFAULT_NUM_CPUS
        else:
            num_cpus = self._num_cpus

        if self._memory_mb is None and not dedicated_node:
            memory_mb = self.DEFAULT_MEMORY_MB
        else:
            memory_mb = self._memory_mb

        args: list[str] = []
        if not dedicated_node:
            args.extend(
                [
                    "--kernel-page-cache=true",
                    "--overprovisioned ",
                    "--reserve-memory=0M",
                ]
            )

        if self._reactor_stall_threshold is not None:
            args.append(f"--blocked-reactor-notify-ms={self._reactor_stall_threshold}")

        if num_cpus is not None:
            args.append(f"--smp={num_cpus}")
        if memory_mb is not None:
            args.append(f"--memory={memory_mb}M")

        args.append(f"--unsafe-bypass-fsync={'1' if self._bypass_fsync else '0'}")

        return preamble, " ".join(args)


class TLSProvider:
    """
    Interface that RedpandaService uses to obtain TLS certificates.
    """

    @property
    def ca(self) -> tls.CertificateAuthority:
        raise NotImplementedError("ca")

    def create_broker_cert(
        self, service: Service, node: ClusterNode
    ) -> tls.Certificate:
        """
        Create a certificate for a broker.
        """
        raise NotImplementedError("create_broker_cert")

    def create_service_client_cert(
        self, service: Service, name: str
    ) -> tls.Certificate:
        """
        Create a certificate for an internal service client.
        """
        raise NotImplementedError("create_service_client_cert")

    def use_pkcs12_file(self) -> bool:
        """
        Use the generated PKCS#12 file instead of the key/cert
        """
        return False

    def p12_password(self, node: ClusterNode) -> str:
        """
        Get the PKCS#12 file password for the node
        """
        raise NotImplementedError("p12_password")


class SaslMechanismOverride(TypedDict):
    listener: str
    sasl_mechanisms: list[str]


class SecurityConfig:
    # the system currently has a single principal mapping rule. this is
    # sufficient to get our first mTLS tests put together, but isn't general
    # enough to cover all the cases. one awkward thing that came up was: admin
    # clients used by the service and clients used by tests both may need to
    # have mappings. the internal admin client can use something fixed, but
    # tests may want to use a variety of rules. currently it is hard to combine
    # the rules, so instead we use a fixed mapping and arrange for certs to use
    # a similar format. this will change when we get closer to GA and the
    # configuration becomes more general.
    __DEFAULT_PRINCIPAL_MAPPING_RULES = "RULE:^O=Redpanda,CN=(.*?)$/$1/L, DEFAULT"

    def __init__(self) -> None:
        self.enable_sasl = False
        self.kafka_enable_authorization: bool | None = None
        self.sasl_mechanisms: list[str] | None = None
        self.sasl_mechanisms_overrides: list[SaslMechanismOverride] | None = None
        self.http_authentication: list[str] | None = None
        self.endpoint_authn_method: str | None = None
        self.tls_provider: TLSProvider | None = None
        self.require_client_auth: bool = True
        self.auto_auth: bool | None = None

        # The rules to extract principal from mtls
        self.principal_mapping_rules = self.__DEFAULT_PRINCIPAL_MAPPING_RULES

    # sasl is required
    def sasl_enabled(self) -> bool:
        return (
            self.kafka_enable_authorization is None
            and self.enable_sasl
            and self.endpoint_authn_method is None
        ) or self.endpoint_authn_method == "sasl"

    # principal is extracted from mtls distinguished name
    def mtls_identity_enabled(self) -> bool:
        return self.endpoint_authn_method == "mtls_identity"


class LoggingConfig:
    # A dictionary that maps logger names to the Redpanda version in which they
    # were introduced. If a logger is not present in this dictionary, it is
    # assumed it was always supported/does not require special handling.
    LOGGER_GENESIS: dict[str, RedpandaVersionTriple] = {}

    def __init__(self, default_level: str, logger_levels: dict[str, str] = {}) -> None:
        self.default_level = default_level
        self.logger_levels = logger_levels

    def enable_finject_logging(self) -> None:
        self.logger_levels["finject"] = "trace"

    def to_args(self, redpanda_version: RedpandaVersionTriple | None = None) -> str:
        """
        Generate redpanda CLI arguments for this logging config

        To support running tests with mixed versions of redpanda, we need to
        be able to generate the correct CLI arguments for the version of
        redpanda we are running against.

        If no version information is provided, we assume that we run against
        dev and all loggers are supported.

        If a logger is not present in the `LOGGER_GENESIS` dictionary, it
        is assumed it was always supported/does not require special handling.

        :return: string
        """
        args = f"--default-log-level {self.default_level}"
        if self.logger_levels:
            levels_arg = ":".join(
                [
                    f"{k}={v}"
                    for k, v in self.logger_levels.items()
                    if not redpanda_version
                    or redpanda_version >= self.LOGGER_GENESIS.get(k, (0, 0, 0))
                ]
            )
            args += f" --logger-log-level={levels_arg}"

        return args


class AuthConfig:
    def __init__(self) -> None:
        self.authn_method: str | None = None


class TlsConfig(AuthConfig):
    def __init__(self) -> None:
        super(TlsConfig, self).__init__()
        self.server_key: str | None = None
        self.server_crt: str | None = None
        self.truststore_file: str | None = None
        self.crl_file: str | None = None
        self.client_key: str | None = None
        self.client_crt: str | None = None
        self.require_client_auth: bool = True
        self.enable_broker_tls: bool = True

    def maybe_write_client_certs(
        self,
        node: ClusterNode,
        logger: Logger,
        tls_client_key_file: str,
        tls_client_crt_file: str,
    ) -> None:
        if self.client_key is not None:
            logger.info(f"Writing client tls key file: {tls_client_key_file}")
            logger.debug(open(self.client_key, "r").read())
            node.account.mkdirs(os.path.dirname(tls_client_key_file))
            node.account.copy_to(self.client_key, tls_client_key_file)

        if self.client_crt is not None:
            logger.info(f"Writing client tls crt file: {tls_client_crt_file}")
            logger.debug(open(self.client_crt, "r").read())
            node.account.mkdirs(os.path.dirname(tls_client_crt_file))
            node.account.copy_to(self.client_crt, tls_client_crt_file)


class PandaproxyConfig(TlsConfig):
    PP_TLS_CLIENT_KEY_FILE = "/etc/redpanda/pp_client.key"
    PP_TLS_CLIENT_CRT_FILE = "/etc/redpanda/pp_client.crt"

    def __init__(self) -> None:
        super(PandaproxyConfig, self).__init__()
        self.cache_keep_alive_ms: int | None = 300000
        self.cache_max_size: int | None = 10
        self.advertised_api_host: str | None = None


class SchemaRegistryConfig(TlsConfig):
    SR_TLS_CLIENT_KEY_FILE = "/etc/redpanda/sr_client.key"
    SR_TLS_CLIENT_CRT_FILE = "/etc/redpanda/sr_client.crt"

    mode_mutability = True

    def __init__(self) -> None:
        super(SchemaRegistryConfig, self).__init__()


class AuditLogConfig(TlsConfig):
    AUDIT_LOG_TLS_CLIENT_KEY_FILE = "/etc/redpanda/audit_log_client.key"
    AUDIT_LOG_TLS_CLIENT_CRT_FILE = "/etc/redpanda/audit_log_client.crt"

    def __init__(
        self,
        listener_port: int | None = None,
        listener_authn_method: str | None = None,
    ) -> None:
        super(AuditLogConfig, self).__init__()
        self.listener_port = listener_port
        self.listener_authn_method = listener_authn_method


class RpkNodeConfig:
    def __init__(self) -> None:
        self.ca_file: str | None = None


class RedpandaServiceConstants:
    SUPERUSER_CREDENTIALS: SaslCredentials = SaslCredentials(
        "admin", "admin1234567890", "SCRAM-SHA-256"
    )

    # Internal namespace (model::kafka_internal_namespace) hosting system topics
    # such as transactions, id allocator, etc.
    KAFKA_INTERNAL_NAMESPACE = "kafka_internal"
    # ID allocator topic (model::id_allocator_nt). Created lazily on the first
    # init_producer_id request, so it may appear in the data directory even on
    # an otherwise-idle cluster.
    ID_ALLOCATOR_TOPIC = "id_allocator"


class RedpandaServiceABC(ABC, RedpandaServiceConstants):
    """A base class for all Redpanda services. This lowest-common denominator
    class has both implementation and abstract methods, only for methods which
    can be implemented by all services. Any methods which the service should
    implement should be @abstractmethod in order to ensure they are implemented
    by base classes.

    If a method can be implemented by more than one service implementation, but
    not all of them, it does NOT belong here.
    """

    context: TestContext

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._usage_stats = UsageStats()
        self._max_workers: int | None = None

    @property
    @abstractmethod
    def logger(self) -> Logger:
        pass

    @property
    def usage_stats(self) -> UsageStats:
        """
        Returns usage statistics for Redpanda. The statistics contains
        information about Redpanda service resource usage. f.e. total amount of
        bytes read/written during the test.
        """
        return self._usage_stats

    @property
    def usage_stats_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self._usage_stats)

    @abstractmethod
    def all_up(self) -> bool:
        pass

    @abstractmethod
    def raise_on_bad_logs(
        self, allow_list: LogAllowList = (), test_start_time: float = 0
    ) -> None:
        pass

    @abstractmethod
    def all_nodes_abc(self) -> list[ClusterNode]:
        """Return a list of all nodes in the cluster.
        This is mostly useful within the ABC itself in order to implement methods
        which pass though all nodes to implementation methods when the user does
        not specify a node."""
        pass

    @abstractmethod
    def kafka_client_security(self) -> KafkaClientSecurity:
        """Return a KafkaClientSecurity object suitable for connecting to the Kafka API
        on this broker."""
        pass

    def export_cluster_config(self) -> None:
        self.logger.debug("export_cluster_config not implemented for this service")

    def wait_until(
        self,
        fn: Callable[[], Any],
        timeout_sec: float,
        backoff_sec: float,
        err_msg: str | Callable[[], str] = "",
        retry_on_exc: bool = False,
    ) -> None:
        """
        Cluster-aware variant of wait_until, which will fail out
        early if a node dies.

        This is useful for long waits, which would otherwise not notice
        a test failure until the end of the timeout, even if redpanda
        already crashed.
        """

        t_initial = time.time()
        # How long to delay doing redpanda liveness checks, to make short waits more efficient
        grace_period = 15

        def wrapped():
            r = fn()
            if not r and time.time() > t_initial + grace_period:
                # Check the cluster is up before waiting + retrying
                assert (
                    self.all_up()
                    or getattr(self, "_tolerate_crashes", False)
                    or getattr(self, "tolerate_not_running", 0) > 0
                )
            return r

        wait_until(
            wrapped,
            timeout_sec=timeout_sec,
            backoff_sec=backoff_sec,
            err_msg=err_msg,
            retry_on_exc=retry_on_exc,
        )

    def wait_until_with_progress_check(
        self,
        check: Callable[[], Any],
        condition: Callable[[], Any],
        timeout_sec: float,
        progress_sec: float,
        backoff_sec: float,
        err_msg: str | None = None,
        logger: Logger | None = None,
    ) -> None:
        """
        Cluster-aware variant of wait_until_with_progress_check, which will
        fail out early if a node dies.

        This is useful for long waits, which would otherwise not notice
        a test failure until the end of the timeout, even if redpanda
        already crashed.
        """
        t_initial = time.time()
        # How long to delay doing redpanda liveness checks, to make short waits more efficient
        grace_period = 15

        def wrapped():
            r = condition()
            if not r and time.time() > t_initial + grace_period:
                # Check the cluster is up before waiting + retrying
                assert (
                    self.all_up()
                    or getattr(self, "_tolerate_crashes", False)
                    or getattr(self, "tolerate_not_running", 0) > 0
                )
            return r

        wait_until_with_progress_check(
            check,
            wrapped,
            timeout_sec,
            progress_sec,
            backoff_sec,
            err_msg=err_msg,
            logger=logger,
        )

    def for_nodes(self, nodes: Collection[U], cb: Callable[[U], T]) -> list[T]:
        if len(nodes) > 0:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self._max_workers
            ) as executor:
                # The list() wrapper is to cause futures to be evaluated here+now
                # (including throwing any exceptions) and not just spawned in background.
                return list(executor.map(cb, nodes))
        else:
            return []

    def _extract_samples(
        self, metrics: list[Metric], sample_pattern: str, node: Any
    ) -> list[MetricSample]:
        """Extract metrics samples given a sample pattern. Embed the node in which it came from."""
        found_sample = None
        sample_values: list[MetricSample] = []

        for family in metrics:
            for sample in family.samples:
                if sample_pattern not in sample.name:
                    continue
                if not found_sample:
                    found_sample = (family.name, sample.name)
                if found_sample != (family.name, sample.name):
                    raise Exception(
                        f"More than one metric matched '{sample_pattern}'. Found {found_sample} and {(family.name, sample.name)}"
                    )
                sample_values.append(
                    MetricSample(sample.name, node, sample.value, sample.labels)
                )

        return sample_values

    @abstractmethod
    def metrics(
        self,
        node: ClusterNode,
        metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS,
        name: str | None = None,
    ) -> list[Metric]:
        """Query and return all metrics from the given node's metrics endpoint.

        :name: If not None, only return metrics matching this exact name."""
        pass

    def metrics_sample(
        self,
        sample_pattern: str = "",
        nodes: list[ClusterNode] | None = None,
        metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS,
        *,
        name: str = "",
    ) -> MetricSamples | None:
        """Query the given metrics endpoint for a single metric name. Returns
        all series for this metric (i.e., all label combinations, on every node).

        :param sample_pattern: Substring to match against metric names. Fuzzy
            matching is used; any metric whose name contains this string will
            be considered a match.
            Exactly one of ``name`` or ``sample_pattern`` must be provided.
        :param nodes: List of nodes to query. If ``None``, all nodes in the
            cluster are queried.
        :param metrics_endpoint: The metrics endpoint to query.
        :param name: If provided, performs exact matching for the metric name.
            If not provided, fuzzy matching is used via ``sample_pattern``.
            Exactly one of ``name`` or ``sample_pattern`` must be provided.

        :returns: MetricSamples for the matched metric, or ``None`` if no
            metric matches the pattern. If ``name`` is provided and no metric
            is found, an exception is raised.
        :rtype: MetricSamples | None

        This either uses fuzzy matching (if sample_pattern is used) or
        exact matching (if name is used) to find the metric.

        Fuzzy matching matches any metric whose name contains the given
        sample_pattern (which is a plain string, not a regex or glob).

        This interface matches the sample pattern against metric names, and
        requires that exactly one metric name match the query (though this name
        may be associated with multiple metric values with varying labels). All
        series for this name are returned, no aggregation is performed.

        None will be returned if no metric matches the pattern. An
        exception will be raised if two or more metric names match the pattern.

        For example, the query:

            redpanda.metrics_sample("under_replicated")

        will return an array containing MetricSample instances for each node and
        core/shard in the cluster. Each entry will correspond to the sum of
        the "vectorized_cluster_partition_under_replicated_replicas" summed across
        all shards.
        """

        # check that name/sample_pattern usage is correct
        if not name and not sample_pattern:
            raise ValueError(
                "Either 'name' or 'sample_pattern' must be provided (neither were)"
            )

        if name and sample_pattern:
            raise ValueError(
                "Either 'name' or 'sample_pattern' must be provided (both were)"
            )

        key = name if name else sample_pattern
        names, sample_patterns = ([name], []) if name else ([], [sample_pattern])

        # delegate to metrics_samples for implementation
        samples = self.metrics_samples(
            sample_patterns, nodes, metrics_endpoint, names=names
        )

        if len(samples) == 0:
            return None
        else:
            assert len(samples) == 1 and key in samples, f"{samples}"
            return samples[key]

    def metrics_samples(
        self,
        sample_patterns: Iterable[str] = (),
        nodes: list[ClusterNode] | None = None,
        metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS,
        *,
        names: Iterable[str] = (),
    ) -> dict[str, MetricSamples]:
        """Implement this method to iterate over nodes to query multiple sample patterns.

        Query metrics for multiple sample names using fuzzy matching.
        Similar to metrics_sample(), but works with multiple patterns.
        """

        # check that name/sample_pattern usage is correct
        if not names and not sample_patterns:
            raise ValueError(
                "Either 'names' or 'sample_patterns' must be provided (neither were)"
            )

        if names and sample_patterns:
            raise ValueError(
                "Either 'names' or 'sample_patterns' must be provided (both were)"
            )

        patterns_or_name = sample_patterns if sample_patterns else names

        sample_values_per_pattern: dict[str, list[MetricSample]] = {
            pattern: [] for pattern in patterns_or_name
        }

        if nodes is None:
            nodes = self.all_nodes_abc()

        def fetch_node_samples(
            n: ClusterNode,
        ) -> dict[str, list[MetricSample]]:
            node_samples: dict[str, list[MetricSample]] = {
                pattern: [] for pattern in patterns_or_name
            }
            # if exact names are provided, we use RP-side filtering support to query
            # only for those metrics, which is much faster especially when many
            # metrics are involved
            if names:
                for name in names:
                    metrics = self.metrics(
                        n,
                        metrics_endpoint=metrics_endpoint,
                        name=name,
                    )
                    node_samples[name] = self._extract_samples(metrics, name, n)
            else:
                metrics = self.metrics(n, metrics_endpoint)
                for pattern in sample_patterns:
                    node_samples[pattern] = self._extract_samples(metrics, pattern, n)
            return node_samples

        for node_samples in self.for_nodes(nodes, fetch_node_samples):
            for pattern, samples in node_samples.items():
                sample_values_per_pattern[pattern] += samples

        return {
            pattern: MetricSamples(values)
            for pattern, values in sample_values_per_pattern.items()
            if values
        }

    @staticmethod
    def _adjust_metric_name(name: str, endpoint: MetricsEndpoint) -> str:
        """Adjust the metric name to be used in the __name__ filter."""

        # do a pre-check of the expected prefix to catch user errors early
        prefix = {
            MetricsEndpoint.METRICS: "vectorized_",
            MetricsEndpoint.PUBLIC_METRICS: "redpanda_",
        }[endpoint]

        assert name.startswith(prefix), (
            f"All {endpoint} names must start with {prefix}: {name}"
        )
        # we need to strip the prefix, because seastar __name__ filtering
        # works on the metric name without the prefix
        return name.removeprefix(prefix)

    @staticmethod
    def _metric_basename(name: str) -> str:
        """The prom text client has an annoying behavior for counter metrics:
        changes either the family or sample name by appending or removing _total,
        depending on if it is present.

        That is, ONLY for TYPE: counter metrics if the scaped metric name is foo, then
        in the Metric object m, m.family = foo, m.samples[].name = foo_total. If
        the metric name was instead foo_total, the result is actually the same! So
        you cannot tell what the _true_ metric name was from the Metric object.

        This interferes with matching metrics by name, since the __name__= filter
        on seastar needs the true metric name. So our policy: always strip _total
        when preparing the the name for querying and then check equality on family.name,
        which also doesn't include total. This means both metrics variations above can
        be successfully queried with foo or foo_total.

        This is *further* complicated by the fact that we have non-counters, like gauges
        which also end in total, e.g., redpanda_application_uptime_seconds_total which
        is a gauge but ends in total. These don't get the above treatment: the family
        and sample name will be the true metric names. So when matching we also strip
        _total from the returned family name to handle this case (essentially we always
        compare the base (stripped) names on both sides, even when that isn't necessary
        because the metric is not a counter - but we don't know it's a counter until
        the query has been returned and the filter applied).

        That's not necessarily desirable, but it's the best we can do without changing
        the prometheus text parser (i.e., using our own).

        See: prometheus_client/parser.py.text_fd_to_metric_families.build_metric"""
        return name.removesuffix("_total")

    def metric_sum(
        self,
        metric_name: str,
        metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS,
        namespace: str | None = None,
        topic: str | None = None,
        nodes: list[ClusterNode] | None = None,
        expect_metric: bool = False,
    ) -> float:
        """Pings the 'metrics_endpoint' of each node and returns the summed values
        of the given metric, optionally filtering by namespace and topic.
        """

        if nodes is None:
            nodes = self.all_nodes_abc()

        basename = self._metric_basename(metric_name)

        @dataclass
        class Result:
            value: float
            metric_seen: bool
            matched_families: set[str]

        def sum_for_node(n: ClusterNode) -> Result:
            value = 0.0
            metric_seen = False
            matched_families: set[str] = set()
            metrics = self.metrics(
                n, metrics_endpoint=metrics_endpoint, name=basename + "*"
            )
            for family in metrics:
                if self._metric_basename(family.name) != basename:
                    continue
                matched_families.add(family.name)
                for sample in family.samples:
                    labels = sample.labels
                    if namespace:
                        assert (
                            "redpanda_namespace" in labels or "namespace" in labels
                        ), f"Missing namespace label: {sample}"
                        if (
                            labels.get("redpanda_namespace", labels.get("namespace"))
                            != namespace
                        ):
                            continue
                    if topic:
                        assert "redpanda_topic" in labels or "topic" in labels, (
                            f"Missing topic label: {sample}"
                        )
                        if labels.get("redpanda_topic", labels.get("topic")) != topic:
                            continue
                    metric_seen = True
                    value += sample.value
            return Result(value, metric_seen, matched_families)

        value = 0.0
        metric_seen = False
        matched_families: set[str] = set()
        for r in self.for_nodes(nodes, sum_for_node):
            value += r.value
            metric_seen |= r.metric_seen
            matched_families |= r.matched_families

        # catch any weirdness, like if two metrics foo_total and foo both exist, which would
        # be ambiguous
        assert len(matched_families) <= 1, (
            f"More than one family matched: {matched_families}"
        )

        if expect_metric and not matched_families:
            assert metric_seen, f"Metric {metric_name} was not observed"

        return value


class CorruptedClusterError(Exception):
    """Throw to indicate a cluster is an unhealthy or otherwise unsuitable state to
    continue the remainder of the tests."""

    pass


class BrokerNotStartedError(Exception):
    """Thrown in cases asks about or tries to perform an operation on a
    broker that it is not started, as such an operation is bound to fail."""

    pass


class RedpandaService(Service, RedpandaServiceABC):
    PERSISTENT_ROOT = "/var/lib/redpanda"
    TRIM_LOGS_KEY = "trim_logs"
    DATA_DIR = os.path.join(PERSISTENT_ROOT, "data")
    CRASH_REPORTS = os.path.join(DATA_DIR, "crash_reports")
    NODE_CONFIG_FILE = "/etc/redpanda/redpanda.yaml"
    RPK_CONFIG_FILE = "/root/.config/rpk/rpk.yaml"
    CLUSTER_BOOTSTRAP_CONFIG_FILE = "/etc/redpanda/.bootstrap.yaml"
    TLS_SERVER_KEY_FILE = "/etc/redpanda/server.key"
    TLS_SERVER_CRT_FILE = "/etc/redpanda/server.crt"
    TLS_SERVER_P12_FILE = "/etc/redpanda/server.p12"
    TLS_CA_CRT_FILE = "/etc/redpanda/ca.crt"
    TLS_CA_CRL_FILE = "/etc/redpanda/ca.crl"
    SYSTEM_TLS_CA_CRT_FILE = "/usr/local/share/ca-certificates/ca.crt"
    STDOUT_STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "redpanda.log")
    BACKTRACE_CAPTURE = os.path.join(PERSISTENT_ROOT, "redpanda_backtrace.log")
    COVERAGE_PROFRAW_CAPTURE = os.path.join(PERSISTENT_ROOT, "redpanda.profraw")
    TEMP_OSSL_CONFIG_FILE = "/etc/openssl.cnf"
    DEFAULT_NODE_READY_TIMEOUT_SEC = 40
    NODE_READY_TIMEOUT_MIN_SEC_KEY = "node_ready_timeout_min_sec"
    DEDICATED_NODE_KEY = "dedicated_nodes"
    RAISE_ON_ERRORS_KEY = "raise_on_error"
    LOG_LEVEL_KEY = "redpanda_log_level"
    DEFAULT_LOG_LEVEL = "info"
    COV_KEY = "enable_cov"
    DEFAULT_COV_OPT = "OFF"

    # Where we put a compressed binary if saving it after failure
    EXECUTABLE_SAVE_PATH = "/tmp/redpanda.gz"

    FAILURE_INJECTION_CONFIG_PATH = "/etc/redpanda/failure_injection_config.json"

    OPENSSL_CONFIG_FILE_BASE = "openssl/openssl.cnf"
    OPENSSL_MODULES_PATH_BASE = "lib/ossl-modules/"

    # When configuring multiple listeners for testing, a secondary port to use
    # instead of the default.
    KAFKA_ALTERNATE_PORT = 9093
    KAFKA_KERBEROS_PORT = 9094
    ADMIN_ALTERNATE_PORT = 9647

    GLOBAL_USE_STRESS_FIBER = "enable_stress_fiber"
    GLOBAL_NUM_STRESS_FIBERS = "num_stress_fibers"
    GLOBAL_STRESS_FIBER_MIN_MS = "stress_fiber_min_ms"
    GLOBAL_STRESS_FIBER_MAX_MS = "stress_fiber_max_ms"
    DEFAULT_USE_STRESS_FIBER = "OFF"
    DEFAULT_NUM_STRESS_FIBERS = 1
    DEFAULT_STRESS_FIBER_MIN_MS = 100
    DEFAULT_STRESS_FIBER_MAX_MS = 200

    CLUSTER_CONFIG_DEFAULTS: dict[str, Any] = {
        "join_retry_timeout_ms": 200,
        "default_topic_partitions": 4,
        "enable_metrics_reporter": False,
        "superusers": [RedpandaServiceConstants.SUPERUSER_CREDENTIALS[0]],
        # Disable segment size jitter to make tests more deterministic if they rely on
        # inspecting storage internals (e.g. number of segments after writing a certain
        # amount of data).
        "log_segment_size_jitter_percent": 0,
        # This is high enough not to interfere with the logic in any tests, while also
        # providing some background coverage of the connection limit code (i.e. that it
        # doesn't crash, it doesn't limit when it shouldn't)
        "kafka_connections_max": 2048,
        "kafka_connections_max_per_ip": 1024,
        "kafka_connections_max_overrides": ["1.2.3.4:5"],
        # configure shutdown watchdog timeout to 20 seconds to give it a chance
        # to fire before Redpanda node that doesn't stopped is killed
        "partition_manager_shutdown_watchdog_timeout": 20000,
    }

    logs = {
        "redpanda_start_stdout_stderr": {
            "path": STDOUT_STDERR_CAPTURE,
            "collect_default": True,
        },
        "code_coverage_profraw_file": {
            "path": COVERAGE_PROFRAW_CAPTURE,
            "collect_default": True,
        },
        "executable": {"path": EXECUTABLE_SAVE_PATH, "collect_default": False},
        "backtraces": {"path": BACKTRACE_CAPTURE, "collect_default": True},
        "crash_reports": {"path": CRASH_REPORTS, "collect_default": True},
    }

    # Thread name of shards to be used with redpanda_tid()
    SHARD_0_THREAD_NAME = "redpanda"
    SHARD_1_THREAD_NAME = "reactor-1"

    nodes: list[ClusterNode]

    def __init__(
        self,
        context: TestContext,
        num_brokers: int,
        *,
        cluster_spec: Any = None,
        extra_rp_conf: dict[str, Any] | None = None,
        extra_node_conf: dict[str, Any] | None = None,
        resource_settings: ResourceSettings | None = None,
        log_level: str | None = None,
        log_config: LoggingConfig | None = None,
        environment: dict[str, str] | None = None,
        security: SecurityConfig = SecurityConfig(),
        node_ready_timeout_s: int | None = None,
        superuser: SaslCredentials | None = None,
        skip_if_no_redpanda_log: bool = False,
        pandaproxy_config: PandaproxyConfig | None = None,
        schema_registry_config: SchemaRegistryConfig | None = None,
        audit_log_config: AuditLogConfig | None = None,
        rpk_node_config: RpkNodeConfig | None = None,
    ) -> None:
        super().__init__(context, num_nodes=num_brokers, cluster_spec=cluster_spec)

        self._context = context
        self._extra_rp_conf = extra_rp_conf or dict()

        if superuser is None:
            superuser = self.SUPERUSER_CREDENTIALS
            self._skip_create_superuser = False
        else:
            # When we are passed explicit superuser credentials, presume that the caller
            # is taking care of user creation themselves (e.g. when testing credential bootstrap)
            self._skip_create_superuser = True

        self._superuser = superuser

        self._admin = Admin(
            self, auth=(self._superuser.username, self._superuser.password)
        )

        if resource_settings is None:
            resource_settings = ResourceSettings()
        self._resource_settings = resource_settings

        self._trim_logs = self._context.globals.get(self.TRIM_LOGS_KEY, True)

        self._node_id_by_idx: dict[int, int] = {}
        self._security_config: dict[str, str | int] = {}

        self._skip_if_no_redpanda_log = skip_if_no_redpanda_log

        self._dedicated_nodes: bool = self._context.globals.get(
            self.DEDICATED_NODE_KEY, False
        )

        self.logger.info(f"ResourceSettings: dedicated_nodes={self._dedicated_nodes}")

        self._security = security
        self._installer: RedpandaInstaller = RedpandaInstaller(self)
        self._pandaproxy_config = pandaproxy_config
        self._schema_registry_config = schema_registry_config
        self._audit_log_config = audit_log_config
        self._failure_injection_enabled = False
        # Tolerate redpanda nodes not running during health checks. This is
        # useful when running i.e. with node_operations.FailureInjectorBackgroundThread
        # which can kill redpanda nodes.
        # This is a number to allow multiple callers to set it.
        self.tolerate_not_running = 0
        self._tolerate_crashes = False
        self._rpk_node_config = rpk_node_config

        if node_ready_timeout_s is None:
            node_ready_timeout_s = RedpandaService.DEFAULT_NODE_READY_TIMEOUT_SEC
        # apply min timeout rule. some tests may override this with larger
        # timeouts, so take the maximum.
        node_ready_timeout_min_s = self._context.globals.get(
            self.NODE_READY_TIMEOUT_MIN_SEC_KEY, node_ready_timeout_s
        )
        node_ready_timeout_s = max(node_ready_timeout_s, node_ready_timeout_min_s)
        self.node_ready_timeout_s = node_ready_timeout_s

        self._extra_node_conf: dict[ClusterNode, dict[str, Any]] = {}
        for node in self.nodes:
            self._extra_node_conf[node] = extra_node_conf or dict()

        if log_config is not None:
            self._log_config = log_config
        else:
            if log_level is None:
                self._log_level = self._context.globals.get(
                    self.LOG_LEVEL_KEY, self.DEFAULT_LOG_LEVEL
                )
            else:
                self._log_level = log_level
            self._log_config = LoggingConfig(
                self._log_level,
                {
                    "exception": "info",
                    "io": "debug",
                    "seastar_memory": "debug",
                    "dns_resolver": "info",
                },
            )

        self._started: Set[ClusterNode] = set()

        self._raise_on_errors = self._context.globals.get(
            self.RAISE_ON_ERRORS_KEY, True
        )

        # enable asan abort / core dumps by default
        self._environment = dict(
            ASAN_OPTIONS="abort_on_error=1:disable_coredump=0:unmap_shadow_on_exit=1"
        )

        # If lsan_suppressions.txt exists, then include it
        if os.path.exists(LSAN_SUPPRESSIONS_FILE):
            self.logger.debug(f"{LSAN_SUPPRESSIONS_FILE} exists")
            self._environment["LSAN_OPTIONS"] = f"suppressions={LSAN_SUPPRESSIONS_FILE}"
        else:
            self.logger.debug(f"{LSAN_SUPPRESSIONS_FILE} does not exist")

        # ubsan, halt at first violation and include a stack trace
        # in the logs.
        ubsan_opts = "print_stacktrace=1:halt_on_error=1:abort_on_error=1"
        if os.path.exists(UBSAN_SUPPRESSIONS_FILE):
            ubsan_opts += f":suppressions={UBSAN_SUPPRESSIONS_FILE}"

        self._environment["UBSAN_OPTIONS"] = ubsan_opts

        if environment is not None:
            self._environment.update(environment)

        self.config_file_lock = threading.Lock()

        self._saved_executable = False

        self._tls_cert = None
        self._init_tls()

        # Each time we start a node and write out its node_config (redpanda.yaml),
        # stash a copy here so that we can quickly look up e.g. addresses later.
        self._node_configs: dict[ClusterNode, dict[str, Any]] = {}

        self._seed_servers = self.nodes

        self._expect_max_controller_records = 1000

    def restart_nodes(
        self,
        nodes: ClusterNode | list[ClusterNode],
        override_cfg_params: dict[str, Any] | None = None,
        start_timeout: int | None = None,
        stop_timeout: int | None = None,
        auto_assign_node_id: bool = False,
        omit_seeds_on_idx_one: bool = True,
        extra_cli: list[str] = [],
    ) -> None:
        nodes = [nodes] if isinstance(nodes, ClusterNode) else nodes
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(nodes)) as executor:
            # The list() wrapper is to cause futures to be evaluated here+now
            # (including throwing any exceptions) and not just spawned in background.
            def stop_with_timeout(n: ClusterNode) -> None:
                self.stop_node(n, timeout=stop_timeout)

            def start_with_params(n: ClusterNode) -> None:
                self.start_node(
                    n,
                    override_cfg_params=override_cfg_params,
                    timeout=start_timeout,
                    auto_assign_node_id=auto_assign_node_id,
                    omit_seeds_on_idx_one=omit_seeds_on_idx_one,
                    extra_cli=extra_cli,
                )

            list(executor.map(stop_with_timeout, nodes))
            list(executor.map(start_with_params, nodes))

    def set_extra_rp_conf(self, conf: dict[str, Any]):
        self._extra_rp_conf = conf

    def use_stress_fiber(self) -> bool:
        """Return true if the test should run with the stress fiber."""
        use_stress_fiber = self._context.globals.get(
            self.GLOBAL_USE_STRESS_FIBER, self.DEFAULT_USE_STRESS_FIBER
        )
        if use_stress_fiber == "ON":
            return True
        elif use_stress_fiber == "OFF":
            return False

        self.logger.warning(f"{self.GLOBAL_USE_STRESS_FIBER} should be 'ON', or 'OFF'")
        return False

    def get_stress_fiber_params(self) -> Tuple[int, int, int]:
        fibers = int(
            self._context.globals.get(
                self.GLOBAL_NUM_STRESS_FIBERS, self.DEFAULT_NUM_STRESS_FIBERS
            )
        )
        min_ms = int(
            self._context.globals.get(
                self.GLOBAL_STRESS_FIBER_MIN_MS, self.DEFAULT_STRESS_FIBER_MIN_MS
            )
        )
        max_ms = int(
            self._context.globals.get(
                self.GLOBAL_STRESS_FIBER_MAX_MS, self.DEFAULT_STRESS_FIBER_MAX_MS
            )
        )
        return (fibers, min_ms, max_ms)

    def add_extra_rp_conf(self, conf: dict[str, Any]):
        self._extra_rp_conf = {**self._extra_rp_conf, **conf}

    def all_nodes_abc(self) -> list[ClusterNode]:
        return self.nodes

    def healthy(self):
        """
        A primitive health check on all the nodes which returns True when all
        nodes report that no under replicated partitions exist. This should
        later be replaced by a proper / official start-up probe type check on
        the health of a node after a restart.
        """

        def check_node(node: ClusterNode) -> tuple[int, bool]:
            """Returns (count, had_error)."""
            try:
                metrics = self.metrics(node)
            except Exception:
                return (0, True)
            count = 0
            for family in metrics:
                for sample in family.samples:
                    if (
                        sample.name
                        == "vectorized_cluster_partition_under_replicated_replicas"
                    ):
                        count += int(sample.value)
            return (count, False)

        results = self.for_nodes(self.nodes, check_node)
        return all(not had_error and count == 0 for count, had_error in results)

    def rolling_restart_nodes(
        self,
        nodes: ClusterNode | list[ClusterNode],
        override_cfg_params: dict[str, Any] | None = None,
        start_timeout: int | None = None,
        stop_timeout: int | None = None,
        use_maintenance_mode: bool = True,
        omit_seeds_on_idx_one: bool = True,
        auto_assign_node_id: bool = False,
    ):
        nodes = [nodes] if isinstance(nodes, ClusterNode) else nodes
        restarter = RollingRestarter(self)
        restarter.restart_nodes(
            nodes,
            override_cfg_params=override_cfg_params,
            start_timeout=start_timeout,
            stop_timeout=stop_timeout,
            use_maintenance_mode=use_maintenance_mode,
            omit_seeds_on_idx_one=omit_seeds_on_idx_one,
            auto_assign_node_id=auto_assign_node_id,
        )

    def set_resource_settings(self, rs: ResourceSettings):
        self._resource_settings = rs

    def trim_logs(self):
        if not self._trim_logs:
            return

        # Excessive logging may cause disks to fill up quickly.
        # Call this method to removes TRACE and DEBUG log lines from redpanda logs
        # Ensure this is only done on tests that have passed
        def prune(node: ClusterNode):
            node.account.ssh(
                f"sed -i -E -e '/TRACE|DEBUG/d' {RedpandaService.STDOUT_STDERR_CAPTURE} || true"
            )

        self.for_nodes(self.nodes, prune)

    def node_id(
        self, node: ClusterNode, force_refresh: bool = False, timeout_sec: int = 30
    ) -> int:
        """
        Returns the node ID (redpanda broker ID) of a given node. Uses a cached
        value if present unless 'force_refresh' is set to True.

        :param: force_refresh if True, always queries for the node ID from the node's
        API, never uses cached data.

        :param: timeout_sec number of seconds to try to find the node before giving up,
        which is relevant in the case there is no cached index <-> node info info and
        we are not able to contact some some. The full timeout may apply for each node
        in the cluster so the before this method returns may be up to timeout_sec *
        node_count. timeout_sec=0 means do not retry failed network calls.

        This throws BrokerNotStarted if the specified node is not started.

        NOTE: this is not thread-safe.
        """
        idx = self.idx(node)
        if not force_refresh:
            if idx in self._node_id_by_idx:
                return self._node_id_by_idx[idx]

        # fail immediately if node is not started, since we cannot possibly
        # fetch its ID in that case
        if node not in self._started:
            raise BrokerNotStartedError(f"node {node.name} not started")

        self.logger.debug(
            f"Fetching node ID (broker ID) for {node.name} (force_refresh={force_refresh})"
        )

        def _try_get_node_id():
            try:
                node_cfg = self._admin.get_node_config(node)
            except Exception as e:
                self.logger.debug(f"error in get_node_config for {node.name}: {e}")
                return (False, -1)
            return (True, node_cfg["node_id"])

        node_id = wait_until_result(
            _try_get_node_id,
            timeout_sec=timeout_sec,
            err_msg=f"couldn't reach admin endpoint for {node.account.hostname}",
        )
        self.logger.info(f"Got node ID for {node.account.hostname}: {node_id}")
        self._node_id_by_idx[idx] = node_id
        return node_id

    def kafka_client_security(self):
        if self._security_config:

            def get_str(key: str):
                v = self._security_config[key]
                assert isinstance(v, str)
                return v

            creds = SaslCredentials(
                username=get_str("sasl_plain_username"),
                password=get_str("sasl_plain_password"),
                algorithm=get_str("sasl_mechanism"),
            )
        else:
            creds = None

        return KafkaClientSecurity(creds, tls_enabled=False)

    def set_skip_if_no_redpanda_log(self, v: bool):
        self._skip_if_no_redpanda_log = v

    def raise_on_bad_logs(
        self, allow_list: LogAllowList = (), test_start_time: float = 0
    ):
        """
        Raise a BadLogLines exception if any nodes' logs contain errors not
        permitted by `allow_list`

        :param allow_list: LogAllowList of additional lines to ignore (default
            ignores are always included)
        """

        allow_list = prepare_allow_list(allow_list)

        def check_node(node: ClusterNode) -> tuple[str | None, ClusterNode] | None:
            if self._skip_if_no_redpanda_log and not node.account.exists(
                RedpandaService.STDOUT_STDERR_CAPTURE
            ):
                self.logger.info(
                    f"{RedpandaService.STDOUT_STDERR_CAPTURE} not found on {node.account.hostname}. Skipping log scan."
                )
                return None
            return (self.get_version_if_not_head(node), node)

        _searchable_nodes = [
            r for r in self.for_nodes(self.nodes, check_node) if r is not None
        ]

        lsearcher = LogSearchLocal(
            self._context,
            allow_list,
            self.logger,
            RedpandaService.STDOUT_STDERR_CAPTURE,
        )
        lsearcher.search_logs(_searchable_nodes)

    @property
    def dedicated_nodes(self):
        """
        If true, the nodes are dedicated linux servers, e.g. EC2 instances.

        If false, the nodes are containers that share CPUs and memory with
        one another.
        :return:
        """
        return self._dedicated_nodes

    def redpanda_env_preamble(self):
        # Pass environment variables via FOO=BAR shell expressions
        return " ".join([f"{k}={v}" for (k, v) in self._environment.items()])

    def set_seed_servers(self, node_list: list[ClusterNode]):
        assert len(node_list) > 0
        self._seed_servers = node_list

    def set_environment(self, environment: dict[str, str]):
        self._environment.update(environment)

    def unset_environment(self, keys: list[str]):
        for k in keys:
            try:
                del self._environment[k]
            except KeyError:
                pass

    def set_extra_node_conf(self, node: ClusterNode, conf: dict[str, Any]):
        assert node in self.nodes, f"Node {node.account.hostname} is not started"
        self._extra_node_conf[node] = conf

    def add_extra_node_conf(self, node: ClusterNode, conf: dict[str, Any]):
        assert node in self.nodes, f"Node {node.account.hostname} is not started"
        self._extra_node_conf[node] = {**self._extra_node_conf[node], **conf}

    def set_security_settings(self, settings: SecurityConfig):
        self._security = settings
        self._init_tls()

    def set_pandaproxy_settings(self, settings: PandaproxyConfig):
        self._pandaproxy_config = settings

    def set_schema_registry_settings(self, settings: SchemaRegistryConfig):
        self._schema_registry_config = settings

    def set_audit_log_settings(self, settings: AuditLogConfig):
        self._audit_log_config = settings

    def _init_tls(self):
        """
        Call this if tls setting may have changed.
        """
        if self._security.tls_provider:
            # build a cert for clients used internally to the service
            self._tls_cert = self._security.tls_provider.create_service_client_cert(
                self, "redpanda.service.admin"
            )

    def sasl_enabled(self):
        return self._security.sasl_enabled()

    def mtls_identity_enabled(self):
        return self._security.mtls_identity_enabled()

    def endpoint_authn_method(self):
        return self._security.endpoint_authn_method

    def require_client_auth(self):
        return self._security.require_client_auth

    def get_node_memory_mb(self) -> int:
        if self._resource_settings.memory_mb is not None:
            self.logger.info("get_node_memory_mb: got from ResourceSettings")
            return self._resource_settings.memory_mb
        elif self._dedicated_nodes is False:
            self.logger.info("get_node_memory_mb: using ResourceSettings default")
            return self._resource_settings.DEFAULT_MEMORY_MB
        else:
            self.logger.info("get_node_memory_mb: fetching from node")
            # Assume nodes are symmetric, so we can just ask one
            # how much memory it has.
            node = self.nodes[0]
            line = node.account.ssh_output(
                "cat /proc/meminfo | grep MemTotal", timeout_sec=10
            )
            # Output line is like "MemTotal:       32552236 kB"
            memory_kb = int(line.strip().split()[1])
            return memory_kb // 1024

    def get_node_cpu_count(self) -> int:
        if self._resource_settings.num_cpus is not None:
            self.logger.info("get_node_cpu_count: got from ResourceSettings")
            return self._resource_settings.num_cpus
        elif self._dedicated_nodes is False:
            self.logger.info("get_node_cpu_count: using ResourceSettings default")
            return self._resource_settings.DEFAULT_NUM_CPUS
        else:
            self.logger.info("get_node_cpu_count: fetching from node")

            # Assume nodes are symmetric, so we can just ask one
            node = self.nodes[0]
            core_count_str = node.account.ssh_output(
                "cat /proc/cpuinfo | grep ^processor | wc -l", timeout_sec=10
            )
            return int(core_count_str.strip())

    def get_node_disk_free(self):
        # Assume nodes are symmetric, so we can just ask one
        node = self.nodes[0]

        if node.account.exists(self.PERSISTENT_ROOT):
            df_path = self.PERSISTENT_ROOT
        else:
            # If dir doesn't exist yet, use the parent.
            df_path = os.path.dirname(self.PERSISTENT_ROOT)

        df_out = node.account.ssh_output(f"df --output=avail {df_path}", timeout_sec=10)

        avail_kb = int(df_out.strip().split(b"\n")[1].strip())

        if not self.dedicated_nodes:
            # Assume docker images share a filesystem.  This may not
            # be the truth (e.g. in CI they get indepdendent XFS
            # filesystems), but it's the safe assumption on e.g.
            # a workstation.
            avail_kb = int(avail_kb / len(self.nodes))

        return avail_kb * 1024

    def get_node_disk_usage(self, node: ClusterNode):
        """
        get disk usage for the redpanda volume on a particular node
        """

        for line in node.account.ssh_capture(
            f"df --block-size 1 {self.PERSISTENT_ROOT}", timeout_sec=10
        ):
            self.logger.debug(line.strip())
            if self.PERSISTENT_ROOT in line:
                return int(line.split()[2])
        assert False, "couldn't parse df output"

    def _startup_poll_interval(self, first_start: bool):
        """
        During startup, our eagerness depends on whether it's the first
        start, where we expect a redpanda node to start up very quickly,
        or a subsequent start where it may be more sedate as data replay
        takes place.
        """
        return 0.2 if first_start else 1.0

    def wait_for_membership(self, first_start: bool, timeout_sec: int = 30):
        self.logger.info("Waiting for all brokers to join cluster")

        wait_until(
            lambda: {n for n in self._started if self.registered(n)} == self._started,
            timeout_sec=timeout_sec,
            backoff_sec=self._startup_poll_interval(first_start),
            err_msg="Cluster membership did not stabilize",
        )

    def start(
        self,
        nodes: list[ClusterNode] | None = None,
        clean_nodes: bool = True,
        expect_fail: bool = False,
        auto_assign_node_id: bool = False,
        omit_seeds_on_idx_one: bool = True,
        node_config_overrides: NodeConfigOverridesT = {},
        skip_storage_init_check: bool = False,
        **kwargs: Any,
    ) -> None:
        """
        Start the service on all nodes.

        :param expect_fail: if true, expect redpanda nodes to terminate shortly
                            after starting.  Raise exception if they don't.
        """

        assert not kwargs, f"unexpected keyword args: {kwargs}"

        # Callers sometimes forget that this must be keyed by ClusterNode.
        for k in node_config_overrides:
            assert type(k) is ClusterNode, (
                f"Expected ClusterNode as key in node_config_overrides, got {type(k)}"
            )

        to_start = nodes if nodes is not None else self.nodes
        assert all((node in self.nodes for node in to_start))
        self.logger.info("%s: starting service" % self.who_am_i())

        first_start = self._start_time < 0
        if first_start:
            self._start_time = time.time()

        self.logger.debug(
            self.who_am_i()
            + ": killing processes and attempting to clean up before starting"
        )

        def clean_one(node: ClusterNode):
            try:
                self.stop_node(node)
            except Exception:
                pass

            try:
                if clean_nodes:
                    # Expected usage is that we may install new binaries before
                    # starting the cluster, and installation-cleaning happened
                    # when we started the installer.
                    self.clean_node(node, preserve_current_install=True)
                else:
                    self.logger.debug("%s: skip cleaning node" % self.who_am_i(node))
            except Exception:
                self.logger.exception(f"Error cleaning node {node.account.hostname}:")
                raise

        if first_start:
            # Clean all nodes on the first start because the test can choose to initialize
            # the cluster with a smaller node subset and start other nodes later with
            # redpanda.start_node() (which doesn't invoke clean_node)
            self.for_nodes(self.nodes, clean_one)
        else:
            self.for_nodes(to_start, clean_one)

        if first_start:
            self.write_tls_certs()
            self.write_bootstrap_cluster_config()

        def start_one(node: ClusterNode):
            node_overrides = (
                node_config_overrides[node] if node in node_config_overrides else {}
            )
            self.logger.debug("%s: starting node" % self.who_am_i(node))
            self.start_node(
                node,
                first_start=first_start,
                expect_fail=expect_fail,
                auto_assign_node_id=auto_assign_node_id,
                omit_seeds_on_idx_one=omit_seeds_on_idx_one,
                override_cfg_params=node_overrides,
            )

        try:
            self.for_nodes(to_start, start_one)
        except TimeoutError as e:
            if expect_fail:
                raise e
            if "failed to start within" in str(e):
                self.logger.debug(f"Checking for crashes after start-up error: {e}")
                self.raise_on_crash()
            raise e

        if expect_fail:
            # If we got here without an exception, it means we failed as expected
            return

        if self._start_duration_seconds < 0:
            self._start_duration_seconds = time.time() - self._start_time

        if not self._skip_create_superuser:
            self._admin.create_user(*self._superuser, await_exists=True)

        self.wait_for_membership(first_start=first_start)

        if not skip_storage_init_check:
            self.logger.info("Verifying storage is in expected state")

            expected = {
                "redpanda": {"controller", "kvstore"},
                "kafka": {
                    "_redpanda.audit_log",
                },
                self.KAFKA_INTERNAL_NAMESPACE: {
                    self.ID_ALLOCATOR_TOPIC,
                },
            }

            storage = self.storage(nodes=to_start)
            for node in storage.nodes:
                unexpected_ns = set(node.ns) - set(expected.keys())
                if unexpected_ns:
                    for ns in unexpected_ns:
                        self.logger.error(
                            f"node {node.name}: unexpected namespace: {ns}, "
                            f"topics: {set(node.ns[ns].topics)}"
                        )
                    raise RuntimeError("Unexpected files in data directory")

                for ns in node.ns:
                    unexpected_topics = set(node.ns[ns].topics) - expected[ns]
                    if unexpected_topics:
                        self.logger.error(
                            f"node {node.name}: unexpected topics in {ns} namespace: "
                            f"{unexpected_topics}"
                        )
                        raise RuntimeError("Unexpected files in data directory")

        if self.sasl_enabled():
            username, password, algorithm = self._superuser
            self._security_config = dict(
                security_protocol="SASL_PLAINTEXT",
                sasl_mechanism=algorithm,
                sasl_plain_username=username,
                sasl_plain_password=password,
                request_timeout_ms=30000,
                api_version_auto_timeout_ms=3000,
            )

        # Start stress fiber if requested
        if self.use_stress_fiber():

            def start_stress_fiber(node: ClusterNode):
                count, min_ms, max_ms = self.get_stress_fiber_params()
                self.start_stress_fiber(node, count, min_ms, max_ms)

            if first_start:
                self.logger.info(f"Starting stress fiber for {len(to_start)} nodes")
                self.for_nodes(to_start, start_stress_fiber)
            else:
                self.logger.info(f"Starting stress fiber for {len(self.nodes)} nodes")
                self.for_nodes(self.nodes, start_stress_fiber)

    def write_crl_file(self, node: ClusterNode, ca: tls.CertificateAuthority) -> None:
        assert ca.crl is not None, "CRL file is required"
        self.logger.info(
            f"Writing Redpanda node tls ca CRL file: {RedpandaService.TLS_CA_CRL_FILE}"
        )
        self.logger.debug(open(ca.crl, "r").read())
        node.account.mkdirs(os.path.dirname(RedpandaService.TLS_CA_CRL_FILE))
        node.account.copy_to(ca.crl, RedpandaService.TLS_CA_CRL_FILE)
        node.account.ssh(f"chmod 755 {RedpandaService.TLS_CA_CRL_FILE}")

    def write_tls_certs(self):
        if not self._security.tls_provider:
            return

        ca = self._security.tls_provider.ca
        for node in self.nodes:
            cert = self._security.tls_provider.create_broker_cert(self, node)

            self.logger.info(
                f"Writing Redpanda node tls key file: {RedpandaService.TLS_SERVER_KEY_FILE}"
            )
            self.logger.debug(open(cert.key, "r").read())
            node.account.mkdirs(os.path.dirname(RedpandaService.TLS_SERVER_KEY_FILE))
            node.account.copy_to(cert.key, RedpandaService.TLS_SERVER_KEY_FILE)

            self.logger.info(
                f"Writing Redpanda node tls cert file: {RedpandaService.TLS_SERVER_CRT_FILE}"
            )
            self.logger.debug(open(cert.crt, "r").read())
            node.account.mkdirs(os.path.dirname(RedpandaService.TLS_SERVER_CRT_FILE))
            node.account.copy_to(cert.crt, RedpandaService.TLS_SERVER_CRT_FILE)

            self.logger.info(
                f"Writing Redpanda node P12 file: {RedpandaService.TLS_SERVER_P12_FILE}"
            )
            self.logger.debug("P12 file is binary encoded")
            node.account.mkdirs(os.path.dirname(RedpandaService.TLS_SERVER_P12_FILE))
            node.account.copy_to(cert.p12_file, RedpandaService.TLS_SERVER_P12_FILE)

            self.logger.info(
                f"Writing Redpanda node tls ca cert file: {RedpandaService.TLS_CA_CRT_FILE}"
            )
            self.logger.debug(open(ca.crt, "r").read())
            node.account.mkdirs(os.path.dirname(RedpandaService.TLS_CA_CRT_FILE))
            node.account.copy_to(ca.crt, RedpandaService.TLS_CA_CRT_FILE)
            node.account.ssh(f"chmod 755 {RedpandaService.TLS_CA_CRT_FILE}")

            assert ca.crl is not None, "Missing CRL file"
            self.write_crl_file(node, ca)

            node.account.copy_to(ca.crt, RedpandaService.SYSTEM_TLS_CA_CRT_FILE)
            node.account.ssh(f"chmod 755 {RedpandaService.SYSTEM_TLS_CA_CRT_FILE}")
            node.account.ssh("update-ca-certificates")

            if self._pandaproxy_config is not None:
                self._pandaproxy_config.maybe_write_client_certs(
                    node,
                    self.logger,
                    PandaproxyConfig.PP_TLS_CLIENT_KEY_FILE,
                    PandaproxyConfig.PP_TLS_CLIENT_CRT_FILE,
                )
                self._pandaproxy_config.server_key = RedpandaService.TLS_SERVER_KEY_FILE
                self._pandaproxy_config.server_crt = RedpandaService.TLS_SERVER_CRT_FILE
                self._pandaproxy_config.truststore_file = (
                    RedpandaService.TLS_CA_CRT_FILE
                )
                self._pandaproxy_config.crl_file = RedpandaService.TLS_CA_CRL_FILE

            if self._schema_registry_config is not None:
                self._schema_registry_config.maybe_write_client_certs(
                    node,
                    self.logger,
                    SchemaRegistryConfig.SR_TLS_CLIENT_KEY_FILE,
                    SchemaRegistryConfig.SR_TLS_CLIENT_CRT_FILE,
                )
                self._schema_registry_config.server_key = (
                    RedpandaService.TLS_SERVER_KEY_FILE
                )
                self._schema_registry_config.server_crt = (
                    RedpandaService.TLS_SERVER_CRT_FILE
                )
                self._schema_registry_config.truststore_file = (
                    RedpandaService.TLS_CA_CRT_FILE
                )
                self._schema_registry_config.crl_file = RedpandaService.TLS_CA_CRL_FILE

            if self._audit_log_config is not None:
                self._audit_log_config.maybe_write_client_certs(
                    node,
                    self.logger,
                    AuditLogConfig.AUDIT_LOG_TLS_CLIENT_KEY_FILE,
                    AuditLogConfig.AUDIT_LOG_TLS_CLIENT_CRT_FILE,
                )
                self._audit_log_config.server_key = RedpandaService.TLS_SERVER_KEY_FILE
                self._audit_log_config.server_crt = RedpandaService.TLS_SERVER_CRT_FILE
                self._audit_log_config.truststore_file = RedpandaService.TLS_CA_CRT_FILE
                self._audit_log_config.crl_file = RedpandaService.TLS_CA_CRL_FILE

    def start_redpanda(self, node: ClusterNode, extra_cli: list[str] = []):
        preamble, res_args = self._resource_settings.to_cli(
            dedicated_node=self._dedicated_nodes
        )

        # each node will create its own copy of the .profraw file
        # since each node creates a redpanda broker.
        if self.cov_enabled():
            self._environment.update(
                dict(LLVM_PROFILE_FILE=f'"{RedpandaService.COVERAGE_PROFRAW_CAPTURE}"')
            )

        env_preamble = self.redpanda_env_preamble()

        cur_ver: RedpandaVersionTriple | None = None
        try:
            cur_ver = self.get_version_int_tuple(node)
        except Exception:  # noqa
            pass

        cmd = (
            f"{preamble} {env_preamble} nohup {self.find_binary('redpanda')}"
            f" --redpanda-cfg {RedpandaService.NODE_CONFIG_FILE}"
            f" {self._log_config.to_args(cur_ver)} "
            " --abort-on-seastar-bad-alloc "
            " --dump-memory-diagnostics-on-alloc-failure-kind=all "
            f" {res_args} "
            f" {' '.join(extra_cli)}"
            f" >> {RedpandaService.STDOUT_STDERR_CAPTURE} 2>&1 &"
        )

        node.account.ssh(cmd)

    def check_node(self, node: ClusterNode):
        pid = self.redpanda_pid(node)
        if not pid:
            self.logger.warning(f"No redpanda PIDs found on {node.name}")
            return False

        if not node.account.exists(f"/proc/{pid}"):
            self.logger.warning(f"PID {pid} (node {node.name}) dead")
            return False

        # fall through
        return True

    def start_stress_fiber(
        self, node: ClusterNode, count: int, min_ms: int, max_ms: int
    ):
        """Start stress fiber"""
        admin = Admin(self)
        admin.stress_fiber_start(
            node=node,
            num_fibers=count,
            min_ms_per_scheduling_point=min_ms,
            max_ms_per_scheduling_point=max_ms,
        )

    def all_up(self):
        def check_node(node: ClusterNode):
            pid = self.redpanda_pid(node)
            if not pid:
                self.logger.warning(f"No redpanda PIDs found on {node.name}")
                return False

            if not node.account.exists(f"/proc/{pid}"):
                self.logger.warning(f"PID {pid} (node {node.name}) dead")
                return False

            # fall through
            return True

        return all(self.for_nodes(list(self._started), check_node))

    def signal_redpanda(
        self,
        node: ClusterNode,
        signal: Signals = signal.SIGKILL,
        idempotent: bool = False,
        thread: str | None = None,
    ) -> None:
        """
        :param idempotent: if true, then kill-like signals are ignored if
                           the process is already gone.
        :param thread: if set, then the signal is sent to the given thread
        """
        if thread is None:
            pid = self.redpanda_pid(node)
            if pid is None:
                if idempotent and signal in {SIGKILL, SIGTERM}:
                    return
                else:
                    raise RuntimeError(
                        f"Can't signal redpanda on node {node.name}, it isn't running"
                    )

            node.account.signal(pid, signal, allow_fail=False)
        else:
            tgid, tid = self.redpanda_tid(node, thread)
            script_path = inject_remote_script(node, "tgkill.py")
            cmd = shlex.join(
                ["python3", script_path, str(tgid), str(tid), str(signal.value)]
            )
            node.account.ssh(cmd, allow_fail=False)

    @contextmanager
    def paused_node(self, node: ClusterNode):
        """Context manager to pause redpanda on a node by sending SIGSTOP"""
        # rpk requires all nodes up to operate, so remove from started nodes
        self.remove_from_started_nodes(node, "paused_node")
        self.signal_redpanda(node, signal=signal.SIGSTOP)
        try:
            yield
        finally:
            self.signal_redpanda(node, signal=signal.SIGCONT)
            self.add_to_started_nodes(node)

    @contextmanager
    def core_dumps_disabled(self):
        """Context manager that disables core dumps for redpanda processes
        started within the block, restoring the prior limit on exit.

        Intended for tests that deliberately abort a node -- e.g. a startup that
        is expected to fail -- where the SIGILL from vassert's __builtin_trap()
        would otherwise trigger a core dump. Flushing a multi-GB core on a loaded
        host can take longer than the start/termination timeout and flake the
        test; the abort reason is already captured in the log and the
        crash_tracker crash file, so the kernel core adds no diagnostic value.

        The limit is a shared resource setting, so this affects every node
        started while the block is active -- fine for the sequential single-node
        starts these tests perform."""
        prev = self._resource_settings.core_dump_limit
        self._resource_settings.core_dump_limit = "0"
        try:
            yield
        finally:
            self._resource_settings.core_dump_limit = prev

    def sockets_clear(self, node: RemoteClusterNode):
        """
        Check that high-numbered redpanda ports (in practice, just the internal
        RPC port) are clear on the node, to avoid TIME_WAIT sockets from previous
        tests interfering with redpanda startup.

        In principle, redpanda should not have a problem with TIME_WAIT sockets
        on its port (redpanda binds with SO_REUSEADDR), but in practice we have
        seen "Address in use" errors:
        https://github.com/redpanda-data/redpanda/pull/3754
        """
        for line in node.account.ssh_capture("netstat -ant", timeout_sec=10):
            self.logger.debug(f"node={node.name} {line.strip()}")

            # Parse output line
            tokens = line.strip().split()
            if len(tokens) != 6:
                # Header, skip
                continue
            _, _, _, src, dst, state = tokens

            if src.endswith(":33145"):
                self.logger.info(
                    f"Port collision on node {node.name}: {src}->{dst} {state}"
                )
                return False

        # Fall through: no problematic lines found
        return True

    def lsof_node(self, node: ClusterNode, filter: str | None = None):
        """
        Get the list of open files for a running node

        :param filter: If given, this is a grep regex that will filter the files we list

        :return: yields strings
        """
        first = True
        cmd = f"lsof -nP -p {self.redpanda_pid(node)}"
        if filter is not None:
            cmd += f" | grep {filter}"
        for line in node.account.ssh_capture(cmd, timeout_sec=60):
            if first and not filter:
                # First line is a header, skip it
                first = False
                continue
            try:
                filename = line.split()[-1]
            except IndexError:
                # Malformed line
                pass
            else:
                yield filename

    def is_node_ready(self, node: ClusterNode):
        """
        Calls Admin API's v1/status/ready endpoint to verify if the node
        is ready
        """
        status = None
        try:
            status = Admin.ready(node).get("status")
        except requests.exceptions.ConnectionError:
            self.logger.debug(f"node {node.name} not yet accepting connections")
            return False
        except:
            self.logger.exception(
                f"error on getting status from {node.account.hostname}"
            )
            raise
        if status != "ready":
            self.logger.debug(
                f"status of {node.account.hostname} isn't ready: {status}"
            )
            return False
        return True

    def start_node(
        self,
        node: ClusterNode,
        override_cfg_params: dict[str, Any] | None = None,
        timeout: int | None = None,
        write_config: bool = True,
        first_start: bool = False,
        expect_fail: bool = False,
        auto_assign_node_id: bool = False,
        omit_seeds_on_idx_one: bool = True,
        skip_readiness_check: bool = False,
        node_id_override: int | None = None,
        extra_cli: list[str] = [],
        **kwargs: Any,
    ) -> None:
        """
        Start a single instance of redpanda. This function will not return until
        redpanda appears to have started successfully. If redpanda does not
        start within a timeout period the service will fail to start. Thus this
        function also acts as an implicit test that redpanda starts quickly.
        """

        assert not kwargs, "kwargs not empty"
        node.account.mkdirs(RedpandaService.DATA_DIR)
        node.account.mkdirs(os.path.dirname(RedpandaService.NODE_CONFIG_FILE))

        self.write_openssl_config_file(node)

        if write_config:
            self.write_node_conf_file(
                node,
                override_cfg_params,
                auto_assign_node_id=auto_assign_node_id,
                omit_seeds_on_idx_one=omit_seeds_on_idx_one,
                node_id_override=node_id_override,
            )

        if timeout is None:
            timeout = self.node_ready_timeout_s

        if self.dedicated_nodes:
            # When running on dedicated nodes, we should always be running on XFS.  If we
            # aren't, it's probably an accident that can easily cause spurious failures
            # and confusion, so be helpful and fail out early.
            fs = node.account.ssh_output(
                f"stat -f -c %T {self.PERSISTENT_ROOT}", timeout_sec=10
            ).strip()
            if fs != b"xfs":
                raise RuntimeError(
                    f"Non-XFS filesystem {fs} at {self.PERSISTENT_ROOT} on {node.name}"
                )

        def start_rp():
            self.start_redpanda(node, extra_cli=extra_cli)

            if expect_fail:
                wait_until(
                    lambda: self.redpanda_pid(node) is None,
                    timeout_sec=timeout,
                    backoff_sec=0.2,
                    err_msg=f"Redpanda processes did not terminate on {node.name} during startup as expected in {timeout} sec",
                )
            elif not skip_readiness_check:
                wait_until(
                    lambda: self.is_node_ready(node),
                    timeout_sec=timeout,
                    backoff_sec=self._startup_poll_interval(first_start),
                    err_msg=f"Redpanda service {node.account.hostname} failed to start within {timeout} sec",
                    retry_on_exc=True,
                )

        self.logger.debug("Node status prior to redpanda startup:")
        self.start_service(node, start_rp)
        if not expect_fail:
            self.add_to_started_nodes(node)

    def start_node_with_rpk(
        self, node: ClusterNode, additional_args: str = "", clean_node: bool = True
    ):
        """
        Start a single instance of redpanda using rpk. similar to start_node,
        this function will not return until redpanda appears to have started
        successfully.
        """
        self.logger.debug(
            self.who_am_i()
            + ": killing processes and attempting to clean up before starting"
        )
        try:
            self.stop_node(node)
        except Exception:
            pass

        if clean_node:
            self.clean_node(node, preserve_current_install=True)
        else:
            self.logger.debug("%s: skip cleaning node" % self.who_am_i(node))
        node.account.mkdirs(RedpandaService.DATA_DIR)
        node.account.mkdirs(os.path.dirname(RedpandaService.NODE_CONFIG_FILE))

        env_vars = " ".join([f"{k}={v}" for (k, v) in self._environment.items()])
        rpk = RpkRemoteTool(self, node)

        _, args = self._resource_settings.to_cli(dedicated_node=self._dedicated_nodes)
        additional_args += " " + args

        def start_rp():
            rpk.redpanda_start(
                RedpandaService.STDOUT_STDERR_CAPTURE, additional_args, env_vars
            )

            wait_until(
                lambda: self.is_node_ready(node),
                timeout_sec=60,
                backoff_sec=1,
                err_msg=f"Redpanda service {node.account.hostname} failed to start within 60 sec using rpk",
                retry_on_exc=True,
            )

        self.logger.debug("Node status prior to redpanda startup:")
        self.start_service(node, start_rp)
        self.add_to_started_nodes(node)

        # We need to manually read the config from the file and add it
        # to _node_configs since we use rpk to write the file instead of
        # the write_node_conf_file method.
        with tempfile.TemporaryDirectory() as d:
            node.account.copy_from(RedpandaService.NODE_CONFIG_FILE, d)
            with open(os.path.join(d, "redpanda.yaml")) as f:
                actual_config = yaml.full_load(f.read())
                self._node_configs[node] = actual_config

    def _log_node_shutdown_analysis(self, node: ClusterNode):
        """
        Analyze a node's failure to shutdown within the allocated time to try
        to diagnose the reason for shutdown hang.
        """
        self.logger.debug(f"Gathering logs to analyze from {node.name}...")
        expr = '"application.*Stopping"'
        cmd = f"grep {expr} {RedpandaService.STDOUT_STDERR_CAPTURE} || true"

        other_stopping: list[str] = []
        last_next_to_shutdown = None
        for line in node.account.ssh_capture(cmd):
            if "next to shutdown" in line:
                last_next_to_shutdown = line
            else:
                other_stopping.append(line)

        if last_next_to_shutdown is None:
            self.logger.debug(
                "No structured shutdown messages found. Hang may have occurred in early shutdown"
            )
            return

        self.logger.debug(f"Last structured shutdown line: {last_next_to_shutdown}")
        next_service = last_next_to_shutdown.split()[-1]

        for line in other_stopping:
            if f"Stopping {next_service}" in line:
                self.logger.debug(f"Found stopping message for last service {line}")
                return

        self.logger.debug(f"Did not find stopping message for service {next_service}")

    def _log_node_process_state(self, node: ClusterNode):
        """
        For debugging issues around starting and stopping processes: log
        which processes are running and which ports are in use.
        """

        self.logger.debug(
            f"Gathering process and port usage information on {node.name}..."
        )

        # Capture general process information
        process_lines: list[str] = []
        for line in node.account.ssh_capture("ps aux --sort=-%mem", timeout_sec=30):
            process_lines.append(line.strip())

        output_str = "\n".join(process_lines)
        self.logger.debug(f"{node.name}: ps aux output:\n{output_str}")

        # Capture network information
        netstat_lines = [
            line.strip()
            for line in node.account.ssh_capture("netstat -panelot", timeout_sec=30)
        ]

        output_str = "\n".join(netstat_lines)
        self.logger.debug(f"{node.name}: netstat -panelot output:\n{output_str}")

    def _log_process_status(self, node: ClusterNode, pid: int):
        """
        Log the status of a process from /proc/[pid]/status
        """
        self.logger.debug(f"{node.name}: Gathering /proc/{pid}/status for node...")
        cmd = f"cat /proc/{pid}/status"
        lines: list[str] = []
        for line in node.account.ssh_capture(cmd, allow_fail=True, timeout_sec=10):
            if re.search(r"CoreDumping:\s*1", line):
                self.logger.warning(
                    f"{node.name}: Detected core dumping in process {pid} status."
                )
            lines.append(line.strip())

        output_str = "\n".join(lines)
        self.logger.debug(f"{node.name}: /proc/{pid}/status:\n{output_str}")

    def start_service(self, node: ClusterNode, start: Callable[[], None]) -> None:
        # Maybe the service collides with something that wasn't cleaned up
        # properly: let's peek at what's going on on the node before starting it.
        self._log_node_process_state(node)

        try:
            start()
        except:
            # In case our failure to start is something like an "address in use", we
            # would like to know what else is going on on this node.
            self.logger.warning(
                f"Failed to start on {node.name}, gathering node ps and netstat..."
            )
            self._log_node_process_state(node)
            raise

    def partitions(self, topic_name: str | None = None) -> list[Partition]:
        """
        Return partition metadata for the topic.
        """
        kc = KafkaCat(self)
        md = kc.metadata()

        result: list[Partition] = []

        def make_partition(topic_name: str, p: dict[str, Any]):
            index = p["partition"]
            leader_id = p["leader"]
            leader = None if leader_id == -1 else self.get_node_by_id(leader_id)
            replicas = [self.node_by_id(r["id"]) for r in p["replicas"]]
            return Partition(topic_name, index, leader, replicas)

        for topic in md["topics"]:
            if topic["topic"] == topic_name or topic_name is None:
                result.extend(
                    make_partition(topic["topic"], p) for p in topic["partitions"]
                )

        return result

    def set_cluster_config_to_null(self, name: str, **kwargs: Any) -> None:
        self.set_cluster_config(values={name: None}, **kwargs)

    def set_cluster_config(
        self,
        values: dict[str, Any],
        expect_restart: bool = False,
        admin_client: Admin | None = None,
        timeout: int = 10,
        tolerate_stopped_nodes: bool = False,
    ):
        """
        Update cluster configuration and wait for all nodes to report that they
        have seen the new config.

        :param values: dict of property name to value.
        :param expect_restart: set to true if you wish to permit a node restart for needs_restart=yes properties.
                               If you set such a property without this flag, an assertion error will be raised.
        """
        if admin_client is None:
            admin_client = self._admin

        patch_result = admin_client.patch_cluster_config(upsert=values, remove=[])
        new_version = patch_result["config_version"]

        self._wait_for_config_version(
            new_version,
            expect_restart,
            timeout,
            admin_client=admin_client,
            tolerate_stopped_nodes=tolerate_stopped_nodes,
        )

    def enable_development_feature_support(self, key: int | None = None):
        """
        Enable experimental feature support.

        The key must be equal to the current broker time expressed as unix epoch
        in seconds, and be within 1 hour.
        """
        key = int(time.time()) if key is None else key
        self.set_cluster_config(
            dict(
                enable_developmental_unrecoverable_data_corrupting_features=key,
            )
        )

    def _wait_for_config_version(
        self,
        config_version: int,
        expect_restart: bool,
        timeout: int,
        admin_client: Admin | None = None,
        tolerate_stopped_nodes: bool = False,
    ):
        admin_client = admin_client or self._admin
        if tolerate_stopped_nodes:
            started_node_ids = {self.node_id(n) for n in self.started_nodes()}
        else:
            started_node_ids = {}

        def is_ready():
            status = admin_client.get_cluster_config_status(node=self.controller())
            ready = all(
                [
                    n["config_version"] >= config_version
                    for n in status
                    if not tolerate_stopped_nodes or n["node_id"] in started_node_ids
                ]
            )

            return ready, status

        # The version check is >= to permit other config writes to happen in
        # the background, including the write to cluster_id that happens
        # early in the cluster's lifetime
        config_status = wait_until_result(
            is_ready,
            timeout_sec=timeout,
            backoff_sec=0.5,
            err_msg=f"Config status versions did not converge on {config_version}",
        )

        any_restarts = any(n["restart"] for n in config_status)
        if any_restarts and expect_restart:
            self.restart_nodes(self.nodes)
            # Having disrupted the cluster with a restart, wait for the controller
            # to be available again before returning to the caller, so that they do
            # not have to worry about subsequent configuration actions failing.
            admin_client.await_stable_leader(
                namespace="redpanda", topic="controller", partition=0
            )
        elif any_restarts:
            raise AssertionError(
                "Nodes report restart required but expect_restart is False"
            )

    def set_feature_active(
        self, feature_name: str, active: bool, *, timeout_sec: int = 15
    ):
        target_state = "active" if active else "disabled"
        cur_state = self.get_feature_state(feature_name)
        if active and cur_state == "unavailable":
            # If we have just restarted after an upgrade, wait for cluster version
            # to progress and for the feature to become available. Features with
            # available_policy::always auto-activate as soon as they become
            # available, so accept "active" too.
            self.await_feature(
                feature_name, {"available", "active"}, timeout_sec=timeout_sec
            )
        self._admin.put_feature(feature_name, {"state": target_state})
        self.await_feature(feature_name, target_state, timeout_sec=timeout_sec)

    def get_feature_state(self, feature_name: str, node: ClusterNode | None = None):
        f = self._admin.get_features(node=node)
        by_name = dict((f["name"], f) for f in f["features"])
        try:
            state = by_name[feature_name]["state"]
        except KeyError:
            state = None
        return state

    def await_feature(
        self,
        feature_name: str,
        await_state: str | set[str],
        *,
        timeout_sec: int,
        nodes: list[ClusterNode] | None = None,
    ):
        """
        For use during upgrade tests, when after upgrade yo uwould like to block
        until a particular feature's active status updates (e.g. if it does migrations)
        """
        if nodes is None:
            nodes = self.started_nodes()

        await_states = {await_state} if isinstance(await_state, str) else await_state

        def is_awaited_state():
            for n in nodes:
                state = self.get_feature_state(feature_name, node=n)
                if state not in await_states:
                    self.logger.info(
                        f"Feature {feature_name} not yet in {await_states} on {n.name} (state {state})"
                    )
                    return False

            self.logger.info(f"Feature {feature_name} is now in {await_states}")
            return True

        wait_until(is_awaited_state, timeout_sec=timeout_sec, backoff_sec=1)

    def monitor_log(self, node: ClusterNode):
        assert node in self.nodes, f"Node {node.account.hostname} is not started"
        return node.account.monitor_log(RedpandaService.STDOUT_STDERR_CAPTURE)

    def raise_on_crash(self, log_allow_list: LogAllowList = ()) -> None:
        """
        Check if any redpanda nodes are unexpectedly not running,
        or if any logs contain segfaults or assertions.

        Call this after a test fails, to generate a more useful
        error message, rather than having failures on "timeouts" which
        are actually redpanda crashes.
        """

        allow_list = prepare_allow_list(log_allow_list)

        def is_allowed_log_line(line: str) -> bool:
            for a in allow_list:
                if a.search(line) is not None:
                    return True
            return False

        def scan_node_for_crash(node: ClusterNode) -> tuple[ClusterNode, str] | None:
            self.logger.info(f"Scanning node {node.account.hostname} log for errors...")
            # crashes appear near the "end" of the file, so examine only the last
            # 10 MB, to avoid timeouts on large logs
            for line in node.account.ssh_capture(
                f"tail --bytes=10000000 {RedpandaService.STDOUT_STDERR_CAPTURE} "
                "| grep -e SEGV -e Segmentation\\ fault -e [Aa]ssert -e Sanitizer "
                "-e 'Aborting on shard' -e 'crash reason to crash file' || true",
                timeout_sec=30,
            ):
                if is_allowed_log_line(line):
                    self.logger.info(f"Ignoring allow-listed log line '{line}'")
                    continue

                if "No such file or directory" not in line:
                    return (node, line)
            return None

        crash_results = self.for_nodes(self.nodes, scan_node_for_crash)
        crashes: list[tuple[ClusterNode, str]] = [
            r for r in crash_results if r is not None
        ]

        if not crashes:
            # Even if there is no assertion or segfault, look for unexpectedly
            # not-running processes
            def check_pid(node: ClusterNode) -> tuple[ClusterNode, str] | None:
                if not self.redpanda_pid(node):
                    dmesg = self._capture_dmesg(node)
                    if dmesg:
                        self._save_dmesg_artifact(node, dmesg)
                    return (node, "Redpanda process unexpectedly stopped")
                return None

            pid_results = self.for_nodes(list(self._started), check_pid)
            crashes = [r for r in pid_results if r is not None]

        if crashes:
            if self._tolerate_crashes:
                self.logger.info(
                    f"Detected crashes, but RedpandaService is configured to allow them: {crashes}"
                )
            else:
                raise NodeCrash(crashes)

    def _capture_dmesg(self, node: ClusterNode) -> str | None:
        """Capture recent dmesg output from ``node`` for crash diagnostics."""
        try:
            slack_secs = 300
            seconds_ago = int(time.time() - self._start_time) + slack_secs
            lines: list[str] = []
            max_lines = 1000
            cmd = (
                f"sudo dmesg -T --since '{seconds_ago} seconds ago' | tail -{max_lines}"
            )
            for line in node.account.ssh_capture(cmd, timeout_sec=10):
                line = line.strip()
                if line:
                    lines.append(line)
            return "\n".join(lines) if lines else None
        except Exception as e:
            self.logger.debug(
                f"Failed to collect dmesg from {node.account.hostname}: {e}"
            )
            return None

    def _save_dmesg_artifact(self, node: ClusterNode, dmesg: str) -> None:
        node_dir = os.path.join(
            TestContext.results_dir(self._context, self._context.test_index),
            self.service_id,
            node.account.hostname,
        )
        artifact_path = os.path.join(node_dir, "dmesg.txt")
        try:
            if not os.path.isdir(node_dir):
                mkdir_p(node_dir)
            self.logger.info(f"writing dmesg output to {artifact_path}")
            with open(artifact_path, "w") as f:
                f.write(dmesg)
        except Exception as e:
            self.logger.debug(f"Failed to write dmesg to {artifact_path}: {e}")

    def raw_metrics(
        self,
        node: ClusterNode,
        metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS,
        name: str | None = None,
    ):
        assert node in self._started, f"Node {node.account.hostname} is not started"

        url = f"http://{node.account.hostname}:9644/{metrics_endpoint.value}"

        params = (
            {"__name__": self._adjust_metric_name(name, metrics_endpoint)}
            if name
            else None
        )
        start_t = time.time()
        resp = None
        try:
            resp = requests.get(url, timeout=10, params=params)
        finally:
            elapsed = time.time() - start_t
            if resp:
                status = resp.status_code
                bytes_len = len(resp.text)
            else:
                status = "<exception>"
                bytes_len = "n/a"
            self.logger.debug(
                f"raw_metrics duration_sec={elapsed:.3f} endpoint={metrics_endpoint.value} host={node.account.hostname} "
                f"status={status} bytes={bytes_len}"
            )
        assert resp.status_code == 200
        return resp.text

    def metrics(
        self,
        node: ClusterNode,
        metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS,
        name: str | None = None,
    ):
        """Parse the prometheus text format metric from a given node."""
        assert isinstance(node, ClusterNode)
        text = self.raw_metrics(node, metrics_endpoint, name)
        return list(text_string_to_metric_families(text))

    def raise_on_storage_usage_inconsistency(self):
        def tracked(fstat: tuple[pathlib.Path, int]):
            """
            filter out files at the root of redpanda's data directory. these
            are not included right now in the local storage costs returned by
            the admin api. we may want to update this in the future, but
            paying the cost to look at the files which are generally small,
            and non-reclaimable doesn't seem worth the hassle. however, for
            small experiements they are relatively large, so we need to filter
            them out for the purposes of looking at the accuracy of storage
            usage tracking.

            Example files:
                   26 startup_log
                10685 config_cache.yaml
            """
            file, _ = fstat
            if len(file.parents) == 1:
                return False
            if file.parents[-2].name == "debug-bundle":
                return False
            if (
                "compaction.staging" in file.name
                or "compaction.compaction_index" in file.name
                or "compaction_index.staging" in file.name
            ):
                # compaction staging files are temporary and are generally
                # cleaned up after compaction finishes, or at next round of
                # compaction if a file was stranded. during shutdown of any
                # generic test we don't have a good opportunity to force this to
                # happen without placing a lot of restrictions on shutdown. for
                # the time being just ignore these.
                return False
            if file.suffix == ".cannotrecover":
                # Unrecoverable segments aren't included in the disk usage report.
                # Since we don't remove them automatically and they don't get
                # cleaned up automatically, we can ignore them here for now.
                return False
            return True

        @dataclass
        class StorageInspectionResult:
            """Result of inspecting storage usage on a node."""

            diff_ratio: float  # Absolute difference ratio between observed and reported
            reclaimable_ratio: float  # Ratio of storage reclaimable by retention
            reported: dict[str, int]  # Reported storage usage by category
            reported_total: int  # Total reported storage usage
            observed: list[tuple[pathlib.Path, int]]  # List of (path, size) tuples
            observed_total: int  # Total observed storage usage

            def should_retry(self) -> bool:
                return self.diff_ratio > 0.05 + self.reclaimable_ratio

        def inspect_node(node: ClusterNode) -> StorageInspectionResult:
            """
            Fetch reported size from admin interface, query the local file
            system, and compute a percentage difference between reported and
            observed disk usage.
            """
            try:
                observed = list(self.data_stat(node))
                reported = self._admin.get_local_storage_usage(node)

                observed_total = sum(s for _, s in filter(tracked, observed))
                reported_total = (
                    reported["data"] + reported["index"] + reported["compaction"]
                )

                diff = observed_total - reported_total
                return StorageInspectionResult(
                    diff_ratio=abs(diff / reported_total),
                    reclaimable_ratio=reported["reclaimable_by_retention"]
                    / reported_total,
                    reported=reported,
                    reported_total=reported_total,
                    observed=observed,
                    observed_total=observed_total,
                )
            except Exception:
                return StorageInspectionResult(
                    diff_ratio=0.0,
                    reclaimable_ratio=0.0,
                    reported={},
                    reported_total=0,
                    observed=[],
                    observed_total=0,
                )

        # inspect the node and check that we fall below a 5% + reclaimabled_by_retention%
        # threshold difference. at this point the test is over, but we allow for a couple
        # retries in case things need to settle.

        def inspect_nodes(nodes_: list[ClusterNode]):
            return self.for_nodes(nodes_, lambda n: (n, inspect_node(n)))

        retries = [r for r in inspect_nodes(self.nodes) if r[1].should_retry()]

        for _ in range(3):
            results = inspect_nodes([node for node, _ in retries])
            retries = [r for r in results if r[1].should_retry()]

            if not retries:
                # all good
                return

            time.sleep(5)

        # if one or more nodes failed the check, then report information about
        # the situation and fail the test by raising an exception.
        node_names: list[str] = []
        max_node, max_diff = retries[0][0], retries[0][1].diff_ratio
        for node, result in retries:
            node_name = f"{self.idx(node)}:{node.account.hostname}"
            node_names.append(node_name)
            if result.diff_ratio > max_diff:
                max_diff = result.diff_ratio
                max_node = node
            diff = result.observed_total - result.reported_total
            for file, size in result.observed:
                self.logger.debug(f"Observed file [{node_name}]: {size:12} {file}")
            for key, value in result.reported.items():
                self.logger.debug(f"Reported [{node_name}]: {key}={value:12}")
            self.logger.warning(
                f"Storage usage [{node_name}]: obs {result.observed_total:12} rep {result.reported_total:12} diff {diff:12} pct {result.diff_ratio} reclaimable_pct {result.reclaimable_ratio}"
            )

        max_node_name = f"{self.idx(max_node)}:{max_node.account.hostname}"
        raise RuntimeError(
            f"Storage usage inconsistency on nodes {node_names}: max difference {max_diff} on node {max_node_name}"
        )

    def decode_backtraces(self, raise_on_failure: bool = False):
        """
        Decodes redpanda backtraces if any of them are present
        :return: None
        """

        def decode_node_backtraces(node: ClusterNode) -> Exception | None:
            if not node.account.exists(RedpandaService.STDOUT_STDERR_CAPTURE):
                # Log may not exist if node never started
                return None

            self.logger.info(f"Decoding backtraces on {node.account.hostname}.")
            cmd = "/opt/scripts/seastar-addr2line"
            cmd += " -a /opt/llvm/llvm-addr2line"
            cmd += f" -e {self.find_raw_binary('redpanda')}"
            cmd += f" -f {RedpandaService.STDOUT_STDERR_CAPTURE}"
            cmd += f" > {RedpandaService.BACKTRACE_CAPTURE} 2>&1"
            cmd += (
                f" && find {RedpandaService.BACKTRACE_CAPTURE} -type f -size 0 -delete"
            )

            try:
                node.account.ssh(cmd)
            except Exception as e:
                # We run during teardown on failures, so if something
                # goes wrong we must not raise, or we would usurp
                # the original exception that caused the failure.
                self.logger.exception("Failed to run seastar-addr2line")
                return e
            return None

        errors = [
            e
            for e in self.for_nodes(self.nodes, decode_node_backtraces)
            if e is not None
        ]
        if raise_on_failure and errors:
            raise errors[0]

    def rp_install_path(self):
        if self._installer._started:
            # The installer sets up binaries to always use /opt/redpanda.
            return "/opt/redpanda"
        return self._context.globals["rp_install_path_root"]

    def find_binary(self, name: str) -> str:
        rp_install_path_root = self.rp_install_path()
        return f"{rp_install_path_root}/bin/{name}"

    def find_raw_binary(self, name: str):
        """
        Like `find_binary`, but find the underlying executable rather tha
        a shell wrapper.
        """
        rp_install_path_root = self.rp_install_path()
        return f"{rp_install_path_root}/libexec/{name}"

    def get_version(self, node: ClusterNode) -> str:
        """
        Returns the redpanda binary version as a string.
        """
        env_preamble = self.redpanda_env_preamble()
        version_cmd = f"{env_preamble} {self.find_binary('redpanda')} --version"
        VERSION_LINE_RE = re.compile(".*(v\\d+\\.\\d+\\.\\d+).*")
        # NOTE: not all versions of Redpanda support the --version field, even
        # though they print out the version.
        version_lines = [
            l
            for l in node.account.ssh_capture(
                version_cmd, allow_fail=True, timeout_sec=10
            )
            if VERSION_LINE_RE.match(l)
        ]
        assert len(version_lines) == 1, version_lines
        return VERSION_LINE_RE.findall(version_lines[0])[0]

    def get_version_int_tuple(self, node: ClusterNode):
        version_str = self.get_version(node)
        return ri_int_tuple(RI_VERSION_RE.findall(version_str)[0])

    def get_version_if_not_head(self, node: ClusterNode) -> str | None:
        """
        Returns the redpanda binary version as a string if it differs from HEAD.
        I.e., if this node is running a previous version of redpanda.
        """
        cur_ver = self._installer.installed_version(node)
        if cur_ver != RedpandaInstaller.HEAD:
            return self.get_version(node)
        return None

    def export_cluster_config(self) -> None:
        """
        Export the cluster configuration of all nodes to the ducktape log
        directory for later inspection.
        """
        self.logger.info("%s: exporting cluster config" % self.who_am_i())

        service_dir = os.path.join(
            TestContext.results_dir(self._context, self._context.test_index),
            self.service_id,
        )
        cluster_config_filename = os.path.join(service_dir, "cluster_config.yaml")
        self.logger.debug(
            "%s: cluster_config_filename %s"
            % (self.who_am_i(), cluster_config_filename)
        )

        if not os.path.isdir(service_dir):
            mkdir_p(service_dir)

        try:
            rpk = RpkTool(self)
            rpk.cluster_config_export(cluster_config_filename, True)
        except Exception as e:
            # Configuration is optional: if redpanda has e.g. crashed, you
            # will not be able to get it from the admin API
            self.logger.info(f"{self.who_am_i()}: error getting config: {e}")

    def stop(self, **kwargs: Any) -> None:
        """
        Override default stop() to execude stop_node in parallel
        """
        self._stop_time = time.time()  # The last time stop is invoked

        self.logger.info("%s: stopping service" % self.who_am_i())

        self.for_nodes(self.nodes, lambda n: self.stop_node(n, **kwargs))

        self._stop_duration_seconds = time.time() - self._stop_time

    def _set_trace_loggers_and_sleep(self, node: ClusterNode, time_sec: int = 10):
        """
        For debugging issues around stopping processes: set the log level to
        trace on all loggers.
        """
        # These tend to be exceptionally chatty, or don't provide much value.
        keep_existing = ["exception", "io", "seastar_memory", "assert"]
        try:
            loggers = self._admin.get_loggers(node)
            for logger in loggers:
                if logger in keep_existing:
                    continue
                self._admin.set_log_level(logger, "trace", time_sec)
            time.sleep(time_sec)
        except Exception as e:
            self.logger.warning(f"Error setting trace loggers: {e}")

    def _update_usage_stats(self, node: ClusterNode):
        if node not in self._started:
            return

        metrics = {
            "vectorized_io_queue_total_read_bytes_total": "disk_bytes_read",
            "vectorized_io_queue_total_write_bytes_total": "disk_bytes_written",
            "vectorized_storage_log_batches_read": "batches_read",
            "vectorized_storage_log_batches_written": "batches_written",
            "vectorized_internal_rpc_sent_bytes": "internal_rpc_bytes_sent",
            "vectorized_internal_rpc_received_bytes": "internal_rpc_bytes_recv",
        }

        try:
            metric_samples = self.metrics_samples(
                list(metrics.keys()),
                [node],
            )

            for key, ms in metric_samples.items():
                current = getattr(self._usage_stats, metrics[key])
                setattr(
                    self._usage_stats,
                    metrics[key],
                    current + int(sum(s.value for s in ms.samples)),
                )
        except Exception as e:
            self.logger.warning(f"Cannot check metrics on shutdown - {e}")

    def stop_node(
        self,
        node: ClusterNode,
        timeout: float | None = None,
        forced: bool = False,
        **kwargs: Any,
    ):
        assert not kwargs, f"Unknown args {kwargs}"
        # collect usage stats before the node is stopped, the usage stats
        # accumulate metrics from all the nodes before they are stopped.
        self._update_usage_stats(node)
        # Assume node is stopped once we enter this path. If stopping succeeds
        # it is the obvious thing to do. If stopping fails we can't differentiate
        # between a node that stopped or will eventually stop so for all intents
        # and purposes we consider it stopped not to trip other logic that expects
        # started to contain nodes that _must_ be running. E.g. crash detection
        # at end of test which iterates through "started nodes".
        self.remove_from_started_nodes(node, "stop_node")

        pid = self.redpanda_pid(node)

        if pid is None:
            return

        self.logger.info(f"{node.name}: Stopping redpanda (pid {pid})")

        node.account.signal(
            pid, signal.SIGKILL if forced else signal.SIGTERM, allow_fail=False
        )

        stop_timeout = timeout or 30

        @debounce(0.5)
        def debounced_log_process_status():
            self._log_process_status(node, pid)

        def check_redpanda_process_stopped():
            is_stopped = self.redpanda_pid(node) is None
            if not is_stopped:
                self.logger.debug(
                    f"{node.name}: Redpanda process (pid {pid}) still running."
                )
                debounced_log_process_status()

            return is_stopped

        try:
            wait_until(
                check_redpanda_process_stopped,
                timeout_sec=stop_timeout,
                err_msg=f"Redpanda node {node.account.hostname} failed to stop in {stop_timeout} seconds",
            )

            self.logger.info(f"{node.name}: Redpanda process has exited.")
        except TimeoutError:
            sleep_sec = 10
            self.logger.warning(
                f"Timed out waiting for stop on {node.name}, setting log_level to 'trace' and sleeping for {sleep_sec}s"
            )
            self._set_trace_loggers_and_sleep(node, time_sec=sleep_sec)
            self.logger.warning(f"Node {node.name} status:")
            self._log_node_process_state(node)
            self._log_process_status(node, pid)
            self._log_node_shutdown_analysis(node)
            # Kill the process if it's still running. If redpanda still runs we
            # might fail to collect logs as the file will be modified while we
            # (ducktape) are reading/compressing it.
            # I.e. `tar: redpanda.log: file changed as we read it`
            node.account.signal(pid, signal.SIGKILL, allow_fail=True)
            raise

    def remove_from_started_nodes(self, node: ClusterNode, reason: str = "unknown"):
        if node in self._started:
            self.logger.debug(
                f"Removed {node.name} from started nodes, reason: {reason}"
            )
            self._started.remove(node)

    def add_to_started_nodes(self, node: ClusterNode):
        self.logger.debug(f"Added {node.name} to started nodes")
        self._started.add(node)

    def clean_node(
        self,
        node: ClusterNode,
        preserve_logs: bool = False,
        preserve_current_install: bool = False,
        **kwargs: Any,
    ):
        assert not kwargs, f"Unknown args {kwargs}"
        # These are allow_fail=True to allow for a race where kill_process finds
        # the PID, but then the process has died before it sends the SIGKILL.  This
        # should be safe against actual failures to of the process to stop, because
        # we're using SIGKILL which does not require the process's cooperation.
        node.account.kill_process("redpanda", clean_shutdown=False, allow_fail=True)
        if node.account.exists(RedpandaService.PERSISTENT_ROOT):
            hidden_cache_folder = f"{RedpandaService.PERSISTENT_ROOT}/.cache"
            self.logger.debug(f"Checking for presence of {hidden_cache_folder}")
            if node.account.exists(hidden_cache_folder):
                self.logger.debug(
                    f"Seeing {hidden_cache_folder}, removing that specifically first"
                )
                node.account.remove(hidden_cache_folder)
            if node.account.sftp_client.listdir(RedpandaService.PERSISTENT_ROOT):
                if not preserve_logs:
                    node.account.remove(f"{RedpandaService.PERSISTENT_ROOT}/*")
                else:
                    node.account.remove(f"{RedpandaService.PERSISTENT_ROOT}/data/*")
        if node.account.exists(RedpandaService.NODE_CONFIG_FILE):
            node.account.remove(f"{RedpandaService.NODE_CONFIG_FILE}")
        if node.account.exists(RedpandaService.RPK_CONFIG_FILE):
            node.account.remove(f"{RedpandaService.RPK_CONFIG_FILE}")
        if node.account.exists(RedpandaService.CLUSTER_BOOTSTRAP_CONFIG_FILE):
            node.account.remove(f"{RedpandaService.CLUSTER_BOOTSTRAP_CONFIG_FILE}")
        if not preserve_logs and node.account.exists(self.EXECUTABLE_SAVE_PATH):
            node.account.remove(self.EXECUTABLE_SAVE_PATH)

        if node.account.exists(RedpandaService.SYSTEM_TLS_CA_CRT_FILE):
            node.account.remove(RedpandaService.SYSTEM_TLS_CA_CRT_FILE)
            node.account.ssh("update-ca-certificates")

        if node.account.exists(RedpandaService.TEMP_OSSL_CONFIG_FILE):
            node.account.remove(RedpandaService.TEMP_OSSL_CONFIG_FILE)

        if not preserve_current_install or not self._installer._started:
            # Reset the binaries to use the original binaries.
            # NOTE: if the installer hasn't been started, there is no
            # installation to preserve!
            self._installer.reset_current_install([node])

        self.clear_cached_broker_metadata(node)

    def remove_local_data(self, node: ClusterNode):
        node.account.remove(f"{RedpandaService.PERSISTENT_ROOT}/data/*")
        # clear the cached metadata since Redpanda often comes up with new
        # broker IDs, so the old broker IDs are invalid
        self.clear_cached_broker_metadata(node)

    def clear_cached_broker_metadata(self, node: ClusterNode):
        """Roughly speaking, this clears any internal metadata about the given node, so that
        the data is queried freshly from the node when it is needed. It should be
        called when an operation that would invalidate this cached metadata is performed."""

        # clear cached index -> id map: this could be invalided by changing the cluster shape
        self.logger.debug(f"Clearing cached broker metadata for {node.name}")
        index = self.idx(node)
        assert index > 0
        if index in self._node_id_by_idx:
            del self._node_id_by_idx[index]

    def redpanda_pid(self, node: ClusterNode):
        try:
            cmd = "pgrep --list-full --exact redpanda"
            for line in node.account.ssh_capture(cmd, timeout_sec=10):
                # Ignore SSH commands that lookup the version of redpanda
                # by running `redpanda --version` like in `self.get_version(node)`
                if "--version" in line:
                    continue

                self.logger.debug(f"{node.name}: pgrep output: {line}")

                # The pid is listed first, that's all we need
                return int(line.split()[0])
            return None
        except RemoteCommandError as e:
            # 1 - No processes matched or none of them could be signalled.
            if e.exit_status == 1:
                self.logger.debug(f"{node.name}: redpanda process not found")
                return None

            self.logger.error(f"{node.name}: error checking redpanda pid: {e}")

            raise e

    def redpanda_tid(self, node: ClusterNode, thread: str):
        """Return the thread group ID and thread ID of the given thread"""
        cmd = "ps -C redpanda -T"
        for line in node.account.ssh_capture(cmd, timeout_sec=10):
            # Example line:
            #     PID    SPID TTY          TIME CMD
            # 2662879 2662879 pts/16   00:00:02 redpanda
            self.logger.debug(f"ps output: {line}")
            parts = line.split()
            thread_name = parts[4]
            if thread == thread_name:
                return int(parts[0]), int(parts[1])
        raise RuntimeError(f"Thread {thread} not found in ps output")

    def started_nodes(self) -> List[ClusterNode]:
        return list(self._started)

    def render(self, path: str, **kwargs: Any):
        with self.config_file_lock:
            return super(RedpandaService, self).render(path, **kwargs)

    @staticmethod
    def get_node_fqdn(node: ClusterNode):
        ip = socket.gethostbyname(node.account.hostname)
        hostname = (
            node.account.ssh_output(cmd=f"dig -x {ip} +short", timeout_sec=10)
            .decode("utf-8")
            .split("\n")[0]
            .removesuffix(".")
        )
        fqdn = (
            node.account.ssh_output(cmd=f"host -t A {hostname}", timeout_sec=10)
            .decode("utf-8")
            .split(" ")[0]
        )
        return fqdn

    def write_openssl_config_file(self, node: ClusterNode):
        conf = self.render(
            "openssl.cnf",
            fips_conf_file=os.path.join(
                self.rp_install_path(), "openssl/fipsmodule.cnf"
            ),
        )
        self.logger.debug(
            f"Writing {RedpandaService.TEMP_OSSL_CONFIG_FILE} to {node.name}:\n{conf}"
        )
        node.account.create_file(RedpandaService.TEMP_OSSL_CONFIG_FILE, conf)

    def get_openssl_config_file_path(self) -> str:
        path = os.path.join(self.rp_install_path(), self.OPENSSL_CONFIG_FILE_BASE)
        if self.rp_install_path() != "/opt/redpanda":
            # If we aren't using an 'installed' Redpanda instance, the openssl config file
            # located in the install path will not point to the correct location of the FIPS
            # module config file.  We generate an openssl config file just for this purpose
            # see write_openssl_config_file above
            path = RedpandaService.TEMP_OSSL_CONFIG_FILE

        self.logger.debug(
            f"OpenSSL Config File Path: {path} ({self.rp_install_path()})"
        )
        return path

    def get_openssl_modules_directory(self) -> str:
        path = os.path.join(self.rp_install_path(), self.OPENSSL_MODULES_PATH_BASE)

        self.logger.debug(
            f"OpenSSL Modules Directory: {path} ({self.rp_install_path()})"
        )
        return path

    def write_node_conf_file(
        self,
        node: ClusterNode,
        override_cfg_params: dict[str, Any] | None = None,
        auto_assign_node_id: bool = False,
        omit_seeds_on_idx_one: bool = True,
        node_id_override: int | None = None,
    ):
        """
        Write the node config file for a redpanda node: this is the YAML representation
        of Redpanda's `node_config` class.  Distinct from Redpanda's _cluster_ configuration
        which is written separately.
        """
        node_info = {self.idx(n): n for n in self.nodes}

        include_seed_servers = True
        if node_id_override:
            assert not auto_assign_node_id, (
                "Can not use node id override when auto assigning node ids"
            )
            node_id = node_id_override
        else:
            node_id = self.idx(node)
        if omit_seeds_on_idx_one and node_id == 1:
            include_seed_servers = False

        if auto_assign_node_id:
            # Supply None so it's omitted from the config.
            node_id = None

        # Grab the IP to use it as an alternative listener address, to
        # exercise code paths that deal with multiple listeners
        node_ip = socket.gethostbyname(node.account.hostname)

        # Grab the node's FQDN which is needed for Kerberos name
        # resolution
        fqdn = self.get_node_fqdn(node)

        if self._security.tls_provider and self._rpk_node_config is not None:
            self._rpk_node_config.ca_file = RedpandaService.TLS_CA_CRT_FILE

        conf = self.render(
            "redpanda.yaml",
            node=node,
            data_dir=RedpandaService.DATA_DIR,
            nodes=node_info,
            node_id=node_id,
            include_seed_servers=include_seed_servers,
            seed_servers=self._seed_servers,
            node_ip=node_ip,
            kafka_alternate_port=self.KAFKA_ALTERNATE_PORT,
            kafka_kerberos_port=self.KAFKA_KERBEROS_PORT,
            fqdn=fqdn,
            admin_alternate_port=self.ADMIN_ALTERNATE_PORT,
            pandaproxy_config=self._pandaproxy_config,
            schema_registry_config=self._schema_registry_config,
            audit_log_config=self._audit_log_config,
            superuser=self._superuser,
            sasl_enabled=self.sasl_enabled(),
            endpoint_authn_method=self.endpoint_authn_method(),
            auto_auth=self._security.auto_auth,
            rpk_node_config=self._rpk_node_config,
        )

        def is_fips_capable(node: ClusterNode) -> bool:
            cur_ver = self._installer.installed_version(node)
            return cur_ver == RedpandaInstaller.HEAD or cur_ver >= (24, 2, 1)

        if in_fips_environment() and is_fips_capable(node):
            fips_mode = get_fips_mode().value
            self.logger.info(f"Setting Redpanda to FIPS mode: {fips_mode}")
            doc = yaml.full_load(conf)
            doc["redpanda"].update(
                dict(
                    fips_mode=fips_mode,
                    openssl_config_file=self.get_openssl_config_file_path(),
                    openssl_module_directory=self.get_openssl_modules_directory(),
                )
            )
            conf = yaml.dump(doc)

        if override_cfg_params or node in self._extra_node_conf:
            doc = yaml.full_load(conf)
            doc["redpanda"].update(self._extra_node_conf[node])
            self.logger.debug(
                f"extra_node_conf[{node.name}]: {self._extra_node_conf[node]}"
            )
            if override_cfg_params:
                self.logger.debug(
                    "Setting custom node configuration options: {}".format(
                        override_cfg_params
                    )
                )
                doc["redpanda"].update(override_cfg_params)
            conf = yaml.dump(doc)

        if self._security.tls_provider:
            p12_password = (
                self._security.tls_provider.p12_password(node)
                if self._security.tls_provider.use_pkcs12_file()
                else None
            )
            tls_config = [
                dict(
                    enabled=True,
                    require_client_auth=self.require_client_auth(),
                    name=n,
                    truststore_file=RedpandaService.TLS_CA_CRT_FILE,
                    crl_file=RedpandaService.TLS_CA_CRL_FILE,
                )
                for n in ["dnslistener", "iplistener"]
            ]
            for n in tls_config:
                if p12_password is None:
                    n["cert_file"] = RedpandaService.TLS_SERVER_CRT_FILE
                    n["key_file"] = RedpandaService.TLS_SERVER_KEY_FILE
                else:
                    n["p12_file"] = RedpandaService.TLS_SERVER_P12_FILE
                    n["p12_password"] = p12_password
            doc = yaml.full_load(conf)
            doc["redpanda"].update(dict(kafka_api_tls=tls_config))
            conf = yaml.dump(doc)

        self.logger.info(
            "Writing Redpanda node config file: {}".format(
                RedpandaService.NODE_CONFIG_FILE
            )
        )
        self.logger.debug(conf)
        node.account.create_file(RedpandaService.NODE_CONFIG_FILE, conf)

        self._node_configs[node] = yaml.full_load(conf)

    def find_path_to_rpk(self) -> str:
        return f"{self._context.globals.get('rp_install_path_root', None)}/bin/rpk"

    def write_bootstrap_cluster_config(self):
        conf = copy.deepcopy(self.CLUSTER_CONFIG_DEFAULTS)

        cur_ver = self._installer.installed_version(self.nodes[0])
        if cur_ver != RedpandaInstaller.HEAD and cur_ver < (22, 2, 7):
            # this configuration property was introduced in 22.2, ensure
            # it doesn't appear in older configurations
            del conf["log_segment_size_jitter_percent"]
        if cur_ver != RedpandaInstaller.HEAD and cur_ver < (23, 2, 1):
            # versions prior to 23.2.1 does not support
            # partition_manager_shutdown_watchdog_timeout property
            del conf["partition_manager_shutdown_watchdog_timeout"]
        if self._extra_rp_conf:
            self.logger.debug(
                "Setting custom cluster configuration options: {}".format(
                    self._extra_rp_conf
                )
            )
            conf.update(self._extra_rp_conf)

        if cur_ver != RedpandaInstaller.HEAD and cur_ver < (23, 2, 1):
            # this configuration property was introduced in 23.2, ensure
            # it doesn't appear in older configurations
            conf.pop("retention_local_strict", None)

        if self._security.enable_sasl:
            self.logger.debug("Enabling SASL in cluster configuration")
            conf.update(dict(enable_sasl=True))
        if self._security.kafka_enable_authorization is not None:
            self.logger.debug(
                f"Setting kafka_enable_authorization: {self._security.kafka_enable_authorization} in cluster configuration"
            )
            conf.update(
                dict(
                    kafka_enable_authorization=self._security.kafka_enable_authorization
                )
            )
        if self._security.sasl_mechanisms is not None:
            self.logger.debug(
                f"Setting sasl_mechanisms: {self._security.sasl_mechanisms} in cluster configuration"
            )
            conf.update(dict(sasl_mechanisms=self._security.sasl_mechanisms))
        if self._security.sasl_mechanisms_overrides is not None:
            self.logger.debug(
                f"Setting sasl_mechanisms_overrides: {self._security.sasl_mechanisms_overrides} in cluster configuration"
            )
            conf.update(
                dict(sasl_mechanisms_overrides=self._security.sasl_mechanisms_overrides)
            )

        if self._security.http_authentication is not None:
            self.logger.debug(
                f"Setting http_authentication: {self._security.http_authentication} in cluster configuration"
            )
            conf.update(dict(http_authentication=self._security.http_authentication))

        # Only override `rpk_path` if not already provided
        if (
            cur_ver == RedpandaInstaller.HEAD or cur_ver >= (24, 3, 1)
        ) and "rpk_path" not in conf:
            # Introduced rpk_path to v24.3
            rpk_path = self.find_path_to_rpk()
            conf.update(dict(rpk_path=rpk_path))

        conf_yaml = yaml.dump(conf)

        def write_config_to_node(node: ClusterNode) -> None:
            self.logger.info(
                "Writing bootstrap cluster config file {}:{}".format(
                    node.name, RedpandaService.CLUSTER_BOOTSTRAP_CONFIG_FILE
                )
            )
            node.account.mkdirs(
                os.path.dirname(RedpandaService.CLUSTER_BOOTSTRAP_CONFIG_FILE)
            )
            node.account.create_file(
                RedpandaService.CLUSTER_BOOTSTRAP_CONFIG_FILE, conf_yaml
            )

        self.for_nodes(self.nodes, write_config_to_node)

    def get_node_by_id(self, node_id: int) -> ClusterNode | None:
        """
        Returns the node that has the requested id or None if node is not found.

        If you expect the node to exist, you may prefer `self.node_by_id(node_id)`,
        which raises an exception if the node is not found.

        This function ignores nodes which are not started (in general it needs to node
        to be up to query its ID).
        """
        for n in self.nodes:
            try:
                if self.node_id(n) == node_id:
                    return n
            except BrokerNotStartedError:
                # skip nodes that are not started (though we may stil obtain
                # their information if it is cached: this exception indicates
                # the the cache was not populated for this node)
                pass

        return None

    def node_by_id(self, node_id: int) -> ClusterNode:
        """
        Returns the node that has requested id or throws NodeNotFoundError
        if not found.
        """
        nid = self.get_node_by_id(node_id)
        if nid is None:
            self.logger.info(
                f"Node with id {node_id} not found, idx_to_id {self._node_id_by_idx}"
            )
            raise NodeNotFoundError(f"Node with id {node_id} not found")
        return nid

    def registered(self, node: ClusterNode):
        """
        Check if a newly added node is fully registered with the cluster, such
        that a kafka metadata request to any node in the cluster will include it.

        We first check the admin API to do a kafka-independent check, and then verify
        that kafka clients see the same thing.
        """
        node_id = self.node_id(node, force_refresh=True)
        self.logger.debug(
            f"registered: checking if broker {node_id} ({node.name}) is registered..."
        )

        # Query all nodes' admin APIs, so that we don't advance during setup until
        # the node is stored in raft0 AND has been replayed on all nodes.  Otherwise
        # a kafka metadata request to the last node to join could return incomplete
        # metadata and cause strange issues within a test.
        for peer in self._started:
            try:
                admin_brokers = self._admin.get_brokers(node=peer)
            except requests.exceptions.RequestException as e:
                # We run during startup, when admin API may not even be listening yet: tolerate
                # API errors but presume that if some APIs are not up yet, then node registration
                # is also not complete.
                self.logger.debug(
                    f"registered: peer {peer.name} admin API not yet available ({e})"
                )
                return False
            found = None
            for b in admin_brokers:
                if b["node_id"] == node_id:
                    found = b
                    break

            if not found:
                self.logger.info(
                    f"registered: node {node.name} not yet found in peer {peer.name}'s broker list ({admin_brokers})"
                )
                return False
            else:
                if not found["is_alive"]:
                    self.logger.info(
                        f"registered: node {node.name} found in {peer.name}'s broker list ({admin_brokers}) but not yet marked as alive"
                    )
                    return False
                self.logger.debug(
                    f"registered: node {node.name} now visible in peer {peer.name}'s broker list ({admin_brokers})"
                )

        if self.brokers():  # Conditional in case Kafka API turned off
            if self.sasl_enabled():
                client = PythonLibrdkafka(
                    self,
                    tls_cert=self._tls_cert,
                    username=self._superuser.username,
                    password=self._superuser.password,
                    algorithm=self._superuser.algorithm,
                )
            else:
                client = PythonLibrdkafka(self, tls_cert=self._tls_cert)
            brokers = client.brokers()
            broker = brokers.get(node_id, None)
            if broker is None:
                # This should never happen, because we already checked via the admin API
                # that the node of interest had become visible to all peers.
                self.logger.error(
                    f"registered: node {node.name} not found in kafka metadata!"
                )
                assert broker is not None

            self.logger.debug(f"registered: found broker info: {broker}")

        return True

    def controller(self):
        """
        :return: the ClusterNode that is currently controller leader, or None if no leader exists
        """
        for node in self.started_nodes():
            try:
                r = requests.request(
                    "get",
                    f"http://{node.account.hostname}:9644/v1/partitions/redpanda/controller/0",
                    timeout=10,
                )
            except requests.exceptions.RequestException:
                continue

            if r.status_code != 200:
                continue
            else:
                resp_leader_id = r.json()["leader_id"]
                if resp_leader_id != -1:
                    return self.get_node_by_id(resp_leader_id)

        return None

    def node_storage(
        self,
        node: ClusterNode,
        sizes: bool = False,
        compaction_footers: bool = False,
    ) -> NodeStorage:
        """
        Retrieve a summary of storage on a node.

        :param sizes: if true, stat each segment file and record its size in the
                      `size` attribute of Segment.
        """

        self.logger.debug(f"Starting storage checks for {node.name} sizes={sizes}")
        store = NodeStorage(node.name, RedpandaService.DATA_DIR)
        script_path = inject_remote_script(node, "compute_storage.py")
        cmd = ["python3", script_path, f"--data-dir={RedpandaService.DATA_DIR}"]
        if sizes:
            cmd.append("--sizes")
        if compaction_footers:
            cmd.append("--compaction-footers")
        output = node.account.ssh_output(
            shlex.join(cmd), combine_stderr=False, timeout_sec=10
        )
        namespaces = json.loads(output)
        for ns, topics in namespaces.items():
            ns_path = os.path.join(store.data_dir, ns)
            ns = store.add_namespace(ns, ns_path)
            for topic, partitions in topics.items():
                topic_path = os.path.join(ns_path, topic)
                topic = ns.add_topic(topic, topic_path)
                for part, segments in partitions.items():
                    partition_path = os.path.join(topic_path, part)
                    partition = topic.add_partition(part, node, partition_path)
                    partition.add_files(list(segments.keys()))
                    for segment, data in segments.items():
                        if "size" in data:
                            partition.set_segment_size(segment, data["size"])
                        if "compaction_footer" in data:
                            partition.set_segment_compaction_footer(
                                segment, data["compaction_footer"]
                            )

        self.logger.debug(f"Finished storage checks for {node.name} sizes={sizes}")

        return store

    def storage(
        self,
        *,
        nodes: Collection[ClusterNode] | None = None,
        sizes: bool = False,
    ) -> ClusterStorage:
        """
        :param nodes: if None, only report on started nodes. Otherwise, report
                      on the nodes in the `nodes` list parameter.
        :param sizes: if true, stat each segment file and record its size in the
                      `size` attribute of Segment.

        :returns: instances of ClusterStorage
        """
        if nodes is None:
            nodes = self._started
        assert nodes, "Empty node list specified for storage stat collection"

        store = ClusterStorage()
        self.logger.debug(f"Starting storage checks nodes={nodes} sizes={sizes}")

        def compute_node_storage(node: ClusterNode):
            s = self.node_storage(node, sizes=sizes)
            store.add_node(s)

        self.for_nodes(nodes, compute_node_storage)
        self.logger.debug(f"Finished storage checks nodes={nodes} sizes={sizes}")
        return store

    def copy_data(self, dest: str, node: ClusterNode):
        # after copying, move all files up a directory level so the caller does
        # not need to know what the name of the storage directory is.
        with tempfile.TemporaryDirectory() as d:
            node.account.copy_from(RedpandaService.DATA_DIR, d)
            data_dir = os.path.basename(RedpandaService.DATA_DIR)
            data_dir = os.path.join(d, data_dir)
            for fn in os.listdir(data_dir):
                shutil.move(os.path.join(data_dir, fn), dest)

    def data_checksum(self, node: ClusterNode) -> FileToChecksumSize:
        """Run command that computes MD5 hash of every file in redpanda data
        directory. The results of the command are turned into a map from path
        to hash-size tuples."""
        script_path = inject_remote_script(node, "compute_storage.py")
        cmd = f"python3 {script_path} --sizes --md5 --print-flat --data-dir {RedpandaService.DATA_DIR}"
        lines = node.account.ssh_output(cmd, timeout_sec=120)
        lines = lines.decode().split("\n")

        # there is a race between `find` iterating over file names and passing
        # those to an invocation of `md5sum` in which the file may be deleted.
        # here we log these instances for debugging, but otherwise ignore them.
        found: list[str] = []
        for line in lines:
            if "No such file or directory" in line:
                self.logger.debug(f"Skipping file that disappeared: {line}")
                continue
            found.append(line)
        lines = found

        # the `find` command will stick a newline at the end of the results
        # which gets parsed as an empty line by `split` above
        if lines[-1] == "":
            lines.pop()

        return {
            tokens[0]: (tokens[2], int(tokens[1]))
            for tokens in map(lambda l: l.split(), lines)
        }

    def data_stat(self, node: ClusterNode):
        """
        Return a collection of (file path, file size) tuples for all files found
        under the Redpanda data directory. File paths are normalized to be relative
        to the data directory.
        """
        # -ignore_readdir_race: a file may disappear between find's readdir and
        # its internal stat for `%s` (e.g. segment GC during traversal). Without
        # this flag, find emits a stderr warning and exits non-zero, which would
        # cause ssh_output to raise before our filtering can drop the warning.
        cmd = f"find {RedpandaService.DATA_DIR} -ignore_readdir_race -type f -printf '%p %s\\n'"
        lines = node.account.ssh_output(cmd, timeout_sec=120)
        lines = lines.decode().split("\n")

        # skip empty lines: find may stick one on the end of the results
        lines = filter(lambda l: len(l) > 0, lines)

        # split into pathlib.Path / file size pairs
        parts = map(lambda l: l.split(), lines)
        parts = ((pathlib.Path(f), int(s)) for f, s in parts)

        # return results with relative paths
        data_path = pathlib.Path(RedpandaService.DATA_DIR)
        return ((p.relative_to(data_path), s) for p, s in parts)

    def data_dir_usage(self, subdir: str, node: ClusterNode):
        """
        Return a rolled up disk usage report for the given data sub-directory.
        """
        dir = os.path.join(RedpandaService.DATA_DIR, subdir)
        if not node.account.exists(dir):
            return 0
        script_path = inject_remote_script(node, "disk_usage.py")
        cmd = ["python3", script_path, dir]
        return int(node.account.ssh_output(shlex.join(cmd), timeout_sec=10))

    def broker_address(
        self, node: ClusterNode, listener: str = "dnslistener"
    ) -> str | None:
        assert node in self.nodes, f"Node {node.account.hostname} is not started"
        assert node in self._started
        cfg = self._node_configs[node]
        if cfg["redpanda"]["kafka_api"]:
            if isinstance(cfg["redpanda"]["kafka_api"], list):
                for entry in cfg["redpanda"]["kafka_api"]:
                    if entry["name"] == listener:
                        return f"{entry['address']}:{entry['port']}"
            else:
                entry = cfg["redpanda"]["kafka_api"]
                return f"{entry['address']}:{entry['port']}"
        else:
            return None

    def admin_endpoint(self, node: ClusterNode):
        assert node in self.nodes, f"Node {node.account.hostname} is not started"
        return f"{node.account.hostname}:9644"

    def admin_endpoints_list(self):
        brokers = [self.admin_endpoint(n) for n in self._started]
        random.shuffle(brokers)
        return brokers

    def admin_endpoints(self):
        return ",".join(self.admin_endpoints_list())

    def brokers(self, limit: int | None = None, listener: str = "dnslistener") -> str:
        return ",".join(self.brokers_list(limit, listener))

    def brokers_list(
        self, limit: int | None = None, listener: str = "dnslistener"
    ) -> list[str]:
        brokers = [
            self.broker_address(n, listener) for n in list(self._started)[:limit]
        ]
        brokers = [b for b in brokers if b is not None]
        random.shuffle(brokers)
        return brokers

    def schema_reg(self, limit: int | None = None) -> str:
        schema_reg = [
            f"http://{n.account.hostname}:8081" for n in list(self._started)[:limit]
        ]
        return ",".join(schema_reg)

    def shards(self):
        """
        Fetch the max shard id for each node.
        """

        def get_node_shards(node: ClusterNode) -> tuple[int, int]:
            num_shards = 0
            metrics = self.metrics(node)
            for family in metrics:
                for sample in family.samples:
                    if sample.name == "vectorized_reactor_utilization":
                        num_shards = max(num_shards, int(sample.labels["shard"]))
            assert num_shards > 0
            return (self.idx(node), num_shards)

        return dict(self.for_nodes(list(self._started), get_node_shards))

    def cov_enabled(self):
        cov_option = self._context.globals.get(self.COV_KEY, self.DEFAULT_COV_OPT)
        if cov_option == "ON":
            return True
        elif cov_option == "OFF":
            return False

        self.logger.warning(f"{self.COV_KEY} should be one of 'ON', or 'OFF'")
        return False

    def count_log_node(self, node: ClusterNode, pattern: str):
        accum = 0
        for line in node.account.ssh_capture(
            f'grep "{pattern}" {RedpandaService.STDOUT_STDERR_CAPTURE} || true',
            timeout_sec=60,
        ):
            # We got a match
            self.logger.debug(f"Found {pattern} on node {node.name}: {line}")
            accum += 1

        return accum

    def search_log_node(self, node: ClusterNode, pattern: str):
        for line in node.account.ssh_capture(
            f'grep "{pattern}" {RedpandaService.STDOUT_STDERR_CAPTURE} || true',
            timeout_sec=60,
        ):
            # We got a match
            self.logger.debug(f"Found {pattern} on node {node.name}: {line}")
            return True

        return False

    def search_log_any(self, pattern: str, nodes: list[ClusterNode] | None = None):
        """
        Test helper for grepping the redpanda log.
        The design follows python's built-in any() function.
        https://docs.python.org/3/library/functions.html#any

        :param pattern: the string to search for
        :param nodes: a list of nodes to run grep on
        :return:  true if any instances of `pattern` found
        """
        if nodes is None:
            nodes = self.nodes

        for node in nodes:
            if self.search_log_node(node, pattern):
                return True

        # Fall through, no matches
        return False

    def search_log_all(self, pattern: str, nodes: list[ClusterNode] | None = None):
        # Test helper for grepping the redpanda log
        # The design follows python's  built-in all() function.
        # https://docs.python.org/3/library/functions.html#all

        # :param pattern: the string to search for
        # :param nodes: a list of nodes to run grep on
        # :return:  true if `pattern` is found in all nodes
        if nodes is None:
            nodes = self.nodes

        def search_node(node: ClusterNode) -> bool:
            exit_status = node.account.ssh(
                f'grep "{pattern}" {RedpandaService.STDOUT_STDERR_CAPTURE}',
                allow_fail=True,
            )
            if exit_status != 0:
                self.logger.debug(f"Did not find {pattern} on node {node.name}")
                return False
            return True

        return all(self.for_nodes(nodes, search_node))

    def wait_for_controller_snapshot(
        self,
        node: ClusterNode,
        prev_mtime: float = 0,
        prev_start_offset: int = 0,
        timeout_sec: int = 30,
    ):
        def check():
            snap_path = os.path.join(self.DATA_DIR, "redpanda/controller/0_0/snapshot")
            try:
                stat = node.account.sftp_client.stat(snap_path)
                mtime = stat.st_mtime
                size = stat.st_size
            except FileNotFoundError:
                mtime = 0
                size = 0

            controller_status = self._admin.get_controller_status(node)
            self.logger.info(
                f"node {node.account.hostname}: "
                f"controller status: {controller_status}, "
                f"snapshot size: {size}, mtime: {mtime}"
            )

            so = controller_status["start_offset"]
            return (mtime > prev_mtime and so > prev_start_offset, (mtime, so))

        return wait_until_result(check, timeout_sec=timeout_sec, backoff_sec=1)

    def set_expected_controller_records(self, max_records: int | None):
        self._expect_max_controller_records = max_records

    def set_up_failure_injection(
        self,
        finject_cfg: FailureInjectionConfig,
        enabled: bool,
        nodes: list[ClusterNode],
        tolerate_crashes: bool,
    ):
        """
        Deploy the given failure injection configuration to the
        requested nodes. Should be called before
        `RedpandaService.start`.
        """
        tmp_file = "/tmp/failure_injection_config.json"
        finject_cfg.write_to_file(tmp_file)

        def setup_node(node: ClusterNode) -> None:
            node.account.mkdirs(
                os.path.dirname(RedpandaService.FAILURE_INJECTION_CONFIG_PATH)
            )
            node.account.copy_to(
                tmp_file, RedpandaService.FAILURE_INJECTION_CONFIG_PATH
            )
            self._extra_node_conf[node].update(
                {
                    "storage_failure_injection_enabled": enabled,
                    "storage_failure_injection_config_path": RedpandaService.FAILURE_INJECTION_CONFIG_PATH,
                }
            )

        self.for_nodes(nodes, setup_node)

        # Disable segment size jitter in order to get more deterministic
        # failure injection.
        self.add_extra_rp_conf({"log_segment_size_jitter_percent": 0})

        # This flag prevents RedpandaService from asserting out when it
        # detects that a Redpanda node has crashed. See
        # `RedpandaService.wait_until`.
        self._failure_injection_enabled = True

        self._tolerate_crashes = tolerate_crashes

        self._log_config.enable_finject_logging()

        self.logger.info(f"Set up failure injection config for nodes: {nodes}")

    def set_tolerate_crashes(self, tolerate_crashes: bool):
        """
        Do not fail test on crashes including asserts. This is useful when
        running with redpanda's fault injection enabled.
        """
        self._tolerate_crashes = tolerate_crashes

    def validate_controller_log(self):
        """
        This method is for use at end of tests, to detect issues that might
        lead to huge numbers of writes to the controller log (e.g. rogue
        loops/retries).

        Any test that intentionally
        """

        def get_node_length(node: ClusterNode) -> int | None:
            try:
                status = self._admin.get_controller_status(node=node)
                return status["committed_index"] - max(0, status["start_offset"] - 1)
            except Exception as e:
                self.logger.warning(
                    f"Failed to read controller status from {node.name}: {e}"
                )
                return None

        lengths = [
            l
            for l in self.for_nodes(list(self.started_nodes()), get_node_length)
            if l is not None
        ]
        max_length = max(lengths) if lengths else None

        if max_length is None:
            self.logger.warning(
                "Failed to read controller status from any node, cannot validate record count"
            )
            return

        if self._expect_max_controller_records is not None:
            self.logger.debug(
                f"Checking controller record count ({max_length}/{self._expect_max_controller_records})"
            )
            if max_length > self._expect_max_controller_records:
                raise RuntimeError(
                    f"Oversized controller log detected!  {max_length} records"
                )

    def estimate_bytes_written(self) -> float | None:
        try:
            samples = self.metrics_sample(
                "vectorized_io_queue_total_write_bytes_total",
                nodes=self.started_nodes(),
            )
        except Exception as e:
            self.logger.warning(
                f"Cannot check metrics, did a test finish with all nodes down? ({e})"
            )
            return None

        if samples is not None and samples.samples:
            return sum(s.value for s in samples.samples)
        else:
            return None

    def estimate_total_disk_bytes_read(self):
        try:
            samples = self.metrics_sample(
                "vectorized_io_queue_total_read_bytes_total", nodes=self.started_nodes()
            )
        except Exception as e:
            self.logger.warning(
                f"Cannot check metrics, did a test finish with all nodes down? ({e})"
            )
            return None

        if samples is not None and samples.samples:
            return sum(s.value for s in samples.samples)
        else:
            return None

    def wait_node_add_rebalance_finished(
        self,
        new_nodes: list[ClusterNode],
        admin: Admin | None = None,
        min_partitions: int = 5,
        progress_timeout: int = 60,
        timeout: int = 300,
        backoff: int = 2,
    ):
        """Waits until the rebalance triggered by adding new nodes is finished."""

        new_node_names = [n.name for n in new_nodes]

        if admin is None:
            admin = Admin(self)
        started_at = time.monotonic()
        last_reconfiguring: set[str] = set()
        last_bytes_moved = 0
        last_update = started_at

        while True:
            time.sleep(backoff)

            if time.monotonic() - last_update > progress_timeout:
                raise TimeoutError(
                    f"rebalance after adding nodes {new_node_names} "
                    "stopped making progress"
                )

            if time.monotonic() - started_at > timeout:
                raise TimeoutError(
                    f"rebalance after adding nodes {new_node_names} timed out"
                )

            cur_reconfiguring: set[str] = set()
            cur_bytes_moved = 0
            for p in admin.list_reconfigurations():
                cur_reconfiguring.add(f"{p['ns']}/{p['topic']}/{p['partition']}")
                cur_bytes_moved += p["bytes_moved"]

            if (
                cur_reconfiguring != last_reconfiguring
                or cur_bytes_moved != last_bytes_moved
            ):
                last_update = time.monotonic()

            last_reconfiguring = cur_reconfiguring
            last_bytes_moved = cur_bytes_moved

            if len(cur_reconfiguring) > 0:
                continue

            partition_counts = [len(admin.get_partitions(node=n)) for n in new_nodes]
            if any(pc < min_partitions for pc in partition_counts):
                continue

            return


def make_redpanda_service(
    context: TestContext,
    num_brokers: int | None,
    *,
    extra_rp_conf: dict[str, Any] | None = None,
    **kwargs: Any,
) -> RedpandaService:
    """Factory function for instatiating the appropriate RedpandaServiceABC subclass."""

    if num_brokers is None:
        # Default to a 3 node cluster if sufficient nodes are available, else
        # a single node cluster.  This is just a default: tests are welcome
        # to override constructor to pass an explicit size.  This logic makes
        # it convenient to mix 3 node and 1 node cases in the same class, by
        # just modifying the @cluster node count per test.
        if context.cluster.available().size() >= 3:
            num_brokers = 3
        else:
            num_brokers = 1

    return RedpandaService(context, num_brokers, extra_rp_conf=extra_rp_conf, **kwargs)


class ValidationMode(str, Enum):
    """
    Redpanda validation modes (see cluster config `kafka_produce_batch_validation`).
    """

    LEGACY = "legacy"
    RELAXED = "relaxed"
    STRICT = "strict"
