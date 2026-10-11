# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re
import time
from logging import Logger
from typing import Callable

from ducktape.mark import parametrize
from ducktape.mark.resource import cluster as ducktape_cluster
from ducktape.tests.test import Test
from kafkatest.services.kafka import KafkaService
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.version import V_3_0_0

from rptest.clients.default import DefaultClient
from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kafka import KafkaServiceAdapter
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.metrics_check import MetricCheck
from rptest.services.redpanda import RedpandaService
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until


class BaseTimeQuery:
    log_segment_size: int
    read_bytes_limit: int
    client: Callable[[], DefaultClient]
    test_context: dict
    logger: Logger

    base_ts = int(time.time() - 600) * 1000

    def _create_and_produce(self, cluster, record_size, msg_count):
        topic = TopicSpec(name="tqtopic", partition_count=1, replication_factor=3)
        self.client().create_topic(topic)

        # Configure topic to trust client-side timestamps, so that
        # we can generate fake ones for the test
        self.client().alter_topic_config(
            topic.name, "message.timestamp.type", "CreateTime"
        )

        # Disable time based retention because we will use synthetic timestamps
        # that may well fall outside of the default 1 week relative to walltime
        self.client().alter_topic_config(topic.name, "retention.ms", "-1")

        # Use small segments
        self.client().alter_topic_config(
            topic.name, "segment.bytes", self.log_segment_size
        )

        # Produce a run of messages with CreateTime-style timestamps, each
        # record having a timestamp 1ms greater than the last.
        producer = KgoVerifierProducer(
            context=self.test_context,
            redpanda=cluster,
            topic=topic.name,
            msg_size=record_size,
            msg_count=msg_count,
            # A respectable number of messages per batch so that we are covering
            # the case of looking up timestamps that fall in the middle of a batch,
            # but small enough that we are getting plenty of distinct batches.
            batch_max_bytes=record_size * 10,
            # A totally arbitrary artificial timestamp base in milliseconds
            fake_timestamp_ms=self.base_ts,
        )
        producer.start()
        producer.wait()

        # We know timestamps, they are generated linearly from the
        # base we provided to kgo-verifier
        timestamps = dict((i, self.base_ts + i) for i in range(0, msg_count))
        return topic, timestamps

    def _test_timequery(self, cluster, batch_cache: bool):
        total_segments = 12
        record_size = 1024
        msg_count = (self.log_segment_size * total_segments) // record_size
        kcat = KafkaCat(cluster)

        # Test the base case with an empty topic.
        empty_topic = TopicSpec(
            name="tq_empty_topic", partition_count=1, replication_factor=3
        )
        self.client().create_topic(empty_topic)
        offset = kcat.query_offset(empty_topic.name, 0, self.base_ts)
        self.logger.info(f"Time query returned offset {offset}")
        assert offset == -1, f"Expected -1, got {offset}"

        # Create a topic and produce a run of messages we will query.
        topic, timestamps = self._create_and_produce(cluster, record_size, msg_count)
        for k, v in timestamps.items():
            self.logger.debug(f"  Offset {k} -> Timestamp {v}")

        # Confirm messages written
        rpk = RpkTool(cluster)
        p = next(rpk.describe_topic(topic.name))
        assert p.high_watermark == msg_count

        # Identify partition leader for use in our metrics checks
        leader_node = cluster.get_node(next(rpk.describe_topic(topic.name)).leader)

        is_redpanda = isinstance(cluster, RedpandaService)

        # Class defining expectations of timequery results to be checked
        class ex:
            def __init__(self, offset, ts=None, expect_read=True):
                if ts is None:
                    ts = timestamps[offset]
                self.ts = ts
                self.offset = offset
                self.expect_read = expect_read

        # We will do approx. 10 timequeries within each segment.
        step = msg_count // total_segments // 10

        expectations = []
        for o in range(0, msg_count, step):
            expect_read = o < msg_count
            expectations.append(ex(o, timestamps[o], expect_read))

        # Add edge cases
        expectations += [
            ex(0, timestamps[0] - 1000),  # Before the start of the log
            ex(-1, timestamps[msg_count - 1] + 1000, False),  # After last message
        ]

        local_metrics = None

        def diff_bytes(old, new):
            return new - old <= self.read_bytes_limit

        for e in expectations:
            ts = e.ts
            o = e.offset

            if is_redpanda:
                local_metrics = MetricCheck(
                    self.logger,
                    cluster,
                    leader_node,
                    re.compile("vectorized_storage_.*"),
                    reduce=sum,
                )

            self.logger.info(f"Attempting time lookup ts={ts} (should be o={o})")
            offset = kcat.query_offset(topic.name, 0, ts)
            self.logger.info(f"Time query returned offset {offset}")
            assert offset == o

            if is_redpanda and not batch_cache and e.expect_read:
                # Expect to read at most one segment from disk: this validates that
                # we are correctly looking up the right segment before seeking to
                # the exact offset, and not e.g. reading from the start of the log.
                local_metrics.expect(
                    [("vectorized_storage_log_read_bytes_total", diff_bytes)]
                )

        # TODO: do some double-restarts to generate segments with
        # no user data, to check that their timestamps don't
        # break out indexing.

    def _test_timequery_below_start_offset(self, cluster):
        """
        Run a timequery for an offset that falls below the start offset
        of the local log and ensure that -1 (i.e. not found) is returned.
        """
        total_segments = 3
        local_retain_segments = 1
        record_size = 1024
        msg_count = (self.log_segment_size * total_segments) // record_size

        topic, timestamps = self._create_and_produce(cluster, record_size, msg_count)

        self.client().alter_topic_config(
            topic.name, "retention.bytes", self.log_segment_size * local_retain_segments
        )

        rpk = RpkTool(cluster)

        def start_offset():
            return next(rpk.describe_topic(topic.name)).start_offset

        wait_until(
            lambda: start_offset() > 0,
            timeout_sec=120,
            backoff_sec=5,
            err_msg="Start offset did not advance",
        )

        kcat = KafkaCat(cluster)
        lwm_before = start_offset()
        offset = kcat.query_offset(topic.name, 0, self.base_ts)
        lwm_after = start_offset()

        # When querying before the start of the log, we should get
        # the offset of the start of the log.
        assert offset >= 0
        # The LWM can move during background housekeeping while we
        # are doing timequery, so success condition is that if falls
        # within a range.
        assert offset >= lwm_before
        assert offset <= lwm_after


class TimeQueryTest(RedpandaTest, BaseTimeQuery):
    # We use small segments to enable quickly exercising the
    # lookup of the proper segment for a time index, as well
    # as the lookup of the offset within that segment.
    log_segment_size = 1024 * 1024
    # Upper bound on bytes read from disk by a single timequery: well under
    # one segment, so a lookup that scans from the start of the log fails.
    read_bytes_limit = 1024 * 128 * 5

    def setUp(self):
        # Don't start up redpanda yet, because we will need the
        # test parameter to set cluster configs before starting.
        pass

    def set_up_cluster(self, batch_cache: bool):
        self.redpanda.set_extra_rp_conf(
            {
                # Testing with batch cache disabled is important, because otherwise
                # we won't touch the path in skipping_consumer that applies
                # timestamp bounds
                "disable_batch_cache": not batch_cache,
                # Our time bounds on segment removal depend on the leadership
                # staying in one place.
                "enable_leader_balancer": False,
                "log_segment_size_min": 32 * 1024,
                "log_segment_size": self.log_segment_size,
                # Disable time-based retention so that we can use synthetic
                # timestamps that may fall outside of the default retention window.
                "log_retention_ms": -1,
            }
        )

        # `test_timequery_with_local_gc` attempts to set the `log_segment_size` lower than the minimum
        # value of 1_MiB- disable bounded property checks to allow this.
        self.redpanda.set_environment(
            {"__REDPANDA_TEST_DISABLE_BOUNDED_PROPERTY_CHECKS": "ON"}
        )
        self.redpanda.start()

    @cluster(num_nodes=4)
    @parametrize(batch_cache=True)
    @parametrize(batch_cache=False)
    def test_timequery(self, batch_cache: bool):
        self.set_up_cluster(batch_cache)
        self._test_timequery(cluster=self.redpanda, batch_cache=batch_cache)

    @cluster(num_nodes=4)
    def test_timequery_below_start_offset(self):
        self.set_up_cluster(batch_cache=False)
        self._test_timequery_below_start_offset(cluster=self.redpanda)

    @cluster(num_nodes=4)
    def test_timequery_with_trim_prefix(self):
        self.set_up_cluster(batch_cache=False)
        total_segments = 12
        record_size = 1024
        msg_count = (self.log_segment_size * total_segments) // record_size
        topic, timestamps = self._create_and_produce(
            self.redpanda, record_size, msg_count
        )

        # Confirm messages written
        rpk = RpkTool(self.redpanda)
        p = next(rpk.describe_topic(topic.name))
        assert p.high_watermark == msg_count

        num_batches_per_segment = self.log_segment_size // record_size
        new_lwm = int(num_batches_per_segment * 2.5)
        trim_response = rpk.trim_prefix(topic.name, offset=new_lwm, partitions=[0])
        assert len(trim_response) == 1
        assert new_lwm == trim_response[0].new_start_offset

        # Double check that the start offset has advanced.
        p = next(rpk.describe_topic(topic.name))
        assert new_lwm == p.start_offset, f"Expected {new_lwm}, got {p.start_offset}"

        # Query below valid timestamps the offset of the first message.
        kcat = KafkaCat(self.redpanda)
        offset = kcat.query_offset(topic.name, 0, timestamps[0] - 1000)
        assert offset == new_lwm, f"Expected {new_lwm}, got {offset}"

        # Leave just the last message in the log.
        trim_response = rpk.trim_prefix(
            topic.name, offset=p.high_watermark - 1, partitions=[0]
        )

        # Query below valid timestamps the offset of the only message left.
        kcat = KafkaCat(self.redpanda)
        offset = kcat.query_offset(topic.name, 0, timestamps[0] - 1000)
        assert offset == msg_count - 1, f"Expected {msg_count - 1}, got {offset}"

        # Trim everything, leaving an empty log.
        rpk.trim_prefix(topic.name, offset=p.high_watermark, partitions=[0])
        kcat = KafkaCat(self.redpanda)
        offset = kcat.query_offset(topic.name, 0, timestamps[0] - 1000)
        assert offset == -1, f"Expected -1, got {offset}"


class TimeQueryKafkaTest(Test, BaseTimeQuery):
    """
    Time queries are one of the less clearly defined aspects of the
    Kafka protocol, so we run our test procedure against Apache Kafka
    to establish a baseline behavior to ensure our compatibility.
    """

    log_segment_size = 1024 * 1024

    # apply retention more frequently. as of writing, the default is 30 seconds
    # and then every 5 minutes. this is problematic if a test expects more
    # frequent operation, but misses that initial round at 30 seconds.
    log_retention_check_interval_ms = 30 * 1000

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.zk = ZookeeperService(self.test_context, num_nodes=1, version=V_3_0_0)

        server_prop_overrides = [
            ["log.retention.check.interval.ms", self.log_retention_check_interval_ms]
        ]

        self.kafka = KafkaServiceAdapter(
            self.test_context,
            KafkaService(
                self.test_context,
                num_nodes=3,
                zk=self.zk,
                server_prop_overrides=server_prop_overrides,
                version=V_3_0_0,
            ),
        )

        self._client = DefaultClient(self.kafka)

    def client(self):
        return self._client

    def setUp(self):
        self.zk.start()
        self.kafka.start()

    def tearDown(self):
        # ducktape handle service teardown automatically, but it is hard
        # to tell what went wrong if one of the services hangs.  Do it
        # explicitly here with some logging, to enable debugging issues
        # like https://github.com/redpanda-data/redpanda/issues/4270

        self.logger.info("Stopping Kafka...")
        self.kafka.stop()

        self.logger.info("Stopping zookeeper...")
        self.zk.stop()

    @ducktape_cluster(num_nodes=5)
    def test_timequery(self):
        self._test_timequery(cluster=self.kafka, batch_cache=True)

    @ducktape_cluster(num_nodes=5)
    def test_timequery_below_start_offset(self):
        self._test_timequery_below_start_offset(cluster=self.kafka)
