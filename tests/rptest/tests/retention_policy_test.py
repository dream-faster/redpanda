# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
from time import sleep

from ducktape.mark import matrix

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda_installer import InstallOptions, RedpandaVersionLine
from rptest.tests.end_to_end import EndToEndTest
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import (
    expect_timeout,
    produce_total_bytes,
    produce_until_segments,
    segments_count,
    wait_for_local_storage_truncate,
    wait_until,
)


def bytes_for_segments(want_segments, segment_size):
    """
    Work out what to set retention.bytes to in order to retain
    just this number of segments (assuming all segments are written
    to their size limit).
    """
    return int(want_segments * segment_size)


class RetentionPolicyTest(RedpandaTest):
    topics = (
        TopicSpec(
            partition_count=1,
            replication_factor=3,
            cleanup_policy=TopicSpec.CLEANUP_DELETE,
        ),
    )

    def __init__(self, test_context):
        extra_rp_conf = dict(
            log_compaction_interval_ms=5000,
            log_segment_size=1048576,
        )

        super(RetentionPolicyTest, self).__init__(
            test_context=test_context, num_brokers=3, extra_rp_conf=extra_rp_conf
        )

    @cluster(num_nodes=3)
    @matrix(
        property=[
            TopicSpec.PROPERTY_RETENTION_TIME,
            TopicSpec.PROPERTY_RETENTION_BYTES,
        ],
        acks=[1, -1],
    )
    def test_changing_topic_retention(self, property, acks):
        """
        Test changing topic retention duration for topics with data produced
        with ACKS=1 and ACKS=-1. This test produces data until 10 segments
        appear, then it changes retention topic property and waits for
        segments to be removed
        """
        # produce until segments have been compacted
        produce_until_segments(
            self.redpanda,
            topic=self.topic,
            partition_idx=0,
            count=10,
            acks=acks,
        )

        # change retention time
        if property == TopicSpec.PROPERTY_RETENTION_BYTES:
            local_retention = 10000
            self.client().alter_topic_configs(
                self.topic,
                {
                    property: local_retention,
                },
            )
            wait_for_local_storage_truncate(
                redpanda=self.redpanda, topic=self.topic, target_bytes=local_retention
            )
        else:
            # Set a tiny time retention, and wait for some local segments
            # to be removed.
            self.client().alter_topic_configs(
                self.topic,
                {
                    property: 10000,
                },
            )
            wait_until(
                lambda: next(segments_count(self.redpanda, self.topic, 0)) <= 5,
                timeout_sec=120,
            )

    @cluster(num_nodes=3)
    def test_changing_topic_retention_with_restart(self):
        """
        Test changing topic retention duration for topics with data produced
        with ACKS=1 and ACKS=-1. This test produces data until 10 segments
        appear, then it changes retention topic property and waits for some
        segmetnts to be removed
        """
        segment_size = 1048576

        # produce until segments have been compacted
        produce_until_segments(
            self.redpanda,
            topic=self.topic,
            partition_idx=0,
            count=20,
            acks=-1,
        )

        # restart all nodes to force replicating raft configuration
        self.redpanda.restart_nodes(self.redpanda.nodes)

        kafka_tools = KafkaCliTools(self.redpanda)
        # Wait for controller, alter configs doesn't have a retry loop
        kafka_tools.describe_topic(self.topic)

        # Retain progressively less data, and validate that retention policy is applied
        for retain_segments in (15, 10, 5):
            local_retention = bytes_for_segments(retain_segments, segment_size)
            self.client().alter_topic_configs(
                self.topic, {TopicSpec.PROPERTY_RETENTION_BYTES: local_retention}
            )
            wait_for_local_storage_truncate(
                self.redpanda, self.topic, target_bytes=local_retention
            )


class RetentionPolicyToggleTest(RedpandaTest):
    topics = (
        TopicSpec(
            partition_count=1,
            replication_factor=3,
            cleanup_policy=TopicSpec.CLEANUP_COMPACT,
        ),
    )

    log_compaction_interval_ms = 1000

    def __init__(self, test_context):
        self.extra_rp_conf = dict(
            log_compaction_interval_ms=self.log_compaction_interval_ms,
            log_segment_size=1048576,
            compacted_log_segment_size=1048576,
        )

        super().__init__(
            test_context=test_context, num_brokers=3, extra_rp_conf=self.extra_rp_conf
        )

    @cluster(num_nodes=3)
    def test_changing_topic_cleanup_policy(self):
        """
        Test changing topic cleanup policy and retention property,
        then waits for segments to be removed.
        """

        self.client().alter_topic_configs(
            self.topic,
            {
                TopicSpec.PROPERTY_CLEANUP_POLICY: TopicSpec.CLEANUP_DELETE,
            },
        )

        produce_until_segments(
            self.redpanda,
            topic=self.topic,
            partition_idx=0,
            count=10,
        )

        local_retention = 10000
        self.client().alter_topic_configs(
            self.topic,
            {
                TopicSpec.PROPERTY_RETENTION_BYTES: local_retention,
            },
        )

        wait_for_local_storage_truncate(
            redpanda=self.redpanda, topic=self.topic, target_bytes=local_retention
        )

        self.logger.debug("Toggling back to cleanup.policy=compact")
        self.client().alter_topic_configs(
            self.topic,
            {
                TopicSpec.PROPERTY_CLEANUP_POLICY: TopicSpec.CLEANUP_COMPACT,
            },
        )

        rpk = RpkTool(self.redpanda)
        start_offset_before = next(rpk.describe_topic(self.topic)).start_offset
        produce_total_bytes(self.redpanda, self.topic, 10 * 1024 * 1024)
        time.sleep(5 * self.log_compaction_interval_ms / 1000)
        start_offset_after = next(rpk.describe_topic(self.topic)).start_offset
        assert start_offset_before == start_offset_after, (
            f"start offsets shouldn't move if cleanup.policy=compact; before: {start_offset_before}, after: {start_offset_after}"
        )


class TimestampDriftRetentionTest(RedpandaTest):
    """
    Tests that timestamps of records that have been allowed into `redpanda` are respected,
    without any overrides or historical hacks applied. Previously we would fall back
    to broker time or an adjusted mtime, both of which are deprecated behaviors now that
    we have proper timestamp validation in the produce path as of v25.3.1
    (`message.timestamp.{before/after}.max.ms`, as well as ensuring `max_timestamp` is set
    in `produce_validation.cc`)
    """

    segment_size = 1048576
    topics = (
        TopicSpec(
            partition_count=1,
            replication_factor=1,
            cleanup_policy=TopicSpec.CLEANUP_DELETE,
            # 1 megabyte segments
            segment_bytes=segment_size,
            # 10 second retention: in the case of mixed timestamps,
            # give active segment time to help adjust retention timestamps and show up in log monitor.
            retention_ms=10000,
        ),
    )

    def __init__(self, *args, **kwargs):
        super().__init__(
            *args, extra_rp_conf={"log_compaction_interval_ms": 1000}, **kwargs
        )

    @cluster(num_nodes=2)
    def test_future_timestamps(self):
        """
        Ensure record timestamps in the future are respected by retention enforcement.
        """

        # Allow for messages up to a week in the future to be produced.
        self.client().alter_topic_config(
            self.topic, "message.timestamp.after.max.ms", 7 * 24 * 3600 * 1000
        )

        # A fictional artificial timestamp base in milliseconds
        future_timestamp = (int(time.time()) + 24 * 3600) * 1000

        # Produce a run of messages with CreateTime-style timestamps, each
        # record having a timestamp 1ms greater than the last.
        msg_size = 14000
        segments_count = 10
        msg_count = (self.segment_size // msg_size) * segments_count

        # Write msg_count messages with timestamps in the future
        producer = KgoVerifierProducer(
            context=self.test_context,
            redpanda=self.redpanda,
            topic=self.topic,
            msg_size=msg_size,
            msg_count=msg_count,
            fake_timestamp_ms=future_timestamp,
            batch_max_bytes=msg_size * 2,
        )
        producer.start()
        producer.wait()

        def prefix_truncated():
            segs = self.redpanda.node_storage(self.redpanda.nodes[0]).segments(
                "kafka", self.topic, 0
            )
            self.logger.debug(f"Segments: {segs}")
            return len(segs) <= 1

        # Don't expect to see any prefix truncation.
        with expect_timeout():
            self.redpanda.wait_until(prefix_truncated, timeout_sec=5, backoff_sec=1)

    @cluster(num_nodes=2)
    def test_past_timestamps(self):
        """
        Ensure record timestamps in the past are respected by retention enforcement.
        """

        # Set `retention.ms` to 25 hours
        retention_ms = 25 * 3600 * 1000
        self.client().alter_topic_config(self.topic, "retention.ms", retention_ms)

        # A fictional artificial timestamp base in milliseconds (one day previous)
        past_timestamp = (int(time.time()) - 24 * 3600) * 1000

        # Produce a run of messages with CreateTime-style timestamps, each
        # record having a timestamp 1ms greater than the last.
        msg_size = 14000
        segments_count = 10

        # Write msg_count messages with timestamps in the past
        producer = KgoVerifierProducer(
            context=self.test_context,
            redpanda=self.redpanda,
            topic=self.topic,
            msg_size=msg_size,
            msg_count=(self.segment_size // msg_size) * segments_count,
            fake_timestamp_ms=past_timestamp,
            batch_max_bytes=msg_size * 2,
        )
        producer.start()
        producer.wait()

        def prefix_truncated():
            segs = self.redpanda.node_storage(self.redpanda.nodes[0]).segments(
                "kafka", self.topic, 0
            )
            self.logger.debug(f"Segments: {segs}")
            return len(segs) <= 1

        # Don't expect to see prefix truncation of day old records with `retention.ms=25h`.
        with expect_timeout():
            self.redpanda.wait_until(prefix_truncated, timeout_sec=5, backoff_sec=1)

        # Set `retention.ms` to 23 hours
        retention_ms = 23 * 3600 * 1000
        self.client().alter_topic_config(self.topic, "retention.ms", retention_ms)

        # Expect to see prefix truncation of day old records with `retention.ms=23h`.
        self.redpanda.wait_until(prefix_truncated, timeout_sec=30, backoff_sec=1)


class BogusTimestampTest(EndToEndTest):
    def __init__(self, test_context):
        self.test_context = test_context
        super().__init__(
            test_context=test_context,
            extra_rp_conf={"log_compaction_interval_ms": 1000},
        )
        self.segment_size = 1048576
        self.topic_spec = TopicSpec(
            partition_count=1,
            replication_factor=1,
            cleanup_policy=TopicSpec.CLEANUP_DELETE,
            # 1 megabyte segments
            segment_bytes=self.segment_size,
            # 10 second retention: in the case of mixed timestamps,
            # give active segment time to help adjust retention timestamps and show up in log monitor.
            retention_ms=10000,
        )
        self.topic = self.topic_spec.name

    @cluster(num_nodes=2)
    @matrix(mixed_timestamps=[True, False], use_broker_timestamps=[True, False])
    def test_bogus_timestamps(
        self, mixed_timestamps: bool, use_broker_timestamps: bool
    ):
        """
        :param mixed_timestamps: if true, test with a mixture of valid and invalid
        timestamps in the same segment (i.e. timestamp adjustment should use the
        valid timestamps rather than falling back to mtime).

        This is a legacy test which depends on the fact that `validated_batch_timestamps`
        is a feature flag with `available_policy::new_clusters_only` in `v25.3.x`.
        For that reason, we start with a cluster of version `v25.2.x`, immediately
        upgrade it, and expect to see the legacy behavior.
        """

        pre_validated_batch_timestamps_version = RedpandaVersionLine((25, 2))

        self.start_redpanda(
            num_nodes=1,
            install_opts=InstallOptions(version=pre_validated_batch_timestamps_version),
        )

        self.client().create_topic(self.topic_spec)

        # Install the latest version of `redpanda` and restart nodes
        for version in self.redpanda._installer.upgrade_path_to_head(
            pre_validated_batch_timestamps_version
        ):
            self.redpanda._installer.install(self.redpanda.nodes, version)
            self.redpanda.stop_node(self.redpanda.nodes[0])
            self.redpanda.start_node(self.redpanda.nodes[0])

        # broker_time_based_retention fixes this test case for new segments. (disable it to simulate a legacy condition)
        self.redpanda.set_feature_active(
            "broker_time_based_retention", use_broker_timestamps, timeout_sec=10
        )

        self.client().alter_topic_config(
            self.topic, "segment.bytes", str(self.segment_size)
        )

        # Allow for messages up to a week in the future to be produced.
        self.client().alter_topic_config(
            self.topic, "message.timestamp.after.max.ms", 7 * 24 * 3600 * 1000
        )

        # A fictional artificial timestamp base in milliseconds
        future_timestamp = (int(time.time()) + 24 * 3600) * 1000

        # Produce a run of messages with CreateTime-style timestamps, each
        # record having a timestamp 1ms greater than the last.
        msg_size = 14000
        segments_count = 10
        msg_count = (self.segment_size // msg_size) * segments_count

        if mixed_timestamps:
            valid_records = (self.segment_size // msg_size) // 2

            # Write enough valid-timestamped records that one should appear in the index
            producer = KgoVerifierProducer(
                context=self.test_context,
                redpanda=self.redpanda,
                topic=self.topic,
                msg_size=msg_size,
                msg_count=valid_records,
                batch_max_bytes=msg_size * 2,
            )
            producer.start()
            producer.wait()
            producer.free()

            # Write the rest of the messages with invalid timestamps
            producer = KgoVerifierProducer(
                context=self.test_context,
                redpanda=self.redpanda,
                topic=self.topic,
                msg_size=msg_size,
                msg_count=msg_count - valid_records,
                fake_timestamp_ms=future_timestamp,
                batch_max_bytes=msg_size * 2,
            )
            producer.start()
            producer.wait()
        else:
            # Write msg_count messages with timestamps in the future
            producer = KgoVerifierProducer(
                context=self.test_context,
                redpanda=self.redpanda,
                topic=self.topic,
                msg_size=msg_size,
                msg_count=(self.segment_size // msg_size) * segments_count,
                fake_timestamp_ms=future_timestamp,
                batch_max_bytes=msg_size * 2,
            )
            producer.start()
            producer.wait()

        if not use_broker_timestamps:
            # We should have written the expected number of segments, and nothing can
            # have been gc'd yet because all the segments have max timestmap in the future
            segs = self.redpanda.node_storage(self.redpanda.nodes[0]).segments(
                "kafka", self.topic, 0
            )
            self.logger.debug(f"Segments after write: {segs}")
            assert len(segs) >= segments_count

            # Give retention code some time to kick in: we are expecting that it does not
            # remove anything because the timestamps are too far in the future.
            with self.redpanda.monitor_log(self.redpanda.nodes[0]) as mon:
                # Time period much larger than what we set log_compaction_interval_ms to
                sleep(10)

                # Even without setting the storage_ignore_timestamps_in_future_sec parameter,
                # we should get warnings about the timestamps.
                mon.wait_until(
                    "found segment with bogus retention timestamp",
                    timeout_sec=30,
                    backoff_sec=1,
                )

            # The GC should not have deleted anything
            segs = self.redpanda.node_storage(self.redpanda.nodes[0]).segments(
                "kafka", self.topic, 0
            )
            self.logger.debug(f"Segments after first GC: {segs}")
            assert len(segs) >= segments_count

            # Enable the escape hatch to tell Redpanda to correct timestamps that are
            # too far in the future
            with self.redpanda.monitor_log(self.redpanda.nodes[0]) as mon:
                self.redpanda.set_cluster_config(
                    {
                        "storage_ignore_timestamps_in_future_sec": 60,
                    }
                )

                if mixed_timestamps:
                    mon.wait_until(
                        "Adjusting retention timestamp.*to max valid record",
                        timeout_sec=30,
                        backoff_sec=1,
                    )
                else:
                    mon.wait_until(
                        "Adjusting retention timestamp.*to file mtime",
                        timeout_sec=30,
                        backoff_sec=1,
                    )

        # either with broker_time_based_retention active or storage_ignore_timestamps_in_future_sec enable, we are now able to apply retention

        def prefix_truncated():
            segs = self.redpanda.node_storage(self.redpanda.nodes[0]).segments(
                "kafka", self.topic, 0
            )
            self.logger.debug(f"Segments: {segs}")
            return len(segs) <= 1

        # Segments should be cleaned up now that we've switched on force-correction
        # of timestamps in the future
        self.redpanda.wait_until(prefix_truncated, timeout_sec=30, backoff_sec=1)
