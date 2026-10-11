# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import random
import string
import subprocess

from ducktape.mark import parametrize

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.kcl import RawKCL
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest


class AlterTopicConfiguration(RedpandaTest):
    """
    Change a partition's replica set.
    """

    topics = (TopicSpec(partition_count=1, replication_factor=3),)

    def __init__(self, test_context):
        super(AlterTopicConfiguration, self).__init__(
            test_context=test_context, num_brokers=3
        )

        self.kafka_tools = KafkaCliTools(self.redpanda)

    @cluster(num_nodes=3)
    @parametrize(property=TopicSpec.PROPERTY_CLEANUP_POLICY, value="compact")
    @parametrize(property=TopicSpec.PROPERTY_SEGMENT_SIZE, value=10 * (2 << 20))
    @parametrize(property=TopicSpec.PROPERTY_RETENTION_BYTES, value=200 * (2 << 20))
    @parametrize(property=TopicSpec.PROPERTY_RETENTION_TIME, value=360000)
    @parametrize(property=TopicSpec.PROPERTY_TIMESTAMP_TYPE, value="LogAppendTime")
    @parametrize(property=TopicSpec.PROPERTY_DELETE_RETENTION_MS, value=123456789)
    def test_altering_topic_configuration(self, property, value):
        topic = self.topics[0].name
        self.client().alter_topic_configs(topic, {property: value})
        kafka_tools = KafkaCliTools(self.redpanda)
        spec = kafka_tools.describe_topic(topic)

        # e.g. retention.ms is TopicSpec.retention_ms
        attr_name = property.replace(".", "_")
        assert getattr(spec, attr_name, None) == value

    @cluster(num_nodes=3)
    def test_alter_config_does_not_change_replication_factor(self):
        topic = self.topics[0].name
        # change default replication factor
        self.redpanda.set_cluster_config({"default_topic_replications": str(5)})
        kcl = RawKCL(self.redpanda)
        kcl.raw_alter_topic_config(
            1,
            topic,
            {
                TopicSpec.PROPERTY_RETENTION_TIME: 360000,
                TopicSpec.PROPERTY_TIMESTAMP_TYPE: "LogAppendTime",
                TopicSpec.PROPERTY_DELETE_RETENTION_MS: 1234567890,
            },
        )
        kafka_tools = KafkaCliTools(self.redpanda)
        spec = kafka_tools.describe_topic(topic)

        assert spec.replication_factor == 3
        assert spec.retention_ms == 360000
        assert spec.message_timestamp_type == "LogAppendTime"
        assert spec.delete_retention_ms == 1234567890

    @cluster(num_nodes=3)
    def test_altering_multiple_topic_configurations(self):
        topic = self.topics[0].name
        kafka_tools = KafkaCliTools(self.redpanda)
        self.client().alter_topic_configs(
            topic,
            {
                TopicSpec.PROPERTY_SEGMENT_SIZE: 1024 * 1024,
                TopicSpec.PROPERTY_RETENTION_TIME: 360000,
                TopicSpec.PROPERTY_TIMESTAMP_TYPE: "LogAppendTime",
                TopicSpec.PROPERTY_DELETE_RETENTION_MS: 1234567890,
            },
        )
        spec = kafka_tools.describe_topic(topic)

        assert spec.segment_bytes == 1024 * 1024
        assert spec.retention_ms == 360000
        assert spec.message_timestamp_type == "LogAppendTime"
        assert spec.delete_retention_ms == 1234567890

    def random_string(self, size):
        return "".join(
            random.choice(string.ascii_uppercase + string.digits) for _ in range(size)
        )

    @cluster(num_nodes=3)
    def test_set_config_from_describe(self):
        topic = self.topics[0].name
        kafka_tools = KafkaCliTools(self.redpanda)
        topic_config = kafka_tools.describe_topic_config(topic=topic)
        self.client().alter_topic_configs(topic, topic_config)

    @cluster(num_nodes=3)
    def test_configuration_properties_kafka_config_allowlist(self):
        rpk = RpkTool(self.redpanda)
        config = rpk.describe_topic_configs(self.topic)

        new_segment_bytes = int(config["segment.bytes"][0]) + 1
        self.client().alter_topic_configs(
            self.topic,
            {
                "unclean.leader.election.enable": True,
                TopicSpec.PROPERTY_SEGMENT_SIZE: new_segment_bytes,
            },
        )

        new_config = rpk.describe_topic_configs(self.topic)
        assert int(new_config["segment.bytes"][0]) == new_segment_bytes

    @cluster(num_nodes=3)
    def test_configuration_properties_name_validation(self):
        topic = self.topics[0].name
        kafka_tools = KafkaCliTools(self.redpanda)
        spec = kafka_tools.describe_topic(topic)
        for _ in range(0, 5):
            key = self.random_string(5)
            try:
                self.client().alter_topic_configs(topic, {key: "123"})
            except Exception as inst:
                self.logger.info(
                    "alter failed as expected: expected exception %s", inst
                )
            else:
                raise RuntimeError("Alter should have failed but succeeded!")

        new_spec = kafka_tools.describe_topic(topic)
        # topic spec shouldn't change
        assert new_spec == spec

    @cluster(num_nodes=3)
    def test_segment_size_validation(self):
        topic = self.topics[0].name
        kafka_tools = KafkaCliTools(self.redpanda)
        initial_spec = kafka_tools.describe_topic(topic)
        self.redpanda.set_cluster_config({"log_segment_size_min": 1024})
        try:
            self.client().alter_topic_configs(
                topic, {TopicSpec.PROPERTY_SEGMENT_SIZE: 16}
            )
        except subprocess.CalledProcessError as e:
            assert "is outside of allowed range" in e.output

        assert (
            initial_spec.segment_bytes
            == kafka_tools.describe_topic(topic).segment_bytes
        ), "segment.bytes shouldn't be changed to invalid value"

        # change min segment bytes redpanda property
        self.redpanda.set_cluster_config({"log_segment_size_min": 1024 * 1024})
        # try setting value that is smaller than requested min
        try:
            self.client().alter_topic_configs(
                topic, {TopicSpec.PROPERTY_SEGMENT_SIZE: 1024 * 1024 - 1}
            )
        except subprocess.CalledProcessError as e:
            assert "is outside of allowed range" in e.output

        assert (
            initial_spec.segment_bytes
            == kafka_tools.describe_topic(topic).segment_bytes
        ), "segment.bytes shouldn't be changed to invalid value"
        valid_segment_size = 1024 * 1024 + 1
        self.client().alter_topic_configs(
            topic, {TopicSpec.PROPERTY_SEGMENT_SIZE: valid_segment_size}
        )
        assert kafka_tools.describe_topic(topic).segment_bytes == valid_segment_size

        self.redpanda.set_cluster_config({"log_segment_size_max": 10 * 1024 * 1024})
        # try to set value greater than max allowed segment size
        try:
            self.client().alter_topic_configs(
                topic, {TopicSpec.PROPERTY_SEGMENT_SIZE: 20 * 1024 * 1024}
            )
        except subprocess.CalledProcessError as e:
            assert "is outside of allowed range" in e.output

        # check that segment size didn't change
        assert kafka_tools.describe_topic(topic).segment_bytes == valid_segment_size

    @cluster(num_nodes=3)
    def test_min_cleanable_dirty_ratio_validation(self):
        topic = self.topics[0].name
        kafka_tools = KafkaCliTools(self.redpanda)
        self.redpanda.set_cluster_config({"min_cleanable_dirty_ratio": 0.5})
        initial_spec = kafka_tools.describe_topic(topic)

        # Check that a value outside valid range is rejected
        try:
            self.client().alter_topic_configs(
                topic, {TopicSpec.PROPERTY_MIN_CLEANABLE_DIRTY_RATIO: 1.01}
            )
        except subprocess.CalledProcessError as e:
            assert "is outside of allowed range" in e.output

        assert (
            initial_spec.min_cleanable_dirty_ratio
            == kafka_tools.describe_topic(topic).min_cleanable_dirty_ratio
        ), "min.cleanable.dirty.ratio shouldn't be changed to invalid value"

        # Check that a valid value is accepted
        self.client().alter_topic_configs(
            topic, {TopicSpec.PROPERTY_MIN_CLEANABLE_DIRTY_RATIO: 0.3}
        )

        assert kafka_tools.describe_topic(topic).min_cleanable_dirty_ratio == 0.3

        # Check that we can disable the tristate with -1
        self.client().alter_topic_configs(
            topic, {TopicSpec.PROPERTY_MIN_CLEANABLE_DIRTY_RATIO: -1}
        )

        assert kafka_tools.describe_topic(topic).min_cleanable_dirty_ratio == -1

        # Check that we can disable the tristate with any value < 0
        self.client().alter_topic_configs(
            topic, {TopicSpec.PROPERTY_MIN_CLEANABLE_DIRTY_RATIO: -123.456}
        )

        assert kafka_tools.describe_topic(topic).min_cleanable_dirty_ratio == -1

        # Check that deleting the topic config resets it back to cluster default
        self.client().delete_topic_config(
            topic, TopicSpec.PROPERTY_MIN_CLEANABLE_DIRTY_RATIO
        )

        assert (
            kafka_tools.describe_topic(topic).min_cleanable_dirty_ratio
            == initial_spec.min_cleanable_dirty_ratio
        )

    @cluster(num_nodes=3)
    def test_batch_max_bytes_validation(self):
        self.redpanda.set_cluster_config(
            {"kafka_max_message_size_upper_limit_bytes": 10}
        )

        topic = self.topics[0].name
        kafka_tools = KafkaCliTools(self.redpanda)
        initial_spec = kafka_tools.describe_topic(topic)

        try:
            self.client().alter_topic_configs(
                topic, {TopicSpec.PROPERTY_MAX_MESSAGE_BYTES: 100}
            )
        except subprocess.CalledProcessError as e:
            assert "is outside of the allowed range" in e.output

        try:
            self.client().alter_topic_configs(
                topic, {TopicSpec.PROPERTY_MAX_MESSAGE_BYTES: 0}
            )
        except subprocess.CalledProcessError as e:
            assert "is outside of the allowed range" in e.output

        not_updated = kafka_tools.describe_topic(topic)
        assert initial_spec.max_message_bytes == not_updated.max_message_bytes, (
            "max.message.bytes should not have changed"
        )

        self.client().alter_topic_configs(
            topic, {TopicSpec.PROPERTY_MAX_MESSAGE_BYTES: 1}
        )
        updated = kafka_tools.describe_topic(topic)
        assert updated.max_message_bytes and int(updated.max_message_bytes) == 1, (
            "max.message.bytes should have changed"
        )

    @cluster(num_nodes=3)
    def test_min_and_max_compaction_lag_ms_validation(self):
        topic = self.topics[0].name
        kafka_tools = KafkaCliTools(self.redpanda)
        self.redpanda.set_cluster_config(
            {"min_compaction_lag_ms": 10000, "max_compaction_lag_ms": 20000}
        )
        initial_spec = kafka_tools.describe_topic(topic)

        # Check that values outsides the valid ranges are rejected and
        # don't change the configured value.
        try:
            self.client().alter_topic_configs(
                topic, {TopicSpec.PROPERTY_MIN_COMPACTION_LAG_MS: -1}
            )
        except subprocess.CalledProcessError as e:
            assert "invalid, expected to be in range" in e.output

        try:
            self.client().alter_topic_configs(
                topic, {TopicSpec.PROPERTY_MAX_COMPACTION_LAG_MS: 0}
            )
        except subprocess.CalledProcessError as e:
            assert "invalid, expected to be in range" in e.output

        not_updated = kafka_tools.describe_topic(topic)
        assert (
            initial_spec.min_compaction_lag_ms == not_updated.min_compaction_lag_ms
        ), "min.compaction.lag.ms should not have changed"
        assert (
            initial_spec.max_compaction_lag_ms == not_updated.max_compaction_lag_ms
        ), "max.compaction.lag.ms should not have changed"

        # Check that values in the accepted ranges are accepted, and
        # that the properties can be unset and revert to the cluster default.
        self.client().alter_topic_configs(
            topic,
            {
                TopicSpec.PROPERTY_MIN_COMPACTION_LAG_MS: 5000,
                TopicSpec.PROPERTY_MAX_COMPACTION_LAG_MS: 25000,
            },
        )

        updated = kafka_tools.describe_topic(topic)
        assert updated.min_compaction_lag_ms == "5000", (
            "min.compaction.lag.ms should have changed"
        )
        assert updated.max_compaction_lag_ms == "25000", (
            "max.compaction.lag.ms should have changed"
        )

        self.client().delete_topic_config(
            topic, TopicSpec.PROPERTY_MIN_COMPACTION_LAG_MS
        )
        self.client().delete_topic_config(
            topic, TopicSpec.PROPERTY_MAX_COMPACTION_LAG_MS
        )

        cleared = kafka_tools.describe_topic(topic)
        assert initial_spec.min_compaction_lag_ms == cleared.min_compaction_lag_ms, (
            "min.compaction.lag.ms should have reverted to default"
        )
        assert initial_spec.max_compaction_lag_ms == cleared.max_compaction_lag_ms, (
            "max.compaction.lag.ms should have reverted to default"
        )
