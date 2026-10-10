# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json

from ducktape.mark import parametrize
from ducktape.utils.util import wait_until
from requests.exceptions import HTTPError

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.offline_log_viewer import OfflineLogViewer
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.metrics_check import MetricCheck
from rptest.services.rpk_producer import RpkProducer
from rptest.tests.partition_movement import PartitionMovementMixin
from rptest.tests.redpanda_test import RedpandaTest


def get_kvstore_topic_key_counts(redpanda):
    """
    Count the keys in KVStore that relate to Kafka topics: this excludes all
    internal topic items: if no Kafka topics exist, this should be zero for
    all nodes.

    :returns: dict of Node to integer
    """

    viewer = OfflineLogViewer(redpanda)

    # Find the raft group IDs of internal topics
    admin = Admin(redpanda)
    internal_group_ids = set()
    for ntp in [
        ("redpanda", "controller", 0),
        ("kafka_internal", "id_allocator", 0),
    ]:
        namespace, topic, partition = ntp
        try:
            p = admin.get_partitions(
                topic=topic, namespace=namespace, partition=partition
            )
        except HTTPError as e:
            # OK if internal topic doesn't exist (e.g. id_allocator
            # doesn't have to exist)
            if e.response.status_code != 404:
                raise
        else:
            internal_group_ids.add(p["raft_group_id"])

    # `kafka_internal/ct_l1_domain` is auto-created by the cloud_topics
    # subsystem whenever cloud storage is enabled, with N partitions. Pull
    # in every partition's raft group so its shard_placement kvstore keys
    # are filtered out as internal.
    try:
        for p in admin.get_partitions(topic="ct_l1_domain", namespace="kafka_internal"):
            internal_group_ids.add(p["raft_group_id"])
    except HTTPError as e:
        if e.response.status_code != 404:
            raise

    result = {}
    for n in redpanda.nodes:
        kvstore_data = viewer.read_kvstore(node=n)

        excess_keys = []
        for shard, items in kvstore_data.items():
            keys = [i["key"] for i in items]

            for k in keys:
                if (
                    k["keyspace"] == "cluster"
                    or k["keyspace"] == "usage"
                    or (
                        k["keyspace"] == "shard_placement"
                        and k["data"]["type"] in (0, 3)
                    )
                ):
                    # Not a per-partition key
                    continue

                if k["data"].get("group", None) in internal_group_ids:
                    # One of the internal topics
                    continue

                if k["data"].get("ntp", {}).get("topic", None) == "controller":
                    # Controller storage item
                    continue
                if k["data"].get("ntp", {}).get("namespace", None) == "kafka_internal":
                    # Internal topic storage item
                    continue
                excess_keys.append(k)

            redpanda.logger.info(
                f"{n.name}.{shard} Excess Keys {json.dumps(excess_keys, indent=2)}"
            )

        key_count = len(excess_keys)
        result[n] = key_count

    return result


def topic_storage_purged(redpanda, topic_name):
    storage = redpanda.storage()
    logs_removed = all(
        map(lambda n: topic_name not in n.ns["kafka"].topics, storage.nodes)
    )

    if not logs_removed:
        redpanda.logger.info(f"Files remain for topic {topic_name}:")
        for n in storage.nodes:
            ns = n.ns["kafka"]
            for topic_name, topic in ns.topics.items():
                for p_id, p in topic.partitions.items():
                    for f in p.files:
                        redpanda.logger.info(f"  {n.name}: {topic_name}_{p_id}_{f}")

        return False

    # Once logs are removed, also do more expensive inspection of
    # kvstore to check that per-partition kvstore contents are
    # gone.  The user doesn't care about this, but it is important
    # to avoid bugs that would cause kvstore to bloat through
    # topic creation/destruction cycles.

    topic_key_counts = get_kvstore_topic_key_counts(redpanda)
    if any([v > 0 for v in topic_key_counts.values()]):
        redpanda.logger.info("Topic keys remain in KVStore")
        for node, count in topic_key_counts.items():
            redpanda.logger.info(f"  {node}: {count}")
        return False

    return True


class TopicDeleteTest(RedpandaTest):
    """
    Verify that topic deletion cleans up storage.
    """

    topics = (TopicSpec(partition_count=3, cleanup_policy=TopicSpec.CLEANUP_COMPACT),)

    def __init__(self, test_context):
        extra_rp_conf = dict(
            log_segment_size=262144,
        )

        super(TopicDeleteTest, self).__init__(
            test_context=test_context, num_brokers=3, extra_rp_conf=extra_rp_conf
        )

        self.kafka_tools = KafkaCliTools(self.redpanda)

    def produce_until_partitions(self):
        self.kafka_tools.produce(self.topic, 1024, 1024)
        storage = self.redpanda.storage()
        return len(list(storage.partitions("kafka", self.topic))) == 9

    def dump_storage_listing(self):
        for node in self.redpanda.nodes:
            self.logger.error(f"Storage listing on {node.name}:")
            for line in node.account.ssh_capture(f"find {self.redpanda.DATA_DIR}"):
                self.logger.error(line.strip())

    @cluster(num_nodes=3)
    @parametrize(with_restart=False)
    @parametrize(with_restart=True)
    def topic_delete_test(self, with_restart):
        wait_until(
            lambda: self.produce_until_partitions(),
            timeout_sec=30,
            backoff_sec=2,
            err_msg="Expected partition did not materialize",
        )

        if with_restart:
            # Do a restart to encourage writes and flushes, especially to
            # the kvstore.
            self.redpanda.restart_nodes(self.redpanda.nodes)

        # Sanity check the kvstore checks: there should be at least one kvstore entry
        # per partition while the topic exists.
        assert (
            sum(get_kvstore_topic_key_counts(self.redpanda).values())
            >= self.topics[0].partition_count
        )

        self.kafka_tools.delete_topic(self.topic)

        try:
            wait_until(
                lambda: topic_storage_purged(self.redpanda, self.topic),
                timeout_sec=30,
                backoff_sec=2,
                err_msg="Topic storage was not removed",
            )

        except:
            self.dump_storage_listing()
            raise

    @cluster(num_nodes=3, log_allow_list=[r"filesystem error: remove failed"])
    def topic_delete_orphan_files_test(self):
        wait_until(
            lambda: self.produce_until_partitions(),
            timeout_sec=30,
            backoff_sec=2,
            err_msg="Expected partition did not materialize",
        )

        # Sanity check the kvstore checks: there should be at least one kvstore entry
        # per partition while the topic exists.
        assert (
            sum(get_kvstore_topic_key_counts(self.redpanda).values())
            >= self.topics[0].partition_count
        )

        down_node = self.redpanda.nodes[-1]
        try:
            # Make topic directory immutable to prevent deleting
            down_node.account.ssh(
                f"chattr +i {self.redpanda.DATA_DIR}/kafka/{self.topic}"
            )

            self.kafka_tools.delete_topic(self.topic)

            def topic_deleted_on_all_nodes_except_one(redpanda, down_node, topic_name):
                storage = redpanda.storage()
                log_not_removed_on_down = (
                    topic_name
                    in next(filter(lambda x: x.name == down_node.name, storage.nodes))
                    .ns["kafka"]
                    .topics
                )
                logs_removed_on_others = all(
                    map(
                        lambda n: topic_name not in n.ns["kafka"].topics,
                        filter(lambda x: x.name != down_node.name, storage.nodes),
                    )
                )
                return log_not_removed_on_down and logs_removed_on_others

            try:
                wait_until(
                    lambda: topic_deleted_on_all_nodes_except_one(
                        self.redpanda, down_node, self.topic
                    ),
                    timeout_sec=30,
                    backoff_sec=2,
                    err_msg="Topic storage was not removed from running nodes or removed from down node",
                )
            except:
                self.dump_storage_listing()
                raise

            self.redpanda.stop_node(down_node)
        finally:
            down_node.account.ssh(
                f"chattr -i {self.redpanda.DATA_DIR}/kafka/{self.topic}"
            )

        self.redpanda.start_node(down_node)

        try:
            wait_until(
                lambda: topic_storage_purged(self.redpanda, self.topic),
                timeout_sec=10,
                backoff_sec=2,
                err_msg="Topic storage was not removed",
            )
        except:
            self.dump_storage_listing()
            raise


class TopicDeleteAfterMovementTest(RedpandaTest):
    """
    Verify that topic deleted after partition movement.
    """

    partition_count = 3
    topics = (TopicSpec(partition_count=partition_count),)

    def __init__(self, test_context):
        rp_conf = {"partition_autobalancing_mode": "off"}
        super(TopicDeleteAfterMovementTest, self).__init__(
            test_context=test_context, num_brokers=4, extra_rp_conf=rp_conf
        )

        self.kafka_tools = KafkaCliTools(self.redpanda)

    def movement_done(self, partition, to_nodes):
        assignments = [dict(node_id=n) for n in to_nodes]
        results = []
        for n in self.redpanda._started:
            info = self.admin.get_partitions(self.topic, partition, node=n)
            self.logger.info(
                f"current assignments for {self.topic}-{partition}: {info}"
            )
            converged = PartitionMovementMixin._equal_assignments(
                info["replicas"], assignments
            )
            results.append(converged and info["status"] == "done")
        return all(results)

    def move_topic(self, to_nodes):
        for partition in range(3):

            def get_nodes(partition):
                return list(r["node_id"] for r in partition["replicas"])

            nodes_before = set(
                get_nodes(self.admin.get_partitions(self.topic, partition))
            )
            nodes_after = set(to_nodes)
            if nodes_before == nodes_after:
                continue
            assignments = [
                dict(node_id=n, core=PartitionMovementMixin.INVALID_CORE)
                for n in to_nodes
            ]
            self.admin.set_partition_replicas(self.topic, partition, assignments)

            wait_until(
                lambda: self.movement_done(partition, to_nodes),
                timeout_sec=60,
                backoff_sec=2,
            )

    @cluster(num_nodes=4, log_allow_list=[r"filesystem error: remove failed"])
    def topic_delete_orphan_files_after_move_test(self):
        # Write out 10MB per partition
        self.kafka_tools.produce(
            self.topic, record_size=4096, num_records=2560 * self.partition_count
        )

        self.admin = Admin(self.redpanda)

        # Move every partition to nodes 1,2,3
        self.move_topic([1, 2, 3])

        down_node = self.redpanda.nodes[0]
        try:
            # Make topic directory immutable to prevent deleting
            down_node.account.ssh(
                f"chattr +i {self.redpanda.DATA_DIR}/kafka/{self.topic}"
            )

            # Move every partition from node 1 to node 4
            self.move_topic([2, 3, 4])

            def topic_exist_on_every_node(redpanda, topic_name):
                storage = redpanda.storage()
                exist_on_every = all(
                    map(lambda n: topic_name in n.ns["kafka"].topics, storage.nodes)
                )
                return exist_on_every

            wait_until(
                lambda: topic_exist_on_every_node(self.redpanda, self.topic),
                timeout_sec=30,
                backoff_sec=2,
                err_msg="Topic doesn't exist on some node",
            )

            self.redpanda.stop_node(down_node)
        finally:
            down_node.account.ssh(
                f"chattr -i {self.redpanda.DATA_DIR}/kafka/{self.topic}"
            )

        self.redpanda.start_node(down_node)

        def topic_deleted_on_down_node_and_exist_on_others(
            redpanda, down_node, topic_name
        ):
            storage = redpanda.storage()
            log_removed_on_down = (
                topic_name
                not in next(filter(lambda x: x.name == down_node.name, storage.nodes))
                .ns["kafka"]
                .topics
            )
            logs_not_removed_on_others = all(
                map(
                    lambda n: topic_name in n.ns["kafka"].topics,
                    filter(lambda x: x.name != down_node.name, storage.nodes),
                )
            )
            return log_removed_on_down and logs_not_removed_on_others

        wait_until(
            lambda: topic_deleted_on_down_node_and_exist_on_others(
                self.redpanda, down_node, self.topic
            ),
            timeout_sec=30,
            backoff_sec=2,
            err_msg="Topic storage was not removed on down node or removed on other",
        )

        # The above check is only of metadata, but the metadata will be validated against
        # data segments during generic test teardown checks.


class TopicDeleteStressTest(RedpandaTest):
    """
    The purpose of this test is to execute topic deletion during compaction process.

    The testing strategy is:
        1. Start to produce messaes
        2. Produce until compaction starting
        3. Delete topic
        4. Verify that all data for kafka namespace will be deleted
    """

    def __init__(self, test_context):
        extra_rp_conf = dict(
            log_segment_size=1048576,
            compacted_log_segment_size=1048576,
            log_compaction_interval_ms=300,
            auto_create_topics_enabled=False,
        )

        super(TopicDeleteStressTest, self).__init__(
            test_context=test_context, num_brokers=3, extra_rp_conf=extra_rp_conf
        )

    @cluster(num_nodes=4)
    def stress_test(self):
        for i in range(10):
            spec = TopicSpec(
                partition_count=2, cleanup_policy=TopicSpec.CLEANUP_COMPACT
            )
            topic_name = spec.name
            self.client().create_topic(spec)

            producer = RpkProducer(
                self.test_context, self.redpanda, topic_name, 1024, 100000
            )
            producer.start()

            metrics = [
                MetricCheck(
                    self.logger,
                    self.redpanda,
                    n,
                    "vectorized_storage_log_compacted_segment_total",
                    {},
                    sum,
                )
                for n in self.redpanda.nodes
            ]

            def check_compaction():
                return all(
                    [
                        m.evaluate(
                            [
                                (
                                    "vectorized_storage_log_compacted_segment_total",
                                    lambda a, b: b > 3,
                                )
                            ]
                        )
                        for m in metrics
                    ]
                )

            wait_until(
                check_compaction,
                timeout_sec=120,
                backoff_sec=5,
                err_msg="Segments were not compacted",
            )

            self.client().delete_topic(topic_name)

            try:
                producer.stop()
            except Exception:
                # Should ignore exception form rpk
                pass
            producer.free()

            try:
                wait_until(
                    lambda: topic_storage_purged(self.redpanda, topic_name),
                    timeout_sec=60,
                    backoff_sec=2,
                    err_msg="Topic storage was not removed",
                )

            except:
                # On errors, dump listing of the storage location
                for node in self.redpanda.nodes:
                    self.logger.error(f"Storage listing on {node.name}:")
                    for line in node.account.ssh_capture(
                        f"find {self.redpanda.DATA_DIR}"
                    ):
                        self.logger.error(line.strip())

                raise
