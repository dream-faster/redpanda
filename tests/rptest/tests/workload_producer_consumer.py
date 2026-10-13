# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import uuid

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.kgo_verifier_services import (
    KgoVerifierProducer,
    KgoVerifierSeqConsumer,
)
from rptest.services.workload_protocol import PWorkload
from rptest.tests.prealloc_nodes import PreallocNodesTest


class ProducerConsumerWorkload(PWorkload):
    """
    This workload will setup a KGoProducer/Consumer to verify operations across an upgrade
    """

    def __init__(self, ctx: PreallocNodesTest) -> None:
        self.ctx = ctx
        self.rpk = RpkTool(self.ctx.redpanda)
        self.last_high_watermark = 0

        is_debug = self.ctx.debug_mode

        # setup a produce workload that should span the whole test run, and then add a buffer to it
        produce_byte_rate = 256 * 1024  # a quarter of Mb per second
        target_runtime_secs = 20 * 60
        self.MSG_SIZE = 256
        self.PRODUCE_COUNT = (produce_byte_rate * target_runtime_secs) // self.MSG_SIZE
        # the topic is requires less resources in debug mode for the sake of the test
        self.topic = TopicSpec(
            name=f"topic-{self.__class__.__name__}-{str(uuid.uuid4())}",
            partition_count=1 if is_debug else 3,
            replication_factor=1 if is_debug else 3,
            segment_bytes=1024 * 1024,
        )

        self._producer = KgoVerifierProducer(
            self.ctx.test_context,
            self.ctx.redpanda,
            self.topic,
            msg_size=self.MSG_SIZE,
            msg_count=self.PRODUCE_COUNT,
            rate_limit_bps=produce_byte_rate,
            custom_node=self.ctx.preallocated_nodes,
        )
        self._seq_consumer = KgoVerifierSeqConsumer(
            self.ctx.test_context,
            self.ctx.redpanda,
            self.topic,
            self.MSG_SIZE,
            nodes=self.ctx.preallocated_nodes,
        )

    def begin(self):
        self.ctx.client().create_topic(self.topic)

        self._producer.start(clean=False)
        self._producer.wait_for_offset_map()
        self._seq_consumer.start(clean=False)

    def end(self):
        self._producer.wait()
        self._producer.stop()
        self._seq_consumer.wait()
        self._seq_consumer.stop()
        wrote_at_least = self._producer.produce_status.acked
        assert (
            self._seq_consumer.consumer_status.validator.valid_reads >= wrote_at_least
        )

        self.ctx.client().delete_topic(self.topic.name)

    def get_earliest_applicable_release(self):
        return (22, 3)

    def on_cluster_upgraded(self, version: tuple[int, int, int]) -> int:
        """
        check that the high watermark of partition 0 advances, i.e. that
        produce progress is made on the active redpanda version
        """
        partition_zero = next(self.rpk.describe_topic(self.topic.name))
        high_watermark = partition_zero.high_watermark

        if high_watermark > self.last_high_watermark:
            self.last_high_watermark = high_watermark
            return PWorkload.DONE

        return PWorkload.NOT_DONE
