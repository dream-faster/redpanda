# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, Optional, cast

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from rptest.services import tls
from rptest.services.redpanda_types import KafkaClientSecurity


@contextmanager
def ck_consumer(config: dict[str, Any], **kwargs: Any) -> Iterator[Consumer]:
    """A confluent_kafka ``Consumer`` that is closed deterministically on exit.

    Prefer this over a bare ``Consumer(...)``: an unclosed consumer destroyed
    later by the GC can deadlock (CORE-16410).
    """
    consumer = Consumer(config, **kwargs)
    try:
        yield consumer
    finally:
        consumer.close()


class PythonLibrdkafka:
    """
    https://github.com/confluentinc/confluent-kafka-python
    """

    def __init__(
        self,
        redpanda,
        *,
        username: str | None = None,
        password: str | None = None,
        algorithm: str | None = None,
        tls_cert: Optional[tls.Certificate] = None,
    ):
        self._redpanda = redpanda
        self._security = cast(
            KafkaClientSecurity, redpanda.kafka_client_security()
        ).override(username, password, algorithm, None)
        self._tls_cert = tls_cert

    def brokers(self) -> dict[int, Any]:
        client = AdminClient(self._get_config())
        return client.list_topics(timeout=10).brokers

    def topics(self, topic: str | None = None):
        client = AdminClient(self._get_config())
        return client.list_topics(timeout=10, topic=topic).topics

    def create_topic(self, spec):
        topics = [
            NewTopic(
                spec.name,
                num_partitions=spec.partition_count,
                replication_factor=spec.replication_factor,
            )
        ]
        client = AdminClient(self._get_config())
        res = client.create_topics(topics, request_timeout=10)
        for topic, fut in res.items():
            try:
                fut.result()
                self._redpanda.logger.debug(f"topic {topic} created")
            except Exception as e:
                self._redpanda.logger.debug(f"topic {topic} creation failed: {e}")
                raise

    def get_client(self):
        return AdminClient(self._get_config())

    def get_producer(self) -> Producer:
        producer_conf = self._get_config()
        self._redpanda.logger.debug(f"{producer_conf}")
        return Producer(producer_conf)

    def get_consumer(self, extra_config: dict[str, Any] = {}):
        conf = self._get_config()
        conf.update(extra_config)
        self._redpanda.logger.debug(f"{conf}")
        return Consumer(conf)

    def _get_config(self):
        conf = {
            "bootstrap.servers": self._redpanda.brokers(),
        }

        conf.update(self._get_sasl_config())

        if self._tls_cert:
            conf.update(
                {
                    "ssl.key.location": self._tls_cert.key,
                    "ssl.certificate.location": self._tls_cert.crt,
                    "ssl.ca.location": self._tls_cert.ca.crt,
                }
            )
            if self._redpanda.sasl_enabled():
                conf.update(
                    {
                        "security.protocol": "sasl_ssl",
                    }
                )
            else:
                conf.update(
                    {
                        "security.protocol": "ssl",
                    }
                )
        # log as debug to eliminate log spamming
        # when creating a lot of producers
        self._redpanda.logger.debug(conf)
        return conf

    def _get_sasl_config(self) -> dict[str, str]:
        c = self._security.simple_credentials()
        return (
            {
                "security.protocol": self._security.security_protocol.name.lower(),
                "sasl.username": c.username,
                "sasl.password": c.password,
                "sasl.mechanism": c.mechanism,
            }
            if c
            else {}
        )
