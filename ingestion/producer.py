from __future__ import annotations

import logging
from typing import Iterable

from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from ingestion.schema import Event

TOPIC = "transactions"
PARTITIONS = 3

log = logging.getLogger(__name__)


async def ensure_topic(bootstrap: str, topic: str = TOPIC, partitions: int = PARTITIONS) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        if topic in await admin.list_topics():
            return
        response = await admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])
        _, code, *_ = response.topic_errors[0]
        if code == 0:
            log.info("created topic %s with %d partitions", topic, partitions)
        elif code != TopicAlreadyExistsError.errno:
            raise RuntimeError(f"create_topics {topic} failed with error code {code}")
    finally:
        await admin.close()


class EventProducer:
    def __init__(self, bootstrap: str, topic: str = TOPIC) -> None:
        self.bootstrap = bootstrap
        self.topic = topic
        self.sent = 0
        self._producer = AIOKafkaProducer(bootstrap_servers=bootstrap, enable_idempotence=True, linger_ms=20)

    async def __aenter__(self) -> EventProducer:
        await ensure_topic(self.bootstrap, self.topic)
        await self._producer.start()
        return self

    async def __aexit__(self, *exc) -> None:
        await self._producer.stop()

    async def send(self, events: Iterable[Event]) -> None:
        for event in events:
            await self._producer.send(self.topic, key=event.sender.encode(), value=event.to_json())
            self.sent += 1
            if self.sent % 1000 == 0:
                log.info("sent %d events", self.sent)
