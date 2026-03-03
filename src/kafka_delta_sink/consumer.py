from __future__ import annotations

import logging
from typing import Optional

from confluent_kafka import Consumer, KafkaError, KafkaException

logger = logging.getLogger(__name__)


class KafkaConsumerWrapper:
    def __init__(self, config: dict, topics: list[str]):
        self._config = config
        self._topics = topics
        self._consumer: Optional[Consumer] = None

    def start(self) -> None:
        self._consumer = Consumer(self._config)
        self._consumer.subscribe(self._topics)
        logger.info("Subscribed to topics: %s", self._topics)

    def poll(self, timeout: float = 1.0):
        if self._consumer is None:
            raise RuntimeError("Consumer not started. Call start() first.")

        msg = self._consumer.poll(timeout=timeout)
        if msg is None:
            return None

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return None
            raise KafkaException(msg.error())

        return msg

    def commit(self) -> None:
        if self._consumer is None:
            raise RuntimeError("Consumer not started. Call start() first.")
        self._consumer.commit(asynchronous=False)

    def close(self) -> None:
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None
            logger.info("Consumer closed.")
