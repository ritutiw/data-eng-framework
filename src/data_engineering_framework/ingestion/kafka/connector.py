"""Kafka connector implementing BaseConnector with Template Method pattern."""

from __future__ import annotations

import signal
import time
from typing import Callable, Iterator, Optional

from data_engineering_framework.common.logger import get_logger
from data_engineering_framework.ingestion.base import BaseConnector
from data_engineering_framework.ingestion.kafka.consumer import KafkaConsumerWrapper
from data_engineering_framework.ingestion.kafka.serialization.json_deser import deserialize_json
from data_engineering_framework.ingestion.registry import ConnectorRegistry

logger = get_logger(__name__)


@ConnectorRegistry.register("kafka")
class KafkaConnector(BaseConnector):
    def __init__(self, config: dict):
        super().__init__(config)
        self._consumer: Optional[KafkaConsumerWrapper] = None
        self._deserializer: Callable = config.get("deserializer", deserialize_json)
        self._batch_size: int = config.get("batch_size", 10_000)
        self._batch_timeout: float = config.get("batch_timeout_seconds", 60.0)
        self._running = False
        self._messages_processed = 0
        self._batches_yielded = 0

    def connect(self) -> None:
        confluent_config = {
            "bootstrap.servers": self._config.get("bootstrap_servers", "localhost:9092"),
            "group.id": self._config.get("group_id", "data-eng-framework"),
            "auto.offset.reset": self._config.get("auto_offset_reset", "earliest"),
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "security.protocol": self._config.get("security_protocol", "PLAINTEXT"),
        }
        sasl_mechanism = self._config.get("sasl_mechanism")
        if sasl_mechanism:
            confluent_config["sasl.mechanism"] = sasl_mechanism
        sasl_username = self._config.get("sasl_username")
        if sasl_username:
            confluent_config["sasl.username"] = sasl_username
        sasl_password = self._config.get("sasl_password")
        if sasl_password:
            confluent_config["sasl.password"] = sasl_password

        topics = self._config.get("topics", ["default-topic"])
        self._consumer = KafkaConsumerWrapper(config=confluent_config, topics=topics)
        self._consumer.start()
        self._connected = True
        logger.info("KafkaConnector connected to %s", confluent_config["bootstrap.servers"])

    def extract(self) -> Iterator[list[dict]]:
        if self._consumer is None:
            raise RuntimeError("Not connected. Call connect() first.")

        self._running = True
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        buffer: list[dict] = []
        last_flush = time.monotonic()

        while self._running:
            msg = self._consumer.poll(timeout=1.0)

            if msg is None:
                elapsed = time.monotonic() - last_flush
                if buffer and elapsed >= self._batch_timeout:
                    yield buffer
                    self._consumer.commit()
                    self._batches_yielded += 1
                    buffer = []
                    last_flush = time.monotonic()
                continue

            record = self._deserializer(msg)
            if record is not None:
                buffer.append(record)
                self._messages_processed += 1

            if len(buffer) >= self._batch_size:
                yield buffer
                self._consumer.commit()
                self._batches_yielded += 1
                buffer = []
                last_flush = time.monotonic()

        if buffer:
            yield buffer
            self._consumer.commit()
            self._batches_yielded += 1

    def close(self) -> None:
        self._running = False
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None
        self._connected = False
        logger.info(
            "KafkaConnector closed. messages=%d batches=%d",
            self._messages_processed,
            self._batches_yielded,
        )

    def _signal_handler(self, signum, frame) -> None:
        logger.info("Received signal %d, shutting down KafkaConnector...", signum)
        self._running = False

    @property
    def stats(self) -> dict:
        return {
            "messages_processed": self._messages_processed,
            "batches_yielded": self._batches_yielded,
            "running": self._running,
        }
