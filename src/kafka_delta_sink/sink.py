from __future__ import annotations

import logging
import signal
import time
from typing import Callable, Optional

import pyarrow as pa

from kafka_delta_sink.config import AzureStorageConfig, KafkaConfig, SinkConfig
from kafka_delta_sink.consumer import KafkaConsumerWrapper
from kafka_delta_sink.delta_writer import DeltaWriter
from kafka_delta_sink.serialization.json_deser import deserialize_json

logger = logging.getLogger(__name__)


class KafkaDeltaSink:
    def __init__(
        self,
        kafka_config: KafkaConfig,
        azure_config: AzureStorageConfig,
        sink_config: SinkConfig,
        schema: pa.Schema,
        deserializer: Optional[Callable] = None,
    ):
        self._kafka_config = kafka_config
        self._sink_config = sink_config
        self._schema = schema
        self._deserializer = deserializer or deserialize_json

        self._consumer = KafkaConsumerWrapper(
            config=kafka_config.to_confluent_config(),
            topics=sink_config.topics,
        )
        self._writer = DeltaWriter(
            table_uri=sink_config.delta_table_uri,
            storage_options=azure_config.to_storage_options(),
            partition_by=sink_config.partition_by or None,
            write_mode=sink_config.write_mode,
            schema_mode=sink_config.schema_mode,
        )

        self._buffer: list[dict] = []
        self._last_flush_time = time.monotonic()
        self._running = False
        self._messages_processed = 0
        self._batches_written = 0

    def start(self) -> None:
        self._running = True
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        self._consumer.start()
        logger.info(
            "Sink started. Topics=%s, Delta=%s, batch_size=%d, timeout=%.1fs",
            self._sink_config.topics,
            self._sink_config.delta_table_uri,
            self._sink_config.batch_size,
            self._sink_config.batch_timeout_seconds,
        )

        try:
            while self._running:
                msg = self._consumer.poll(timeout=1.0)

                if msg is None:
                    self._maybe_flush_on_timeout()
                    continue

                record = self._deserializer(msg)
                if record is not None:
                    self._buffer.append(record)
                    self._messages_processed += 1

                if len(self._buffer) >= self._sink_config.batch_size:
                    self._flush()
                else:
                    self._maybe_flush_on_timeout()
        finally:
            self._flush()
            self._consumer.close()
            logger.info(
                "Sink stopped. Total messages=%d, batches=%d",
                self._messages_processed,
                self._batches_written,
            )

    def _maybe_flush_on_timeout(self) -> None:
        elapsed = time.monotonic() - self._last_flush_time
        if self._buffer and elapsed >= self._sink_config.batch_timeout_seconds:
            self._flush()

    def _flush(self) -> None:
        if not self._buffer:
            return

        count = len(self._buffer)
        logger.info("Flushing %d records to Delta table...", count)

        try:
            table = pa.Table.from_pylist(self._buffer, schema=self._schema)
            self._writer.write(table)
            self._consumer.commit()

            self._buffer.clear()
            self._last_flush_time = time.monotonic()
            self._batches_written += 1
            logger.info("Batch %d written (%d records).", self._batches_written, count)
        except Exception:
            logger.exception("Failed to flush batch of %d records. Will retry.", count)

    def _signal_handler(self, signum, frame) -> None:
        logger.info("Received signal %d, shutting down...", signum)
        self._running = False

    @property
    def stats(self) -> dict:
        return {
            "messages_processed": self._messages_processed,
            "batches_written": self._batches_written,
            "buffer_size": len(self._buffer),
            "running": self._running,
        }
