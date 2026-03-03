"""Avro message deserializer with Confluent Schema Registry."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from data_engineering_framework.common.logger import get_logger

logger = get_logger(__name__)


def create_avro_deserializer(schema_registry_url: str, schema_str: Optional[str] = None):
    try:
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroDeserializer
    except ImportError:
        raise ImportError(
            "Avro support requires 'confluent-kafka[avro]'. "
            "Install with: pip install 'data-engineering-framework[avro]'"
        )

    registry_client = SchemaRegistryClient({"url": schema_registry_url})
    avro_deser = AvroDeserializer(registry_client, schema_str)

    def deserialize_avro(msg) -> Optional[dict]:
        try:
            value = avro_deser(msg.value(), None)
            if value is None:
                return None
            value["_kafka_topic"] = msg.topic()
            value["_kafka_partition"] = msg.partition()
            value["_kafka_offset"] = msg.offset()
            ts_type, ts_value = msg.timestamp()
            if ts_value and ts_value > 0:
                value["_kafka_timestamp"] = datetime.fromtimestamp(
                    ts_value / 1000, tz=timezone.utc
                ).isoformat()
            else:
                value["_kafka_timestamp"] = datetime.now(tz=timezone.utc).isoformat()
            return value
        except Exception:
            logger.exception(
                "Failed to deserialize Avro message at topic=%s partition=%d offset=%d",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )
            return None

    return deserialize_avro
