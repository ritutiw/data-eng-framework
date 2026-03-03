from __future__ import annotations

import argparse
import logging
import sys

import pyarrow as pa
import yaml

from kafka_delta_sink.config import AzureStorageConfig, KafkaConfig, SinkConfig
from kafka_delta_sink.sink import KafkaDeltaSink


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_config_from_yaml(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def build_schema_from_config(schema_config: list[dict]) -> pa.Schema:
    type_map = {
        "string": pa.string(),
        "int32": pa.int32(),
        "int64": pa.int64(),
        "float32": pa.float32(),
        "float64": pa.float64(),
        "bool": pa.bool_(),
        "timestamp": pa.timestamp("us", tz="UTC"),
    }
    fields = []
    for col in schema_config:
        pa_type = type_map.get(col["type"], pa.string())
        fields.append(pa.field(col["name"], pa_type, nullable=True))

    fields.extend(
        [
            pa.field("_kafka_topic", pa.string(), nullable=True),
            pa.field("_kafka_partition", pa.int32(), nullable=True),
            pa.field("_kafka_offset", pa.int64(), nullable=True),
            pa.field("_kafka_timestamp", pa.string(), nullable=True),
        ]
    )
    return pa.schema(fields)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="kafka-delta-sink",
        description="Stream data from Apache Kafka to Delta Lake on Azure ADLS Gen2",
    )
    parser.add_argument(
        "-c",
        "--config",
        help="Path to YAML config file",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args(argv)

    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)

    if args.config:
        cfg = load_config_from_yaml(args.config)
        kafka_config = KafkaConfig(**cfg.get("kafka", {}))
        azure_config = AzureStorageConfig(**cfg.get("azure", {}))
        sink_config = SinkConfig(**cfg.get("sink", {}))
        schema = build_schema_from_config(cfg.get("schema", []))
    else:
        kafka_config = KafkaConfig()
        azure_config = AzureStorageConfig()
        sink_config = SinkConfig()
        schema = pa.schema(
            [
                pa.field("_kafka_topic", pa.string(), nullable=True),
                pa.field("_kafka_partition", pa.int32(), nullable=True),
                pa.field("_kafka_offset", pa.int64(), nullable=True),
                pa.field("_kafka_timestamp", pa.string(), nullable=True),
            ]
        )

    if not sink_config.delta_table_uri:
        logger.error("delta_table_uri is required. Set SINK_DELTA_TABLE_URI or use a config file.")
        sys.exit(1)

    if not azure_config.account_name:
        logger.error("account_name is required. Set AZURE_ACCOUNT_NAME or use a config file.")
        sys.exit(1)

    sink = KafkaDeltaSink(
        kafka_config=kafka_config,
        azure_config=azure_config,
        sink_config=sink_config,
        schema=schema,
    )
    sink.start()


if __name__ == "__main__":
    main()
