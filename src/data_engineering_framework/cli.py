"""CLI entry point for data-engineering-framework."""

from __future__ import annotations

import argparse
import sys

import pyarrow as pa
import yaml

from data_engineering_framework.common.config import AzureStorageConfig
from data_engineering_framework.common.logger import get_logger
from data_engineering_framework.ingestion.factory import ConnectorFactory
from data_engineering_framework.orchestration.pipeline import Pipeline
from data_engineering_framework.writers.delta_writer import DeltaWriter

logger = get_logger(__name__)

TYPE_MAP = {
    "string": pa.string(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "bool": pa.bool_(),
    "timestamp": pa.timestamp("us", tz="UTC"),
}


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def build_schema(schema_config: list[dict]) -> pa.Schema:
    fields = []
    for col in schema_config:
        pa_type = TYPE_MAP.get(col["type"], pa.string())
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
        prog="data-eng-framework",
        description="Data Engineering Framework — Medallion architecture pipeline",
    )
    parser.add_argument("-c", "--config", help="Path to YAML config file")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    parser.add_argument(
        "--connector",
        default="kafka",
        help="Connector type: kafka, jdbc",
    )
    args = parser.parse_args(argv)

    get_logger("data_engineering_framework", args.log_level)

    if not args.config:
        logger.error("Config file required. Use -c/--config.")
        sys.exit(1)

    cfg = load_config(args.config)

    connector_type = cfg.get("connector_type", args.connector)
    connector_config = cfg.get("connector", {})
    connector = ConnectorFactory.create(connector_type, connector_config)

    azure_config = AzureStorageConfig(**cfg.get("azure", {}))
    sink_cfg = cfg.get("sink", {})

    writer = DeltaWriter(
        table_uri=sink_cfg.get("delta_table_uri", ""),
        storage_options=azure_config.to_storage_options(),
        partition_by=sink_cfg.get("partition_by"),
        write_mode=sink_cfg.get("write_mode", "append"),
        schema_mode=sink_cfg.get("schema_mode"),
    )

    schema = build_schema(cfg.get("schema", []))

    pipeline = Pipeline(
        name=cfg.get("pipeline_name", "default"),
        connector=connector,
        writer=writer,
        schema=schema,
    )

    pipeline.execute()


if __name__ == "__main__":
    main()
