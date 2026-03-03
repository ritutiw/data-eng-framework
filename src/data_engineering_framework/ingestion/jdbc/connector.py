"""JDBC connector using PySpark JDBC driver with Adapter strategy pattern."""

from __future__ import annotations

from typing import Iterator

from data_engineering_framework.common.logger import get_logger
from data_engineering_framework.ingestion.base import BaseConnector
from data_engineering_framework.ingestion.jdbc.adapters import ADAPTER_MAP, BaseAdapter
from data_engineering_framework.ingestion.registry import ConnectorRegistry

logger = get_logger(__name__)


@ConnectorRegistry.register("jdbc")
class JDBCConnector(BaseConnector):
    """JDBC connector using PySpark JDBC driver with pluggable adapter strategy.

    Config keys:
        adapter: mysql | postgresql | oracle
        host, port, database, user, password: connection params
        table: source table name
        columns: optional list of columns to select
        watermark_column: optional column for incremental loads
        watermark_value: optional last-seen watermark value
        fetch_size: rows per batch (default 10000)
        num_partitions: parallel read partitions (default 1)
        partition_column: column for parallel reads
        lower_bound: lower bound for partition column
        upper_bound: upper bound for partition column
    """

    def __init__(self, config: dict):
        super().__init__(config)
        adapter_name = config.get("adapter", "postgresql")
        adapter_cls = ADAPTER_MAP.get(adapter_name)
        if adapter_cls is None:
            raise ValueError(
                f"Unknown adapter '{adapter_name}'. Available: {list(ADAPTER_MAP.keys())}"
            )
        self._adapter: BaseAdapter = adapter_cls()
        self._spark = None
        self._fetch_size: int = config.get("fetch_size", 10_000)

    def connect(self) -> None:
        from pyspark.sql import SparkSession

        self._spark = SparkSession.builder.appName(
            f"jdbc-{self._config.get('adapter', 'postgresql')}-{self._config['table']}"
        ).getOrCreate()
        self._connected = True
        logger.info(
            "JDBCConnector connected via PySpark to %s:%s/%s using %s",
            self._config["host"],
            self._config.get("port"),
            self._config["database"],
            self._adapter.jdbc_driver(),
        )

    def extract(self) -> Iterator[list[dict]]:
        if self._spark is None:
            raise RuntimeError("Not connected. Call connect() first.")

        table = self._config["table"]
        columns = self._config.get("columns")
        watermark_column = self._config.get("watermark_column")
        watermark_value = self._config.get("watermark_value")

        if watermark_column and watermark_value:
            query = self._adapter.incremental_query(
                table, watermark_column, watermark_value, columns
            )
            dbtable = f"({query}) as incremental_query"
        elif columns:
            query = self._adapter.select_query(table, columns)
            dbtable = f"({query}) as full_query"
        else:
            dbtable = table

        jdbc_url = self._adapter.jdbc_url(
            host=self._config["host"],
            port=self._config.get("port", 5432),
            database=self._config["database"],
        )

        read_options = {
            "url": jdbc_url,
            "dbtable": dbtable,
            "user": self._config["user"],
            "password": self._config["password"],
            "driver": self._adapter.jdbc_driver(),
            "fetchsize": str(self._fetch_size),
        }

        partition_column = self._config.get("partition_column")
        if partition_column:
            read_options["partitionColumn"] = partition_column
            read_options["numPartitions"] = str(self._config.get("num_partitions", 1))
            read_options["lowerBound"] = str(self._config.get("lower_bound", 0))
            read_options["upperBound"] = str(self._config.get("upper_bound", 1000000))

        df = self._spark.read.format("jdbc").options(**read_options).load()

        for partition in df.toLocalIterator(prefetchPartitions=True):
            yield [partition.asDict()]

    def close(self) -> None:
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
        self._connected = False
        logger.info("JDBCConnector closed.")
