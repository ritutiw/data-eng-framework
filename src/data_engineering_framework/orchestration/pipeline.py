"""Pipeline orchestrator for end-to-end data flows."""

from __future__ import annotations

from typing import Optional

import pyarrow as pa

from data_engineering_framework.common.logger import get_logger
from data_engineering_framework.ingestion.base import BaseConnector
from data_engineering_framework.processing.base import BaseProcessor
from data_engineering_framework.writers.base import BaseWriter

logger = get_logger(__name__)


class Pipeline:
    def __init__(
        self,
        name: str,
        connector: BaseConnector,
        writer: BaseWriter,
        schema: pa.Schema,
        processor: Optional[BaseProcessor] = None,
    ):
        self._name = name
        self._connector = connector
        self._writer = writer
        self._schema = schema
        self._processor = processor
        self._batches_written = 0
        self._total_rows = 0

    def execute(self) -> dict:
        logger.info("Pipeline '%s' started", self._name)

        for batch in self._connector.run():
            table = pa.Table.from_pylist(batch, schema=self._schema)

            if self._processor:
                if not self._processor.validate(table):
                    logger.warning("Batch failed validation, skipping")
                    continue
                table = self._processor.process(table)

            self._writer.write(table)
            self._batches_written += 1
            self._total_rows += table.num_rows
            logger.info(
                "Pipeline '%s': batch %d written (%d rows)",
                self._name,
                self._batches_written,
                table.num_rows,
            )

        logger.info(
            "Pipeline '%s' completed: %d batches, %d total rows",
            self._name,
            self._batches_written,
            self._total_rows,
        )
        return self.stats

    @property
    def stats(self) -> dict:
        return {
            "pipeline": self._name,
            "batches_written": self._batches_written,
            "total_rows": self._total_rows,
        }
