"""Data cleansing operations for Silver layer."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

from data_engineering_framework.common.logger import get_logger
from data_engineering_framework.processing.base import BaseProcessor

logger = get_logger(__name__)


class CleansingProcessor(BaseProcessor):
    def __init__(
        self,
        dedup_columns: list[str] | None = None,
        drop_null_columns: list[str] | None = None,
    ):
        self._dedup_columns = dedup_columns
        self._drop_null_columns = drop_null_columns

    def process(self, table: pa.Table) -> pa.Table:
        if self._drop_null_columns:
            for col in self._drop_null_columns:
                if col in table.column_names:
                    mask = pc.is_valid(table.column(col))
                    table = table.filter(mask)

        if self._dedup_columns:
            table = self._deduplicate(table)

        logger.info("Cleansing complete: %d rows", table.num_rows)
        return table

    def validate(self, table: pa.Table) -> bool:
        return table.num_rows > 0

    def _deduplicate(self, table: pa.Table) -> pa.Table:
        indices = list(range(table.num_rows))
        seen: set[tuple] = set()
        keep: list[int] = []

        cols = self._dedup_columns or table.column_names
        for i in indices:
            key = tuple(table.column(c)[i].as_py() for c in cols if c in table.column_names)
            if key not in seen:
                seen.add(key)
                keep.append(i)

        return table.take(keep)
