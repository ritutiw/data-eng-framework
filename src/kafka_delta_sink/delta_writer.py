"""Delta Lake writer using delta-rs."""

from __future__ import annotations

import logging
from typing import Optional

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

logger = logging.getLogger(__name__)


class DeltaWriter:
    def __init__(
        self,
        table_uri: str,
        storage_options: dict,
        partition_by: Optional[list[str]] = None,
        write_mode: str = "append",
        schema_mode: Optional[str] = None,
    ):
        self.table_uri = table_uri
        self.storage_options = storage_options
        self.partition_by = partition_by or []
        self.write_mode = write_mode
        self.schema_mode = schema_mode

    def write(self, table: pa.Table) -> None:
        kwargs: dict = {
            "table_or_uri": self.table_uri,
            "data": table,
            "mode": self.write_mode,
            "storage_options": self.storage_options,
        }
        if self.partition_by:
            kwargs["partition_by"] = self.partition_by
        if self.schema_mode:
            kwargs["schema_mode"] = self.schema_mode

        write_deltalake(**kwargs)
        logger.info("Wrote %d rows to %s", table.num_rows, self.table_uri)

    def optimize(self) -> None:
        dt = DeltaTable(self.table_uri, storage_options=self.storage_options)
        dt.optimize.compact()

    def vacuum(self, retention_hours: int = 168) -> None:
        dt = DeltaTable(self.table_uri, storage_options=self.storage_options)
        dt.vacuum(retention_hours=retention_hours, enforce_retention_duration=True, dry_run=False)

    def table_exists(self) -> bool:
        try:
            DeltaTable(self.table_uri, storage_options=self.storage_options)
            return True
        except Exception:
            return False

    def get_version(self) -> int:
        dt = DeltaTable(self.table_uri, storage_options=self.storage_options)
        return dt.version()
