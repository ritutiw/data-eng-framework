from __future__ import annotations

from datetime import datetime, timezone

import pyarrow as pa

from data_engineering_framework.common.logger import get_logger

logger = get_logger(__name__)

SCD_COLUMNS = ["_effective_from", "_effective_to", "_is_current", "_version"]


def add_scd_columns(table: pa.Table) -> pa.Table:
    now = datetime.now(tz=timezone.utc).isoformat()
    n = table.num_rows

    table = table.append_column("_effective_from", pa.array([now] * n, type=pa.string()))
    table = table.append_column("_effective_to", pa.array([None] * n, type=pa.string()))
    table = table.append_column("_is_current", pa.array([True] * n, type=pa.bool_()))
    table = table.append_column("_version", pa.array([1] * n, type=pa.int64()))

    logger.info("Added SCD Type 2 columns to %d rows", n)
    return table


def expire_records(table: pa.Table) -> pa.Table:
    now = datetime.now(tz=timezone.utc).isoformat()
    n = table.num_rows

    idx = table.column_names.index("_effective_to")
    table = table.set_column(idx, "_effective_to", pa.array([now] * n, type=pa.string()))

    idx = table.column_names.index("_is_current")
    table = table.set_column(idx, "_is_current", pa.array([False] * n, type=pa.bool_()))

    return table
