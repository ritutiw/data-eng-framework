"""Feature store for Platinum layer ML pipelines."""

from __future__ import annotations

from datetime import datetime, timezone

import pyarrow as pa

from data_engineering_framework.common.logger import get_logger

logger = get_logger(__name__)


class FeatureStore:
    def __init__(self, entity_key: str, feature_columns: list[str]):
        self._entity_key = entity_key
        self._feature_columns = feature_columns

    def build_feature_table(self, source: pa.Table) -> pa.Table:
        columns = [self._entity_key] + [
            c for c in self._feature_columns if c in source.column_names
        ]
        table = source.select(columns)

        now = datetime.now(tz=timezone.utc).isoformat()
        table = table.append_column(
            "_feature_timestamp",
            pa.array([now] * table.num_rows, type=pa.string()),
        )

        logger.info(
            "Built feature table: %d rows, %d features",
            table.num_rows,
            len(columns) - 1,
        )
        return table

    def select_features(self, table: pa.Table, features: list[str]) -> pa.Table:
        available = [f for f in features if f in table.column_names]
        return table.select([self._entity_key] + available)
