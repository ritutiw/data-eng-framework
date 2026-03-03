"""Aggregation operations for Gold layer."""

from __future__ import annotations

import pyarrow as pa

from data_engineering_framework.common.logger import get_logger
from data_engineering_framework.consumption.base import BaseAggregator

logger = get_logger(__name__)


class GroupByAggregator(BaseAggregator):
    def __init__(self, group_keys: list[str], aggregations: dict[str, str]):
        self._group_keys = group_keys
        self._aggregations = aggregations

    def aggregate(self, table: pa.Table) -> pa.Table:
        agg_specs = []
        for col, func in self._aggregations.items():
            agg_specs.append((col, func))

        result = table.group_by(self._group_keys).aggregate(agg_specs)
        logger.info(
            "Aggregated %d rows -> %d rows by %s",
            table.num_rows,
            result.num_rows,
            self._group_keys,
        )
        return result


class CountAggregator(BaseAggregator):
    def __init__(self, group_keys: list[str]):
        self._group_keys = group_keys

    def aggregate(self, table: pa.Table) -> pa.Table:
        counts = table.group_by(self._group_keys).aggregate([(self._group_keys[0], "count")])
        logger.info("Count aggregation: %d groups", counts.num_rows)
        return counts
