"""Abstract base aggregator for Gold layer."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pyarrow as pa


class BaseAggregator(ABC):
    @abstractmethod
    def aggregate(self, table: pa.Table) -> pa.Table: ...
