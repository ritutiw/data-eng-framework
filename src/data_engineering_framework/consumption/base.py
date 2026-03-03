from __future__ import annotations

from abc import ABC, abstractmethod

import pyarrow as pa


class BaseAggregator(ABC):
    """Abstract base aggregator for Gold layer."""

    @abstractmethod
    def aggregate(self, table: pa.Table) -> pa.Table: ...
