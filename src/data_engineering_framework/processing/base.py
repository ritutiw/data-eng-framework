from __future__ import annotations

from abc import ABC, abstractmethod

import pyarrow as pa


class BaseProcessor(ABC):
    @abstractmethod
    def process(self, table: pa.Table) -> pa.Table: ...

    @abstractmethod
    def validate(self, table: pa.Table) -> bool: ...
