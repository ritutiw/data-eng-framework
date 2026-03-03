from __future__ import annotations

from abc import ABC, abstractmethod

import pyarrow as pa


class BaseWriter(ABC):
    @abstractmethod
    def write(self, table: pa.Table) -> None: ...

    @abstractmethod
    def table_exists(self) -> bool: ...
