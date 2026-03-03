from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterator

from data_engineering_framework.common.logger import get_logger

logger = get_logger(__name__)


class BaseConnector(ABC):
    """Template Method pattern for source connectors.

    Subclasses implement connect/extract/close; the run() method
    orchestrates the lifecycle.
    """

    def __init__(self, config: dict):
        self._config = config
        self._connected = False

    def run(self) -> Iterator[list[dict]]:
        self.connect()
        try:
            yield from self.extract()
        finally:
            self.close()

    @abstractmethod
    def connect(self) -> None: ...

    @abstractmethod
    def extract(self) -> Iterator[list[dict]]: ...

    @abstractmethod
    def close(self) -> None: ...

    @property
    def connector_type(self) -> str:
        return self.__class__.__name__
