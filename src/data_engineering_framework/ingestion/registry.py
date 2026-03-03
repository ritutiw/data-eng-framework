from __future__ import annotations

from typing import Type

from data_engineering_framework.common.logger import get_logger
from data_engineering_framework.ingestion.base import BaseConnector

logger = get_logger(__name__)


class ConnectorRegistry:
    _connectors: dict[str, Type[BaseConnector]] = {}

    @classmethod
    def register(cls, name: str):
        def decorator(connector_cls: Type[BaseConnector]):
            cls._connectors[name] = connector_cls
            logger.debug("Registered connector: %s -> %s", name, connector_cls.__name__)
            return connector_cls

        return decorator

    @classmethod
    def get(cls, name: str) -> Type[BaseConnector]:
        connector_cls = cls._connectors.get(name)
        if connector_cls is None:
            raise KeyError(f"Unknown connector '{name}'. Available: {list(cls._connectors.keys())}")
        return connector_cls

    @classmethod
    def available(cls) -> list[str]:
        return list(cls._connectors.keys())
