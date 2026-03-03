"""Connector factory using Factory pattern backed by the registry."""

from __future__ import annotations

from data_engineering_framework.ingestion.base import BaseConnector
from data_engineering_framework.ingestion.registry import ConnectorRegistry


class ConnectorFactory:
    @staticmethod
    def create(connector_type: str, config: dict) -> BaseConnector:
        connector_cls = ConnectorRegistry.get(connector_type)
        return connector_cls(config)
