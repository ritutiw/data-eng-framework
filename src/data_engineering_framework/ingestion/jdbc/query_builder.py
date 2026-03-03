"""Parameterized query builder for JDBC connectors."""

from __future__ import annotations

from data_engineering_framework.ingestion.jdbc.adapters import BaseAdapter


class QueryBuilder:
    def __init__(self, adapter: BaseAdapter):
        self._adapter = adapter

    def full_load(self, table: str, columns: list[str] | None = None) -> str:
        return self._adapter.select_query(table, columns)

    def incremental_load(
        self,
        table: str,
        watermark_column: str,
        watermark_value: str,
        columns: list[str] | None = None,
    ) -> str:
        return self._adapter.incremental_query(table, watermark_column, watermark_value, columns)

    def count_query(self, table: str) -> str:
        return f"SELECT COUNT(*) FROM {self._adapter.quote_identifier(table)}"
