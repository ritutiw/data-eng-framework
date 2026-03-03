from __future__ import annotations

from abc import ABC, abstractmethod


class BaseAdapter(ABC):
    @abstractmethod
    def jdbc_driver(self) -> str: ...

    @abstractmethod
    def jdbc_url(self, host: str, port: int, database: str) -> str: ...

    @abstractmethod
    def quote_identifier(self, identifier: str) -> str: ...

    def select_query(self, table: str, columns: list[str] | None = None) -> str:
        cols = ", ".join(self.quote_identifier(c) for c in columns) if columns else "*"
        return f"SELECT {cols} FROM {self.quote_identifier(table)}"

    def incremental_query(
        self,
        table: str,
        watermark_column: str,
        watermark_value: str,
        columns: list[str] | None = None,
    ) -> str:
        base = self.select_query(table, columns)
        return f"{base} WHERE {self.quote_identifier(watermark_column)} > '{watermark_value}'"


class MySQLAdapter(BaseAdapter):
    def jdbc_driver(self) -> str:
        return "com.mysql.cj.jdbc.Driver"

    def jdbc_url(self, host: str, port: int, database: str) -> str:
        return f"jdbc:mysql://{host}:{port}/{database}"

    def quote_identifier(self, identifier: str) -> str:
        return f"`{identifier}`"


class PostgreSQLAdapter(BaseAdapter):
    def jdbc_driver(self) -> str:
        return "org.postgresql.Driver"

    def jdbc_url(self, host: str, port: int, database: str) -> str:
        return f"jdbc:postgresql://{host}:{port}/{database}"

    def quote_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'


class OracleAdapter(BaseAdapter):
    def jdbc_driver(self) -> str:
        return "oracle.jdbc.OracleDriver"

    def jdbc_url(self, host: str, port: int, database: str) -> str:
        return f"jdbc:oracle:thin:@{host}:{port}/{database}"

    def quote_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'


ADAPTER_MAP: dict[str, type[BaseAdapter]] = {
    "mysql": MySQLAdapter,
    "postgresql": PostgreSQLAdapter,
    "oracle": OracleAdapter,
}
