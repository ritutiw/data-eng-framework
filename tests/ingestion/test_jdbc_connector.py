"""Tests for JDBCConnector and adapters."""

import pytest

from data_engineering_framework.ingestion.jdbc.adapters import (
    ADAPTER_MAP,
    MySQLAdapter,
    OracleAdapter,
    PostgreSQLAdapter,
)
from data_engineering_framework.ingestion.jdbc.connector import JDBCConnector
from data_engineering_framework.ingestion.jdbc.query_builder import QueryBuilder


class TestAdapters:
    def test_mysql_jdbc_url(self):
        adapter = MySQLAdapter()
        url = adapter.jdbc_url("localhost", 3306, "testdb")
        assert url == "jdbc:mysql://localhost:3306/testdb"

    def test_mysql_driver(self):
        adapter = MySQLAdapter()
        assert adapter.jdbc_driver() == "com.mysql.cj.jdbc.Driver"

    def test_postgresql_jdbc_url(self):
        adapter = PostgreSQLAdapter()
        url = adapter.jdbc_url("localhost", 5432, "testdb")
        assert url == "jdbc:postgresql://localhost:5432/testdb"

    def test_postgresql_driver(self):
        adapter = PostgreSQLAdapter()
        assert adapter.jdbc_driver() == "org.postgresql.Driver"

    def test_oracle_jdbc_url(self):
        adapter = OracleAdapter()
        url = adapter.jdbc_url("localhost", 1521, "testdb")
        assert url == "jdbc:oracle:thin:@localhost:1521/testdb"

    def test_oracle_driver(self):
        adapter = OracleAdapter()
        assert adapter.jdbc_driver() == "oracle.jdbc.OracleDriver"

    def test_mysql_quote(self):
        adapter = MySQLAdapter()
        assert adapter.quote_identifier("column") == "`column`"

    def test_postgresql_quote(self):
        adapter = PostgreSQLAdapter()
        assert adapter.quote_identifier("column") == '"column"'

    def test_adapter_map_completeness(self):
        assert "mysql" in ADAPTER_MAP
        assert "postgresql" in ADAPTER_MAP
        assert "oracle" in ADAPTER_MAP


class TestQueryBuilder:
    def test_full_load(self):
        adapter = PostgreSQLAdapter()
        builder = QueryBuilder(adapter)
        query = builder.full_load("orders")
        assert query == 'SELECT * FROM "orders"'

    def test_full_load_with_columns(self):
        adapter = MySQLAdapter()
        builder = QueryBuilder(adapter)
        query = builder.full_load("orders", ["id", "amount"])
        assert query == "SELECT `id`, `amount` FROM `orders`"

    def test_incremental_load(self):
        adapter = PostgreSQLAdapter()
        builder = QueryBuilder(adapter)
        query = builder.incremental_load("orders", "updated_at", "2024-01-01")
        assert '"updated_at"' in query
        assert "2024-01-01" in query

    def test_count_query(self):
        adapter = PostgreSQLAdapter()
        builder = QueryBuilder(adapter)
        query = builder.count_query("orders")
        assert query == 'SELECT COUNT(*) FROM "orders"'


class TestJDBCConnector:
    def test_init_default_adapter(self):
        config = {
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "user": "user",
            "password": "pass",
            "table": "orders",
        }
        connector = JDBCConnector(config)
        assert isinstance(connector._adapter, PostgreSQLAdapter)

    def test_init_mysql_adapter(self):
        config = {
            "adapter": "mysql",
            "host": "localhost",
            "port": 3306,
            "database": "testdb",
            "user": "user",
            "password": "pass",
            "table": "orders",
        }
        connector = JDBCConnector(config)
        assert isinstance(connector._adapter, MySQLAdapter)

    def test_init_unknown_adapter_raises(self):
        config = {
            "adapter": "unknown",
            "host": "localhost",
            "database": "testdb",
            "user": "user",
            "password": "pass",
            "table": "orders",
        }
        with pytest.raises(ValueError, match="Unknown adapter"):
            JDBCConnector(config)

    def test_connector_type(self):
        config = {
            "host": "localhost",
            "database": "testdb",
            "user": "user",
            "password": "pass",
            "table": "orders",
        }
        connector = JDBCConnector(config)
        assert connector.connector_type == "JDBCConnector"
