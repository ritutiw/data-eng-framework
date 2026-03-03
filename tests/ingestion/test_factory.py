import pytest

from data_engineering_framework.ingestion.factory import ConnectorFactory
from data_engineering_framework.ingestion.jdbc.connector import JDBCConnector
from data_engineering_framework.ingestion.kafka.connector import KafkaConnector
from data_engineering_framework.ingestion.registry import ConnectorRegistry


class TestConnectorRegistry:
    def test_kafka_registered(self):
        assert "kafka" in ConnectorRegistry.available()

    def test_jdbc_registered(self):
        assert "jdbc" in ConnectorRegistry.available()

    def test_unknown_connector_raises(self):
        with pytest.raises(KeyError, match="Unknown connector"):
            ConnectorRegistry.get("nonexistent")


class TestConnectorFactory:
    def test_create_kafka_connector(self):
        config = {
            "bootstrap_servers": "localhost:9092",
            "topics": ["test"],
        }
        connector = ConnectorFactory.create("kafka", config)
        assert isinstance(connector, KafkaConnector)

    def test_create_jdbc_connector(self):
        config = {
            "adapter": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "user": "testuser",
            "password": "testpass",
            "table": "test_table",
        }
        connector = ConnectorFactory.create("jdbc", config)
        assert isinstance(connector, JDBCConnector)

    def test_create_unknown_raises(self):
        with pytest.raises(KeyError):
            ConnectorFactory.create("unknown", {})
