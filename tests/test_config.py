"""Tests for configuration classes."""

from data_engineering_framework.common.config import (
    AzureStorageConfig,
    JDBCConfig,
    KafkaConfig,
    SinkConfig,
)


class TestKafkaConfig:
    def test_defaults(self):
        cfg = KafkaConfig()
        assert cfg.bootstrap_servers == "localhost:9092"
        assert cfg.group_id == "data-eng-framework"

    def test_to_confluent_config(self):
        cfg = KafkaConfig(bootstrap_servers="broker:9092", group_id="my-group")
        result = cfg.to_confluent_config()
        assert result["bootstrap.servers"] == "broker:9092"
        assert result["group.id"] == "my-group"
        assert result["enable.auto.commit"] is False
        assert result["enable.auto.offset.store"] is False

    def test_sasl_config(self):
        cfg = KafkaConfig(
            sasl_mechanism="PLAIN",
            sasl_username="user",
            sasl_password="pass",
        )
        result = cfg.to_confluent_config()
        assert result["sasl.mechanism"] == "PLAIN"
        assert result["sasl.username"] == "user"
        assert result["sasl.password"] == "pass"


class TestAzureStorageConfig:
    def test_service_principal(self):
        cfg = AzureStorageConfig(
            account_name="myaccount",
            auth_method="service_principal",
            client_id="cid",
            client_secret="csecret",
            tenant_id="tid",
        )
        opts = cfg.to_storage_options()
        assert opts["account_name"] == "myaccount"
        assert opts["client_id"] == "cid"
        assert opts["client_secret"] == "csecret"
        assert opts["authority_id"] == "tid"

    def test_account_key(self):
        cfg = AzureStorageConfig(
            account_name="myaccount",
            auth_method="account_key",
            account_key="abc123",
        )
        opts = cfg.to_storage_options()
        assert opts["account_key"] == "abc123"

    def test_sas_token(self):
        cfg = AzureStorageConfig(
            account_name="myaccount",
            auth_method="sas_token",
            sas_token="sv=2021&sig=abc",
        )
        opts = cfg.to_storage_options()
        assert opts["sas_key"] == "sv=2021&sig=abc"

    def test_cli_auth(self):
        cfg = AzureStorageConfig(
            account_name="myaccount",
            auth_method="cli",
        )
        opts = cfg.to_storage_options()
        assert opts["use_azure_cli"] == "true"


class TestSinkConfig:
    def test_defaults(self):
        cfg = SinkConfig()
        assert cfg.batch_size == 10_000
        assert cfg.batch_timeout_seconds == 60.0
        assert cfg.write_mode == "append"

    def test_custom_values(self):
        cfg = SinkConfig(
            topics=["topic-a", "topic-b"],
            delta_table_uri="abfss://c@a.dfs.core.windows.net/t",
            batch_size=5000,
            partition_by=["event_type"],
        )
        assert cfg.topics == ["topic-a", "topic-b"]
        assert cfg.batch_size == 5000
        assert cfg.partition_by == ["event_type"]


class TestJDBCConfig:
    def test_defaults(self):
        cfg = JDBCConfig()
        assert cfg.dialect == "postgresql"
        assert cfg.port == 5432
        assert cfg.fetch_size == 10_000

    def test_to_connector_config(self):
        cfg = JDBCConfig(host="dbhost", database="mydb", user="u", password="p", table="t")
        d = cfg.to_connector_config()
        assert d["host"] == "dbhost"
        assert d["database"] == "mydb"
