from __future__ import annotations

from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings


class KafkaConfig(BaseSettings):
    model_config = {"env_prefix": "KAFKA_"}

    bootstrap_servers: str = "localhost:9092"
    group_id: str = "data-eng-framework"
    auto_offset_reset: str = "earliest"
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None

    def to_confluent_config(self) -> dict:
        conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "security.protocol": self.security_protocol,
        }
        if self.sasl_mechanism:
            conf["sasl.mechanism"] = self.sasl_mechanism
        if self.sasl_username:
            conf["sasl.username"] = self.sasl_username
        if self.sasl_password:
            conf["sasl.password"] = self.sasl_password
        return conf


class AzureStorageConfig(BaseSettings):
    model_config = {"env_prefix": "AZURE_"}

    account_name: str = ""
    auth_method: str = "service_principal"
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    tenant_id: Optional[str] = None
    account_key: Optional[str] = None
    sas_token: Optional[str] = None

    def to_storage_options(self) -> dict:
        opts: dict = {"account_name": self.account_name}
        match self.auth_method:
            case "service_principal":
                if self.client_id:
                    opts["client_id"] = self.client_id
                if self.client_secret:
                    opts["client_secret"] = self.client_secret
                if self.tenant_id:
                    opts["authority_id"] = self.tenant_id
            case "account_key":
                if self.account_key:
                    opts["account_key"] = self.account_key
            case "sas_token":
                if self.sas_token:
                    opts["sas_key"] = self.sas_token
            case "cli":
                opts["use_azure_cli"] = "true"
        return opts


class JDBCConfig(BaseSettings):
    model_config = {"env_prefix": "JDBC_"}

    dialect: str = "postgresql"
    host: str = "localhost"
    port: int = 5432
    database: str = ""
    user: str = ""
    password: str = ""
    table: str = ""
    columns: Optional[list[str]] = None
    watermark_column: Optional[str] = None
    watermark_value: Optional[str] = None
    fetch_size: int = 10_000

    def to_connector_config(self) -> dict:
        return self.model_dump()


class SinkConfig(BaseSettings):
    model_config = {"env_prefix": "SINK_"}

    topics: list[str] = Field(default_factory=lambda: ["default-topic"])
    delta_table_uri: str = ""
    batch_size: int = 10_000
    batch_timeout_seconds: float = 60.0
    partition_by: list[str] = Field(default_factory=list)
    write_mode: str = "append"
    schema_mode: Optional[str] = None
