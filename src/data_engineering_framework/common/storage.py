"""Azure ADLS Gen2 storage options builder."""

from __future__ import annotations

from data_engineering_framework.common.config import AzureStorageConfig


def build_storage_options(config: AzureStorageConfig) -> dict:
    return config.to_storage_options()


def build_storage_options_from_env() -> dict:
    config = AzureStorageConfig()
    return config.to_storage_options()
