"""Shared test fixtures."""

import pyarrow as pa
import pytest


@pytest.fixture
def sample_schema():
    return pa.schema(
        [
            pa.field("event_id", pa.string()),
            pa.field("event_type", pa.string()),
            pa.field("payload", pa.string()),
            pa.field("_kafka_topic", pa.string()),
            pa.field("_kafka_partition", pa.int32()),
            pa.field("_kafka_offset", pa.int64()),
            pa.field("_kafka_timestamp", pa.string()),
        ]
    )


@pytest.fixture
def sample_records():
    return [
        {
            "event_id": "evt-001",
            "event_type": "click",
            "payload": '{"page": "/home"}',
            "_kafka_topic": "test-events",
            "_kafka_partition": 0,
            "_kafka_offset": 0,
            "_kafka_timestamp": "2026-02-23T10:00:00+00:00",
        },
        {
            "event_id": "evt-002",
            "event_type": "view",
            "payload": '{"page": "/about"}',
            "_kafka_topic": "test-events",
            "_kafka_partition": 0,
            "_kafka_offset": 1,
            "_kafka_timestamp": "2026-02-23T10:00:01+00:00",
        },
    ]


@pytest.fixture
def tmp_delta_path(tmp_path):
    return str(tmp_path / "test_delta_table")
