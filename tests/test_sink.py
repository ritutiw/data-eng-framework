"""Tests for the KafkaDeltaSink (unit tests with mocks)."""

from unittest.mock import MagicMock, patch

from kafka_delta_sink.config import AzureStorageConfig, KafkaConfig, SinkConfig
from kafka_delta_sink.sink import KafkaDeltaSink


def make_sink(tmp_path, schema):
    kafka_config = KafkaConfig(bootstrap_servers="localhost:9092")
    azure_config = AzureStorageConfig(
        account_name="testaccount", auth_method="account_key", account_key="testkey"
    )
    sink_config = SinkConfig(
        topics=["test"],
        delta_table_uri=str(tmp_path / "delta"),
        batch_size=2,
        batch_timeout_seconds=60.0,
    )
    return KafkaDeltaSink(
        kafka_config=kafka_config,
        azure_config=azure_config,
        sink_config=sink_config,
        schema=schema,
    )


class TestKafkaDeltaSinkUnit:
    def test_stats_initial(self, sample_schema, tmp_path):
        sink = make_sink(tmp_path, sample_schema)
        stats = sink.stats
        assert stats["messages_processed"] == 0
        assert stats["batches_written"] == 0
        assert stats["buffer_size"] == 0
        assert stats["running"] is False

    @patch("kafka_delta_sink.sink.KafkaConsumerWrapper")
    def test_flush_writes_and_commits(
        self, mock_consumer_cls, sample_schema, sample_records, tmp_path
    ):
        sink = make_sink(tmp_path, sample_schema)

        mock_consumer = MagicMock()
        sink._consumer = mock_consumer

        sink._buffer = list(sample_records)
        sink._flush()

        mock_consumer.commit.assert_called_once()
        assert sink._batches_written == 1
        assert len(sink._buffer) == 0

    @patch("kafka_delta_sink.sink.KafkaConsumerWrapper")
    def test_flush_empty_buffer_noop(self, mock_consumer_cls, sample_schema, tmp_path):
        sink = make_sink(tmp_path, sample_schema)
        mock_consumer = MagicMock()
        sink._consumer = mock_consumer

        sink._flush()

        mock_consumer.commit.assert_not_called()
        assert sink._batches_written == 0
