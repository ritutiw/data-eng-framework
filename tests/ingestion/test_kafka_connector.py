from unittest.mock import MagicMock, patch

from data_engineering_framework.ingestion.kafka.connector import KafkaConnector


class TestKafkaConnector:
    def test_init_defaults(self):
        config = {"bootstrap_servers": "localhost:9092", "topics": ["test"]}
        connector = KafkaConnector(config)
        assert connector._batch_size == 10_000
        assert connector._batch_timeout == 60.0
        assert connector._running is False

    def test_stats_initial(self):
        config = {"bootstrap_servers": "localhost:9092", "topics": ["test"]}
        connector = KafkaConnector(config)
        stats = connector.stats
        assert stats["messages_processed"] == 0
        assert stats["batches_yielded"] == 0
        assert stats["running"] is False

    @patch("data_engineering_framework.ingestion.kafka.connector.KafkaConsumerWrapper")
    def test_close(self, mock_consumer_cls):
        config = {"bootstrap_servers": "localhost:9092", "topics": ["test"]}
        connector = KafkaConnector(config)
        mock_consumer = MagicMock()
        connector._consumer = mock_consumer
        connector._connected = True

        connector.close()

        mock_consumer.close.assert_called_once()
        assert connector._connected is False

    def test_connector_type(self):
        config = {"bootstrap_servers": "localhost:9092", "topics": ["test"]}
        connector = KafkaConnector(config)
        assert connector.connector_type == "KafkaConnector"
