from unittest.mock import MagicMock, patch

from data_engineering_framework.ingestion.kafka.connector import KafkaConnector


class TestKafkaConnectorUnit:
    def test_stats_initial(self):
        config = {"bootstrap_servers": "localhost:9092", "topics": ["test"]}
        connector = KafkaConnector(config)
        stats = connector.stats
        assert stats["messages_processed"] == 0
        assert stats["batches_yielded"] == 0
        assert stats["running"] is False

    @patch("data_engineering_framework.ingestion.kafka.connector.KafkaConsumerWrapper")
    def test_close_calls_consumer_close(self, mock_consumer_cls):
        config = {"bootstrap_servers": "localhost:9092", "topics": ["test"]}
        connector = KafkaConnector(config)
        mock_consumer = MagicMock()
        connector._consumer = mock_consumer
        connector._connected = True

        connector.close()

        mock_consumer.close.assert_called_once()
        assert connector._connected is False
