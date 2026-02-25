"""Basic JSON Kafka consumer writing to Delta Lake on ADLS Gen2."""

import pyarrow as pa

from kafka_delta_sink.config import AzureStorageConfig, KafkaConfig, SinkConfig
from kafka_delta_sink.sink import KafkaDeltaSink

schema = pa.schema(
    [
        pa.field("event_id", pa.string()),
        pa.field("event_type", pa.string()),
        pa.field("user_id", pa.string()),
        pa.field("payload", pa.string()),
        pa.field("_kafka_topic", pa.string()),
        pa.field("_kafka_partition", pa.int32()),
        pa.field("_kafka_offset", pa.int64()),
        pa.field("_kafka_timestamp", pa.string()),
    ]
)

kafka_config = KafkaConfig(
    bootstrap_servers="localhost:9092",
    group_id="delta-sink-example",
    auto_offset_reset="earliest",
)

azure_config = AzureStorageConfig(
    account_name="stritulandqas",
    auth_method="service_principal",
    client_id="<your-client-id>",
    client_secret="<your-client-secret>",
    tenant_id="<your-tenant-id>",
)

sink_config = SinkConfig(
    topics=["events.clickstream"],
    delta_table_uri="abfss://datalake@stritulandqas.dfs.core.windows.net/bronze/clickstream",
    batch_size=10_000,
    batch_timeout_seconds=30.0,
    partition_by=["event_type"],
)

sink = KafkaDeltaSink(
    kafka_config=kafka_config,
    azure_config=azure_config,
    sink_config=sink_config,
    schema=schema,
)

if __name__ == "__main__":
    sink.start()
