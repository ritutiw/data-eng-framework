# Configuration

kafka-delta-sink can be configured via a YAML file or environment variables.

## YAML Config File

```yaml
kafka:
  bootstrap_servers: "broker1:9092,broker2:9092"
  group_id: "kafka-delta-sink"
  auto_offset_reset: "earliest"
  security_protocol: "PLAINTEXT"
  # Optional SASL
  # sasl_mechanism: "PLAIN"
  # sasl_username: "user"
  # sasl_password: "pass"

azure:
  account_name: "stedhenis01landqas"
  auth_method: "service_principal"  # service_principal | account_key | sas_token | cli
  client_id: "<client-id>"
  client_secret: "<client-secret>"
  tenant_id: "<tenant-id>"

sink:
  topics:
    - "events.clickstream"
    - "events.transactions"
  delta_table_uri: "abfss://datalake@stedhenis01landqas.dfs.core.windows.net/bronze/events"
  batch_size: 10000
  batch_timeout_seconds: 30.0
  write_mode: "append"
  partition_by:
    - "event_type"

schema:
  - name: "event_id"
    type: "string"
  - name: "event_type"
    type: "string"
  - name: "user_id"
    type: "string"
  - name: "payload"
    type: "string"
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker addresses |
| `KAFKA_GROUP_ID` | `kafka-delta-sink` | Consumer group ID |
| `KAFKA_AUTO_OFFSET_RESET` | `earliest` | Where to start consuming |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | Security protocol |
| `KAFKA_SASL_MECHANISM` | None | SASL mechanism (PLAIN, SCRAM-SHA-256, etc.) |
| `KAFKA_SASL_USERNAME` | None | SASL username |
| `KAFKA_SASL_PASSWORD` | None | SASL password |
| `AZURE_ACCOUNT_NAME` | | Azure storage account name |
| `AZURE_AUTH_METHOD` | `service_principal` | Auth method |
| `AZURE_CLIENT_ID` | None | Service principal client ID |
| `AZURE_CLIENT_SECRET` | None | Service principal secret |
| `AZURE_TENANT_ID` | None | Azure AD tenant ID |
| `AZURE_ACCOUNT_KEY` | None | Storage account access key |
| `AZURE_SAS_TOKEN` | None | SAS token |
| `SINK_TOPICS` | `["default-topic"]` | Kafka topics (JSON array) |
| `SINK_DELTA_TABLE_URI` | | Delta table ADLS URI |
| `SINK_BATCH_SIZE` | `10000` | Records per batch |
| `SINK_BATCH_TIMEOUT_SECONDS` | `60.0` | Max seconds before flush |
| `SINK_WRITE_MODE` | `append` | Delta write mode |
| `SINK_PARTITION_BY` | `[]` | Partition columns (JSON array) |

## Schema Types

Supported PyArrow types in the `schema` config section:

| Config Type | PyArrow Type |
|-------------|-------------|
| `string` | `pa.string()` |
| `int32` | `pa.int32()` |
| `int64` | `pa.int64()` |
| `float32` | `pa.float32()` |
| `float64` | `pa.float64()` |
| `bool` | `pa.bool_()` |
| `timestamp` | `pa.timestamp("us", tz="UTC")` |
