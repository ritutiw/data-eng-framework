# Configuration

data-engineering-framework is configured via YAML config files or environment variables.

## Kafka Source Config

```yaml
pipeline_name: kafka-clickstream-bronze
connector_type: kafka

connector:
  bootstrap_servers: "broker1:9092,broker2:9092"
  group_id: "data-eng-framework"
  auto_offset_reset: "earliest"
  security_protocol: "PLAINTEXT"
  topics:
    - "events.clickstream"
    - "events.transactions"
  batch_size: 10000
  batch_timeout_seconds: 30.0

azure:
  account_name: "sample-storage-account"
  auth_method: "service_principal"
  client_id: "sample-client-id"
  client_secret: "sample-client-secret"
  tenant_id: "sample-tenant-id"

sink:
  delta_table_uri: "abfss://datalake@sample-storage-account.dfs.core.windows.net/bronze/events"
  write_mode: "append"
  partition_by:
    - "event_type"

schema:
  - name: "event_id"
    type: "string"
  - name: "event_type"
    type: "string"
  - name: "payload"
    type: "string"
```

## JDBC Source Config

```yaml
pipeline_name: jdbc-orders-bronze
connector_type: jdbc

connector:
  adapter: postgresql          # mysql | postgresql | oracle
  host: "sample-db-host"
  port: 5432                   # 3306 for MySQL, 1521 for Oracle
  database: "sample-database"
  user: "sample-user"
  password: "sample-password"
  table: "orders"
  columns:                     # optional, default: all columns
    - "order_id"
    - "customer_id"
    - "amount"
  watermark_column: "created_at"   # optional, for incremental loads
  watermark_value: "2024-01-01"    # optional, last watermark value
  fetch_size: 10000
  partition_column: "id"           # optional, for parallel reads
  num_partitions: 4
  lower_bound: 0
  upper_bound: 1000000

azure:
  account_name: "sample-storage-account"
  auth_method: "account_key"
  account_key: "sample-account-key"

sink:
  delta_table_uri: "abfss://datalake@sample-storage-account.dfs.core.windows.net/bronze/orders"
  write_mode: "append"

schema:
  - name: "order_id"
    type: "string"
  - name: "amount"
    type: "float64"
```

## Environment Variables

### Kafka Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker addresses |
| `KAFKA_GROUP_ID` | `data-eng-framework` | Consumer group ID |
| `KAFKA_AUTO_OFFSET_RESET` | `earliest` | Where to start consuming |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | Security protocol |
| `KAFKA_SASL_MECHANISM` | None | SASL mechanism |
| `KAFKA_SASL_USERNAME` | None | SASL username |
| `KAFKA_SASL_PASSWORD` | None | SASL password |

### Azure Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `AZURE_ACCOUNT_NAME` | | Azure storage account name |
| `AZURE_AUTH_METHOD` | `service_principal` | Auth method |
| `AZURE_CLIENT_ID` | None | Service principal client ID |
| `AZURE_CLIENT_SECRET` | None | Service principal secret |
| `AZURE_TENANT_ID` | None | Azure AD tenant ID |
| `AZURE_ACCOUNT_KEY` | None | Storage account access key |
| `AZURE_SAS_TOKEN` | None | SAS token |

### Sink Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `SINK_TOPICS` | `["default-topic"]` | Kafka topics (JSON array) |
| `SINK_DELTA_TABLE_URI` | | Delta table ADLS URI |
| `SINK_BATCH_SIZE` | `10000` | Records per batch |
| `SINK_BATCH_TIMEOUT_SECONDS` | `60.0` | Max seconds before flush |
| `SINK_WRITE_MODE` | `append` | Delta write mode |
| `SINK_PARTITION_BY` | `[]` | Partition columns (JSON array) |

### JDBC Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `JDBC_DIALECT` | `postgresql` | Database adapter type |
| `JDBC_HOST` | `localhost` | Database host |
| `JDBC_PORT` | `5432` | Database port |
| `JDBC_DATABASE` | | Database name |
| `JDBC_USER` | | Database user |
| `JDBC_PASSWORD` | | Database password |
| `JDBC_TABLE` | | Source table |
| `JDBC_FETCH_SIZE` | `10000` | Rows per fetch |

### Logging

| Variable | Default | Description |
|----------|---------|-------------|
| `APPLICATIONINSIGHTS_CONNECTION_STRING` | None | Azure Application Insights connection string |

## JDBC Adapters

| Adapter | JDBC Driver | Default Port |
|---------|-------------|-------------|
| `mysql` | `com.mysql.cj.jdbc.Driver` | 3306 |
| `postgresql` | `org.postgresql.Driver` | 5432 |
| `oracle` | `oracle.jdbc.OracleDriver` | 1521 |

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
