# kafka-delta-sink

Stream data from Apache Kafka to Delta Lake on Azure ADLS Gen2 — **no Spark required**.

A lightweight Python-based sink that consumes Kafka messages and writes them as Delta Lake tables using [delta-rs](https://github.com/delta-io/delta-rs), with ACID transactions and zero JVM dependency.

## Architecture

```
┌──────────┐     ┌──────────────────┐     ┌─────────────────────────────┐
│  Kafka   │────>│ kafka-delta-sink │────>│  Delta Lake on ADLS Gen2    │
│  Topics  │     │  (Python)        │     │  (Parquet + Delta Log)      │
└──────────┘     └──────────────────┘     └─────────────────────────────┘
                   │
                   ├─ confluent-kafka (consumer)
                   ├─ delta-rs (writer, no Spark)
                   └─ pyarrow (in-memory format)
```

## Features

- **No Spark / No JVM** — Uses delta-rs (Rust) Python bindings (~50MB vs multi-GB Spark)
- **At-least-once delivery** — Offsets committed only after successful Delta write
- **Configurable batching** — Flush by record count or time timeout
- **Multiple auth methods** — Service Principal, Account Key, SAS Token, Azure CLI
- **Schema evolution** — Supports Delta Lake schema merge
- **JSON & Avro** — Pluggable deserializers (Avro via Confluent Schema Registry)
- **Docker ready** — Slim container image for Kubernetes deployment
- **Table maintenance** — Built-in OPTIMIZE and VACUUM support

## Quick Start

### Install

```bash
uv add kafka-delta-sink
```

Or with pip:
```bash
pip install kafka-delta-sink
```

For Avro support:
```bash
uv add 'kafka-delta-sink[avro]'
```

### Run with YAML config

```bash
kafka-delta-sink --config config.yaml
```

### Run with environment variables

```bash
export KAFKA_BOOTSTRAP_SERVERS=broker:9092
export AZURE_ACCOUNT_NAME=mystorageaccount
export AZURE_AUTH_METHOD=service_principal
export AZURE_CLIENT_ID=<client-id>
export AZURE_CLIENT_SECRET=<client-secret>
export AZURE_TENANT_ID=<tenant-id>
export SINK_TOPICS='["my-topic"]'
export SINK_DELTA_TABLE_URI=abfss://container@mystorageaccount.dfs.core.windows.net/bronze/events

kafka-delta-sink
```

### Run with Docker

```bash
docker-compose up
```

## Example Config (YAML)

```yaml
kafka:
  bootstrap_servers: "broker:9092"
  group_id: "kafka-delta-sink"
  auto_offset_reset: "earliest"

azure:
  account_name: "mystorageaccount"
  auth_method: "service_principal"
  client_id: "<client-id>"
  client_secret: "<client-secret>"
  tenant_id: "<tenant-id>"

sink:
  topics: ["events.clickstream"]
  delta_table_uri: "abfss://datalake@mystorageaccount.dfs.core.windows.net/bronze/clickstream"
  batch_size: 10000
  batch_timeout_seconds: 30
  partition_by: ["event_type"]

schema:
  - name: "event_id"
    type: "string"
  - name: "event_type"
    type: "string"
  - name: "payload"
    type: "string"
```

## How It Works

1. **Consume** — Polls Kafka topics using confluent-kafka
2. **Deserialize** — Parses JSON (or Avro) messages, enriches with Kafka metadata
3. **Buffer** — Collects records in memory until batch size or timeout is reached
4. **Write** — Converts buffer to PyArrow Table and writes to Delta Lake via delta-rs
5. **Commit** — Commits Kafka offsets only after successful Delta write (at-least-once)

## Documentation

- [Configuration Reference](docs/configuration.md)
- [Authentication Guide](docs/authentication.md)

## Development

```bash
git clone https://github.com/ritutiw/kafka-delta-sink.git
cd kafka-delta-sink

uv sync --all-extras

uv run pytest

uv run ruff check src/ tests/

docker-compose up -d kafka azurite
```

## License

Apache License 2.0
