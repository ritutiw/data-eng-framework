# Data Engineering Framework

Enterprise-grade lakehouse framework implementing medallion architecture for multi-source data ingestion, processing, and consumption.

## Architecture

```
Sources → Bronze → Silver → Gold → Platinum
          (Raw)   (Curated) (Business) (AI/ML)
                                ↓
                          DatabricksSql (Analytics)
                          Neo4j (Time Series)
```

```
┌───────────────┐     ┌──────────────────────────┐     ┌──────────────────────────┐
│  Kafka Topics │────>│                          │────>│  Delta Lake on ADLS Gen2 │
└───────────────┘     │  data-eng-framework      │     │  (Bronze / Silver / Gold)│
┌───────────────┐     │  (PySpark + delta-rs)    │     └──────────────────────────┘
│  MySQL        │────>│                          │
│  PostgreSQL   │     │  Design Patterns:        │
│  OracleDB     │────>│  Factory, Registry,      │
└───────────────┘     │  Strategy, Template      │
                      └──────────────────────────┘
```

## Technology Stack

- **Orchestration**: Azure Data Factory, Databricks Workflows
- **Processing**: Apache Spark (PySpark), Delta Lake
- **Governance**: Unity Catalog
- **Streaming**: Kafka, Azure Event Hub
- **Analytics**: DatabricksSql
- **Time Series**: Neo4j
- **Cloud**: Azure (adaptable to AWS/GCP)

## Project Structure

```
data-engineering-framework/
├── config/                          # YAML configurations
│   ├── kafka_sources.yaml
│   ├── jdbc_sources.yaml
│   └── pipeline.yaml
├── ingestion/                       # Source connectors (Bronze)
│   ├── base.py                      # BaseConnector (Template Method)
│   ├── registry.py                  # ConnectorRegistry (Registry pattern)
│   ├── factory.py                   # ConnectorFactory (Factory pattern)
│   ├── kafka/                       # Kafka connector
│   │   ├── connector.py
│   │   ├── consumer.py
│   │   └── serialization/           # JSON + Avro deserializers
│   └── jdbc/                        # JDBC connectors (PySpark JDBC)
│       ├── connector.py             # Strategy pattern with adapters
│       ├── adapters.py              # MySQL, PostgreSQL, Oracle adapters
│       └── query_builder.py
├── processing/                      # Silver layer transformations
│   ├── cleansing.py                 # Dedup, null handling
│   ├── quality.py                   # Data quality validation
│   └── scd.py                       # SCD Type 2 tracking
├── consumption/                     # Gold layer aggregations
│   └── aggregator.py
├── ml/                              # Platinum layer
│   └── feature_store.py
├── writers/                         # Output writers
│   └── delta_writer.py              # Delta Lake writer (delta-rs)
├── orchestration/                   # Pipeline definitions
│   └── pipeline.py
├── monitoring/                      # Metrics collection
│   └── metrics.py
├── common/                          # Shared utilities
│   ├── config.py                    # Pydantic settings
│   ├── logger.py                    # Centralized logger + Application Insights
│   ├── storage.py                   # Azure ADLS storage helpers
│   └── transforms.py               # Flatten, rename, select
└── tests/                           # Unit and integration tests
```

## Design Patterns

| Pattern | Location | Purpose |
|---------|----------|---------|
| Template Method | `ingestion/base.py` | `connect()` → `extract()` → `close()` lifecycle |
| Factory | `ingestion/factory.py` | `ConnectorFactory.create("kafka", config)` |
| Registry | `ingestion/registry.py` | `@ConnectorRegistry.register("kafka")` decorator |
| Strategy | `ingestion/jdbc/adapters.py` | Pluggable DB adapters (MySQL, PostgreSQL, Oracle) |

## Connectors

### Kafka Connector
Primary ingestion mechanism for streaming data:
- confluent-kafka consumer with at-least-once delivery
- JSON and Avro deserialization (Schema Registry)
- Configurable batching by count or timeout
- Graceful shutdown with signal handling

### JDBC Connectors (PySpark JDBC)
Batch ingestion from relational databases:
- **MySQL** — `com.mysql.cj.jdbc.Driver`
- **PostgreSQL** — `org.postgresql.Driver`
- **OracleDB** — `oracle.jdbc.OracleDriver`
- Full and incremental loads with watermark columns
- Parallel reads via partition column

## Quick Start

### Install

```bash
uv add data-engineering-framework
```

### Run with YAML config

```bash
data-eng-framework -c config/kafka_sources.yaml
```

### Run with Docker

```bash
docker-compose up
```

## Example Configs

### Kafka Source
```yaml
pipeline_name: kafka-clickstream-bronze
connector_type: kafka

connector:
  bootstrap_servers: "broker:9092"
  group_id: "data-eng-framework"
  topics: ["events.clickstream"]
  batch_size: 10000

azure:
  account_name: "sample-storage-account"
  auth_method: "service_principal"

sink:
  delta_table_uri: "abfss://datalake@account.dfs.core.windows.net/bronze/clickstream"
  partition_by: ["event_type"]
```

### JDBC Source (PostgreSQL)
```yaml
pipeline_name: jdbc-orders-bronze
connector_type: jdbc

connector:
  adapter: postgresql
  host: "sample-db-host"
  port: 5432
  database: "sample-database"
  user: "sample-user"
  password: "sample-password"
  table: "orders"
  watermark_column: "created_at"
```

## Data Quality

Built-in validation framework with configurable rules:

```python
from data_engineering_framework.processing.quality import QualityValidator, RuleSeverity

validator = QualityValidator()
validator.add_not_null("customer_id")
validator.add_unique("order_id", severity=RuleSeverity.ERROR)

results = validator.validate(table)
if validator.has_errors(results):
    raise ValueError("Data quality check failed")
```

## Logging

Centralized logger with Azure Application Insights integration:

```python
from data_engineering_framework.common.logger import get_logger

logger = get_logger(__name__)
```

Set `APPLICATIONINSIGHTS_CONNECTION_STRING` environment variable to enable Application Insights.

## Development

```bash
git clone https://github.com/ritutiw/kafka-delta-sink.git
cd kafka-delta-sink

uv sync --all-extras

uv run pytest

uv run ruff check src/ tests/

docker-compose up -d kafka postgresql mysql
```

## Documentation

- [Configuration Reference](docs/configuration.md)
- [Authentication Guide](docs/authentication.md)

## License

Apache License 2.0
