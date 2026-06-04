# GoMigrate

**A concurrent, production-grade database migration and backup tool — built for 100M+ record workloads.**

[![Go Version](https://img.shields.io/badge/Go-1.26+-blue)](https://go.dev)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## Table of Contents

1. [Overview](#overview)
2. [Supported Databases](#supported-databases)
3. [Installation](#installation)
4. [Quick Start](#quick-start)
5. [CLI Reference](#cli-reference)
6. [Configuration Reference](#configuration-reference)
7. [Environment Variables](#environment-variables)
8. [Observability](#observability)
9. [Security & Secrets](#security--secrets)
10. [Kubernetes Deployment](#kubernetes-deployment)
11. [Development](#development)

---

## Overview

GoMigrate handles large-scale database operations with:

- **Zero-data-loss backups** — Parquet/NDJSON chunks with SHA-256 manifests
- **Online migration** — concurrent reader/transformer/writer pipeline, no full downtime
- **Resumable operations** — bbolt checkpoint store; a 100M-row job that crashes at 73M restarts from 73M
- **Data integrity** — post-migration row-count and record checksum verification
- **Observability** — Prometheus metrics, structured Zap logging, OpenTelemetry tracing, audit log
- **Data masking** — SHA-256 hashing, redaction, and partial masking for PII columns

---

## Supported Databases

| Database          | Source | Target | Driver                             |
|-------------------|--------|--------|------------------------------------|
| **PostgreSQL**    | ✅     | ✅     | `pgx/v5`                          |
| **MongoDB**       | ✅     | ✅     | `mongo-driver`                    |
| **Cassandra**     | ✅     | ✅     | `gocql`                           |
| **Apache Iceberg**| ✅     | ✅     | REST Catalog + `apache/arrow-go`  |
| **MySQL**         | 🔜     | 🔜     | `go-sql-driver/mysql`             |
| **SQLite**        | 🔜     | 🔜     | `modernc.org/sqlite`              |

---

## Installation

### Prerequisites

- Go 1.26+
- Docker (for integration tests)

### Build from Source

```bash
git clone https://github.com/dinocodesx/gomigrate.git
cd gomigrate

# Copy the example environment file and populate your credentials
cp .env.example .env

make build
```

The binary is placed at `./gomigrate`.

### Docker

```bash
docker pull ghcr.io/dinocodesx/gomigrate:latest
```

---

## Quick Start

### Postgres → Postgres migration

```bash
# 1. Create your config file
cat > config.yaml <<EOF
source:
  type: postgres
  host: localhost
  port: 5432
  database: myapp
  user: migrator
  password: "${PG_PASSWORD}"
  tables: [users]

target:
  type: postgres
  host: new-pg.internal
  port: 5432
  database: myapp_v2
  user: migrator
  password: "${PG_TARGET_PASSWORD}"

concurrency:
  num_readers: 8
  num_writers: 8
  batch_size: 1000

checkpoint:
  path: ./migration.bolt

telemetry:
  log_level: info
  log_format: json
  metrics_addr: ":9090"
EOF

# 2. Run the migration
./gomigrate migrate --config config.yaml

# 3. Check status at any time
./gomigrate status --checkpoint ./migration.bolt
```

### Backup to S3

```bash
cat > backup.yaml <<EOF
source:
  type: postgres
  host: prod-pg.internal
  port: 5432
  database: appdb
  user: backupuser
  password: "${PG_PASSWORD}"
  tables: [users, orders]

backup:
  format: parquet
  compression: zstd
  chunk_size_mb: 512
  storage:
    type: s3
    bucket: company-backups
    prefix: prod/postgres
    region: us-east-1

concurrency:
  num_readers: 16
EOF

./gomigrate backup --config backup.yaml
```

---

## CLI Reference

### Global Flags

| Flag        | Default         | Description                          |
|-------------|-----------------|--------------------------------------|
| `--config`  | `./config.yaml` | Path to the YAML configuration file  |

### Commands

#### `gomigrate migrate`

Migrate data between databases.

```
gomigrate migrate --config <file> [flags]
```

#### `gomigrate backup`

Export a database table to cloud/local storage.

```
gomigrate backup --config <file> [flags]
```

#### `gomigrate restore`

Import data from a backup manifest.

```
gomigrate restore --config <file> --manifest <path>
```

| Flag         | Default           | Description                      |
|--------------|-------------------|----------------------------------|
| `--manifest` | `manifest.json`   | Path to the backup manifest file |

#### `gomigrate verify`

Verify backup integrity (manifest + chunk existence).

```
gomigrate verify --config <file> --manifest <path>
```

#### `gomigrate status`

Print a progress table from a checkpoint file.

```
gomigrate status [--checkpoint <path>]
```

| Flag           | Default              | Description               |
|----------------|----------------------|---------------------------|
| `--checkpoint` | from config or `checkpoint.bolt` | Path to the bbolt checkpoint file |

**Example output:**

```
PARTITION ID   STATUS      ROWS WRITTEN   ERRORS   LAST UPDATED
────────────   ──────      ────────────   ──────   ────────────
users-p0       Done        1250000        0        2025-11-01T00:10:00Z
users-p1       InProgress  873450         3        2025-11-01T00:11:42Z
users-p2       Pending     0              0        2025-11-01T00:00:00Z
```

#### `gomigrate replay`

Re-attempt records from a dead-letter queue file.

```
gomigrate replay --config <file> --failed-file <path>
```

| Flag            | Required | Description                              |
|-----------------|----------|------------------------------------------|
| `--failed-file` | ✅       | Path to the `*_failed.ndjson` DLQ file  |

#### `gomigrate version`

Print the version string.

---

## Configuration Reference

```yaml
# ─── Source Database ───────────────────────────────────────────────────────
source:
  type: postgres          # postgres | mongo | cassandra | iceberg
  host: prod-pg.internal
  port: 5432
  database: appdb
  user: migrator
  password: "${PG_PASSWORD}"   # env-var interpolation supported
  tables:
    - users
    - orders
  params:                  # engine-specific extra parameters
    sslmode: require

# ─── Target Database ───────────────────────────────────────────────────────
target:
  type: cassandra
  hosts:
    - cass1.internal
    - cass2.internal
  keyspace: prod
  params:
    consistency: LOCAL_QUORUM

# ─── Concurrency ───────────────────────────────────────────────────────────
concurrency:
  num_readers: 16          # goroutines reading from source
  num_transformers: 8      # goroutines doing schema mapping
  num_writers: 16          # goroutines writing to target
  batch_size: 1000         # records per write batch
  batch_timeout: 5s        # max wait before flushing partial batch
  rate_limit_rps: 0        # 0 = unlimited
  flush_every_n_batches: 10

# ─── Migration ─────────────────────────────────────────────────────────────
migration:
  schema_mapping_file: configs/mappings/pg_to_cassandra.yaml
  conflict_strategy: upsert    # upsert | skip | error
  verify_after: true
  verify_sample_pct: 1.0
  masking:
    - column: email
      strategy: sha256          # sha256 | redact | partial
    - column: phone
      strategy: redact

# ─── Backup ────────────────────────────────────────────────────────────────
backup:
  format: parquet               # parquet | ndjson
  compression: zstd
  chunk_size_mb: 512
  storage:
    type: s3                    # local | s3 | gcs
    bucket: company-db-backups
    prefix: prod/postgres/users
    region: us-east-1
  retention:
    keep_last: 7

# ─── Checkpoint ────────────────────────────────────────────────────────────
checkpoint:
  path: /var/lib/gomigrate/migration.bolt
  flush_every_n_batches: 10

# ─── Telemetry ─────────────────────────────────────────────────────────────
telemetry:
  log_level: info               # debug | info | warn | error
  log_format: json              # json | console
  metrics_addr: ":9090"         # Prometheus scrape endpoint
  tracing_endpoint: "http://otel-collector:4318"   # OTLP/HTTP
```

### Schema Mapping File

```yaml
# configs/mappings/pg_to_cassandra.yaml
mappings:
  - source_column: user_id
    target_column: user_id
    transform: none

  - source_column: created_at
    target_column: created_at
    transform: to_unix_ms       # timestamptz → bigint (ms since epoch)

  - source_column: metadata
    target_column: metadata_json
    transform: to_json_string   # jsonb → text
```

**Available transforms:** `none`, `to_json_string`, `from_json_string`, `to_unix_ms`, `from_unix_ms`, `to_upper`, `to_lower`, `uuid_to_string`, `string_to_uuid`, `flatten_json`

---

## Environment Variables

Any config key can be overridden with an environment variable using the pattern `GOMIGRATE_<KEY>` (nested keys use `_` separators):

| Environment Variable                          | Config Key                          | Example                         |
|-----------------------------------------------|-------------------------------------|---------------------------------|
| `GOMIGRATE_SOURCE_HOST`                       | `source.host`                       | `prod-pg.internal`              |
| `GOMIGRATE_SOURCE_PASSWORD`                   | `source.password`                   | `s3cr3t`                        |
| `GOMIGRATE_TARGET_HOST`                       | `target.host`                       | `new-pg.internal`               |
| `GOMIGRATE_CONCURRENCY_NUM_READERS`           | `concurrency.num_readers`           | `32`                            |
| `GOMIGRATE_CONCURRENCY_BATCH_SIZE`            | `concurrency.batch_size`            | `5000`                          |
| `GOMIGRATE_CHECKPOINT_PATH`                   | `checkpoint.path`                   | `/tmp/my.bolt`                  |
| `GOMIGRATE_TELEMETRY_LOG_LEVEL`               | `telemetry.log_level`               | `debug`                         |
| `GOMIGRATE_TELEMETRY_METRICS_ADDR`            | `telemetry.metrics_addr`            | `:9090`                         |
| `GOMIGRATE_TELEMETRY_TRACING_ENDPOINT`        | `telemetry.tracing_endpoint`        | `http://jaeger:4318`            |

---

## Observability

### Prometheus Metrics

Exposed at `telemetry.metrics_addr` (default `:9090/metrics`):

| Metric                                    | Type      | Description                         |
|-------------------------------------------|-----------|-------------------------------------|
| `gomigrate_records_read_total`            | Counter   | Records read from source            |
| `gomigrate_records_written_total`         | Counter   | Records written to target           |
| `gomigrate_records_failed_total`          | Counter   | Records dead-lettered               |
| `gomigrate_partitions_total`              | Gauge     | Total partitions planned            |
| `gomigrate_partitions_done`               | Gauge     | Partitions completed                |
| `gomigrate_batch_write_duration_seconds`  | Histogram | Write latency per batch             |

Import the pre-built Grafana dashboard from [`docs/grafana-dashboard.json`](docs/grafana-dashboard.json).

### OpenTelemetry Tracing

Set `telemetry.tracing_endpoint` to an OTLP/HTTP collector (e.g., Jaeger, Tempo). A root span is created per migration, with child spans per partition.

### Audit Log

An append-only JSON-lines file (`audit.jsonl`) is written alongside the checkpoint file. Each line contains:

```json
{
  "ts": "2025-11-01T00:00:00Z",
  "user": "deployer",
  "hostname": "worker-1",
  "operation": "migrate",
  "source": "postgres/prod-pg.internal",
  "target": "cassandra/cass1.internal",
  "tables": ["users"],
  "start_time": "2025-11-01T00:00:00Z",
  "end_time": "2025-11-01T01:30:00Z",
  "outcome": "success",
  "row_count": 100000000,
  "config_hash": "abc123..."
}
```

---

## Security & Secrets

### Credentials

Never hardcode passwords. Use one of:

- **Environment variables** (default): `source.password: "${PG_PASSWORD}"`
- **`.env` file**: `cp .env.example .env` — loaded automatically
- **HashiCorp Vault**: configure `secrets.provider: vault` with AppRole credentials

### TLS

- PostgreSQL: `params.sslmode: require` (default)
- MongoDB: `params.tls: "true"`
- Cassandra: `params.ssl: "true"`

A `WARN` is logged/printed if `sslmode=disable` is detected.

---

## Kubernetes Deployment

See [`configs/k8s/`](configs/k8s/) for ready-to-use manifests:

```bash
# Create prerequisites
kubectl create configmap gomigrate-config --from-file=config.yaml
kubectl create secret generic gomigrate-secrets \
  --from-literal=PG_SOURCE_PASSWORD='...'

# One-shot migration
kubectl apply -f configs/k8s/job.yaml

# Scheduled backup (daily at 02:00 UTC)
kubectl apply -f configs/k8s/cronjob.yaml
```

---

## Development

### Running Tests

```bash
# Unit tests
make test

# Integration tests (requires Docker)
make test-integration

# End-to-end crash-and-resume test
go test -tags=e2e ./test/e2e/... -v -timeout 5m
```

### Linting

```bash
make lint
```

### Local Dev Databases

```bash
cp .env.example .env
docker compose up -d
```

### Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## License

MIT — see [LICENSE](LICENSE).
