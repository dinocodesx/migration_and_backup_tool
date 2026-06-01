# GoMigrate — Database Migration & Backup Tool

### A Concurrent, Production-Grade Go Tool for 100M+ Record Workloads

---

## Table of Contents

1. [Project Vision & Goals](#1-project-vision--goals)
2. [High-Level Architecture](#2-high-level-architecture)
3. [Repository Layout](#3-repository-layout)
4. [Core Design Principles](#4-core-design-principles)
5. [Concurrency Model](#5-concurrency-model)
6. [Database Adapters](#6-database-adapters)
7. [Migration Engine](#7-migration-engine)
8. [Backup Engine](#8-backup-engine)
9. [Checkpoint & Resume System](#9-checkpoint--resume-system)
10. [Configuration & CLI](#10-configuration--cli)
11. [Observability — Metrics, Logging & Tracing](#11-observability--metrics-logging--tracing)
12. [Error Handling & Retry Strategy](#12-error-handling--retry-strategy)
13. [Security](#13-security)
14. [Testing Strategy](#14-testing-strategy)
15. [Phase-by-Phase Delivery Plan](#15-phase-by-phase-delivery-plan)
16. [Open Questions & Risk Register](#16-open-questions--risk-register)

---

## 1. Project Vision & Goals

### Setup & Credentials

Before running the tool or the development environment, ensure you have a `.env` file in the root directory. You can use `.env.example` as a template:

```bash
cp .env.example .env
```

The `docker-compose.yml` file and the application itself reference these variables. In production, it is recommended to use an external secret manager (e.g., HashiCorp Vault, AWS Secrets Manager) as described in the [Security](#13-security) section.

### Problem Statement

A production server holds **~100 million user records** spread across one or more databases. The company needs:

- **Zero-data-loss backups** on a schedule, with point-in-time restore capability.
- **Online or offline migration** between database engines (e.g., Postgres → Cassandra, MongoDB → Postgres) without full downtime.
- **Verifiable integrity** — every backup and migration must produce a checksum manifest that can be re-verified at any time.
- **Resumable operations** — a 100M-row migration that crashes at row 73M must restart from row 73M, not row 0.

### Supported Databases

| Role            | Database         | Protocol / Driver                              |
| --------------- | ---------------- | ---------------------------------------------- |
| Source & Target | PostgreSQL       | `pgx/v5`                                       |
| Source & Target | MySQL            | `go-sql-driver/mysql`                          |
| Source & Target | MongoDB          | `mongo-driver`                                 |
| Source & Target | Apache Cassandra | `gocql`                                        |
| Source & Target | Apache Iceberg   | REST Catalog API + Parquet (`apache/arrow-go`) |

> **Note:** "Mongo" and "MongoDB" in the requirements are treated as one engine. If FoundationDB or another engine surfaces later, the adapter interface makes addition straightforward.

### Non-Goals (v1)

- GUI / web dashboard (CLI + structured logs are sufficient for v1).
- Streaming CDC (Change Data Capture) replication (planned for v2).
- Cross-cloud network routing (the operator is responsible for network connectivity).

---

## 2. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                           gomigrate CLI                             │
│   migrate | backup | restore | verify | status                      │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                    ┌────────▼────────┐
                    │  Config Loader  │  YAML / ENV / Vault
                    └────────┬────────┘
                             │
          ┌──────────────────▼──────────────────┐
          │            Orchestrator             │
          │  - Builds pipeline DAG              │
          │  - Manages worker pools             │
          │  - Owns checkpoint state            │
          └──┬──────────────┬──────────────┬───┘
             │              │              │
    ┌────────▼───┐  ┌───────▼────┐  ┌────▼────────┐
    │  Reader    │  │ Transformer │  │   Writer    │
    │  Workers   │  │  Workers   │  │   Workers   │
    │  (goroutines)│ (goroutines)│  │  (goroutines)│
    └────────┬───┘  └───────┬────┘  └────┬────────┘
             │              │              │
    ┌────────▼──────────────▼──────────────▼────────┐
    │               Adapter Layer                   │
    │  PostgresAdapter | MySQLAdapter | MongoAdapter│
    │  CassandraAdapter | IcebergAdapter            │
    └────────────────────────────────────────────────┘
             │                            │
    ┌────────▼────────┐         ┌─────────▼────────┐
    │  Source DB(s)   │         │   Target DB(s)   │
    └─────────────────┘         └──────────────────┘

    ┌──────────────────────────────────────────────┐
    │              Cross-Cutting Concerns           │
    │  Checkpoint Store (bbolt) | Metrics (prom)   │
    │  Structured Logger (zap)  | Tracer (otel)    │
    └──────────────────────────────────────────────┘
```

### Data Flow (Migration)

```
Source DB
  └─► Partitioned Cursor (N goroutines, each owns a range)
         └─► Record Channel (buffered, back-pressure aware)
                └─► Transformer Pool (schema mapping, type coercion)
                       └─► Batch Assembler (collects M records)
                              └─► Writer Pool (parallel upserts to target)
                                     └─► Checkpoint Writer (persists progress)
```

### Data Flow (Backup)

```
Source DB
  └─► Partitioned Cursor
         └─► Serializer (Parquet or NDJSON)
                └─► Compressor (zstd)
                       └─► Chunk Writer (fixed-size files, e.g. 512 MB)
                              └─► Checksum (SHA-256 per chunk)
                                     └─► Manifest Writer (JSON index)
                                            └─► Storage Backend (local / S3 / GCS)
```

---

## 3. Repository Layout

```
gomigrate/
├── cmd/
│   └── gomigrate/
│       └── main.go                  # CLI entry point (cobra)
│
├── internal/
│   ├── config/
│   │   ├── config.go                # Config structs, loader (viper)
│   │   └── validate.go              # Config validation
│   │
│   ├── adapter/
│   │   ├── adapter.go               # SourceAdapter / TargetAdapter interfaces
│   │   ├── postgres/
│   │   │   ├── reader.go            # Cursor-based parallel read
│   │   │   ├── writer.go            # COPY protocol bulk writer
│   │   │   └── schema.go            # Schema introspection
│   │   ├── mysql/
│   │   │   ├── reader.go            # Range-based parallel read
│   │   │   ├── writer.go            # LOAD DATA or Bulk Insert
│   │   │   └── schema.go            # Schema introspection
│   │   ├── mongo/
│   │   │   ├── reader.go            # Parallel collection scan
│   │   │   ├── writer.go            # BulkWrite with ordered=false
│   │   │   └── schema.go
│   │   ├── cassandra/
│   │   │   ├── reader.go            # Token-range parallel scan
│   │   │   ├── writer.go            # Async batched INSERT
│   │   │   └── schema.go
│   │   └── iceberg/
│   │       ├── reader.go            # REST catalog + Parquet file scan
│   │       ├── writer.go            # Parquet file writer + catalog commit
│   │       └── schema.go
│   │
│   ├── pipeline/
│   │   ├── orchestrator.go          # Top-level pipeline coordinator
│   │   ├── reader_pool.go           # Fan-out reader goroutines
│   │   ├── transformer_pool.go      # Concurrent schema transformation
│   │   ├── writer_pool.go           # Fan-in writer goroutines
│   │   ├── batch.go                 # Batch assembler with size+time flush
│   │   └── backpressure.go          # Channel sizing, token bucket
│   │
│   ├── migration/
│   │   ├── engine.go                # Migration workflow
│   │   ├── schema_mapper.go         # Cross-DB type mapping rules
│   │   ├── verifier.go              # Row-count + checksum verification
│   │   └── planner.go               # Partition planner (splits source)
│   │
│   ├── backup/
│   │   ├── engine.go                # Backup workflow
│   │   ├── serializer.go            # Parquet / NDJSON serialization
│   │   ├── compressor.go            # zstd streaming compression
│   │   ├── manifest.go              # Chunk manifest (JSON)
│   │   └── restore.go               # Restore workflow
│   │
│   ├── checkpoint/
│   │   ├── store.go                 # bbolt-backed checkpoint store
│   │   ├── model.go                 # Checkpoint data structures
│   │   └── gc.go                    # Old checkpoint garbage collection
│   │
│   ├── storage/
│   │   ├── storage.go               # Storage interface
│   │   ├── local.go                 # Local filesystem backend
│   │   ├── s3.go                    # AWS S3 backend (aws-sdk-go-v2)
│   │   └── gcs.go                   # Google Cloud Storage backend
│   │
│   ├── schema/
│   │   ├── model.go                 # Canonical schema representation
│   │   └── diff.go                  # Schema diff for migration planning
│   │
│   ├── record/
│   │   └── record.go                # Universal in-memory record (map + metadata)
│   │
│   ├── metrics/
│   │   └── metrics.go               # Prometheus metrics registry
│   │
│   ├── telemetry/
│   │   ├── logger.go                # zap structured logger setup
│   │   └── tracer.go                # OpenTelemetry tracer setup
│   │
│   └── errs/
│       ├── errors.go                # Sentinel errors, error types
│       └── retry.go                 # Exponential backoff + jitter
│
├── test/
│   ├── integration/
│   │   ├── postgres_test.go
│   │   ├── mongo_test.go
│   │   ├── cassandra_test.go
│   │   └── iceberg_test.go
│   └── e2e/
│       └── migration_e2e_test.go
│
├── scripts/
│   ├── docker-compose.yml           # Local dev databases
│   └── seed/                        # Test data generators
│
├── configs/
│   └── example.yaml                 # Annotated sample config
│
├── go.mod
├── go.sum
├── Makefile
└── README.md
```

---

## 4. Core Design Principles

### 4.1 Universal Record Type

All adapters translate their native row/document into a single canonical `Record`:

```go
// internal/record/record.go
type Record struct {
    ID       string                 // Source-assigned logical ID
    Data     map[string]any         // Column/field values (normalized)
    Metadata RecordMetadata
}

type RecordMetadata struct {
    SourceTable   string
    SourceDB      string
    PartitionKey  string
    Checksum      [32]byte           // SHA-256 of Data bytes
    IngestionTime time.Time
}
```

This decouples the reader pipeline from the writer pipeline entirely. The Transformer layer handles type coercion between source and target schemas.

### 4.2 Adapter Interface

```go
// internal/adapter/adapter.go

type SourceAdapter interface {
    // Connect validates credentials and opens a connection pool.
    Connect(ctx context.Context, cfg config.DBConfig) error

    // Partitions splits the source table/collection into N roughly equal
    // partitions for parallel reading. Returns partition descriptors.
    Partitions(ctx context.Context, table string, n int) ([]Partition, error)

    // ReadPartition streams records from a single partition into ch.
    // It must respect ctx cancellation and send on errCh on fatal errors.
    ReadPartition(ctx context.Context, p Partition, ch chan<- *record.Record, errCh chan<- error)

    // Schema returns the canonical schema for a table/collection.
    Schema(ctx context.Context, table string) (*schema.Schema, error)

    // Close releases all connections.
    Close() error
}

type TargetAdapter interface {
    Connect(ctx context.Context, cfg config.DBConfig) error

    // WriteBatch atomically (best-effort) writes a batch of records.
    // Returns the count of successfully written records.
    WriteBatch(ctx context.Context, batch []*record.Record) (int, error)

    // ApplySchema creates or alters the target table to match s.
    ApplySchema(ctx context.Context, s *schema.Schema) error

    Close() error
}
```

### 4.3 Partition Strategies Per Database

| DB         | Partition Strategy                                  |
| ---------- | --------------------------------------------------- |
| PostgreSQL | `ctid` range scan OR integer PK range split         |
| MySQL      | Integer PK range split (auto-incrementing PK)       |
| MongoDB    | `_id` ObjectID range split (or `$sample`-based)     |
| Cassandra  | Native token range splits (via `system.local`/ring) |
| Iceberg    | File-level splits (one goroutine per Parquet file)  |

---

## 5. Concurrency Model

### 5.1 Pipeline Goroutine Architecture

```
                    ┌─────────────────────────────────────┐
                    │         Orchestrator goroutine       │
                    │  - Calls Partitions(ctx, table, N)  │
                    │  - Spawns N reader goroutines        │
                    │  - Spawns M transformer goroutines   │
                    │  - Spawns W writer goroutines        │
                    │  - Monitors errGroup                 │
                    └──┬──────────────────────────────┬───┘
                       │ errgroup.WithContext          │
            ┌──────────▼──────────┐       ┌───────────▼──────────┐
            │  Reader goroutines  │       │  Writer goroutines   │
            │  (N = num_readers)  │       │  (W = num_writers)   │
            │                     │       │                      │
            │  each: ReadPartition│       │  each: WriteBatch    │
            └──────────┬──────────┘       └───────────┬──────────┘
                       │                              ▲
                  recordCh (buffered)         batchCh (buffered)
                  capacity: N * batchSize           │
                       │                            │
            ┌──────────▼──────────────────────────┐ │
            │      Transformer goroutines          │─┘
            │  (M = num_transformers)              │
            │  reads from recordCh, maps schema,   │
            │  assembles batches, sends to batchCh │
            └──────────────────────────────────────┘
```

### 5.2 Back-Pressure

- `recordCh` is a buffered channel with capacity `N * batch_size`. Readers block naturally when the transformer pool falls behind.
- `batchCh` is a buffered channel with capacity `W * 2`. Transformers block when writers fall behind.
- A **token bucket** rate limiter (via `golang.org/x/time/rate`) is applied per adapter to respect target DB write throughput limits, configurable per environment.

### 5.3 Worker Pool Parameters (Defaults, Tunable)

```yaml
concurrency:
  num_readers: 16 # goroutines reading from source
  num_transformers: 8 # goroutines doing schema mapping
  num_writers: 16 # goroutines writing to target
  batch_size: 1000 # records per write batch
  batch_timeout: 5s # max wait before flushing partial batch
  channel_multiplier: 4 # channel capacity = workers * this
  rate_limit_rps: 0 # 0 = unlimited
```

### 5.4 Error Propagation

- All goroutines run inside `golang.org/x/sync/errgroup`.
- A fatal error in any goroutine cancels the shared `context.Context`, causing all other goroutines to drain and exit.
- Non-fatal errors (e.g., a single bad record) go to a **dead-letter channel** that is persisted to a `failed_records.ndjson` file for post-hoc inspection.

### 5.5 Graceful Shutdown

Signal handler (`SIGINT` / `SIGTERM`) triggers:

1. Context cancellation → readers stop accepting new partitions.
2. `recordCh` drains → transformers finish in-flight records.
3. `batchCh` drains → writers flush remaining batches.
4. Checkpoint is written with the last committed partition/offset.
5. Connections are closed cleanly.

---

## 6. Database Adapters

### 6.1 PostgreSQL Adapter

**Reading:**

- Use `pgx` connection pool (`pgxpool`).
- Introspect `information_schema` for tables, columns, types.
- Partition via `WHERE id >= $1 AND id < $2` on primary key (or `ctid` for tables without integer PK).
- Use server-side cursor (`DECLARE … CURSOR FOR SELECT …`) to stream large partitions without loading all rows into memory.
- Use `COPY … TO STDOUT (FORMAT BINARY)` for maximum throughput when doing full-table backup.

**Writing:**

- Use `COPY … FROM STDIN (FORMAT BINARY)` via `pgx.CopyFrom` for bulk inserts — far faster than individual `INSERT`.
- For migrations with conflict handling, use `INSERT … ON CONFLICT DO UPDATE`.
- Wrap each batch in a transaction; roll back only the failed batch, not the entire migration.

**Schema Mapping (as source):**

- `integer` → `int64`
- `text` / `varchar` → `string`
- `timestamptz` → `time.Time` (UTC-normalized)
- `jsonb` → `map[string]any`
- `uuid` → `string` (canonical UUID format)
- Arrays → `[]any`

### 6.2 MongoDB Adapter

**Reading:**

- Use the official `go.mongodb.org/mongo-driver`.
- Partition by `_id` ObjectID range: sample N boundary ObjectIDs using `$sample` + sort, then issue N range queries in parallel.
- Use `.Find()` with a cursor and `BatchSize` set to `batch_size` config value.
- Flatten nested documents into dotted-path keys for relational targets; preserve as `map[string]any` for document targets.

**Writing:**

- Use `Collection.BulkWrite` with `ordered: false` for maximum parallelism.
- Use `ReplaceOne` with `upsert: true` to make writes idempotent.

**Schema Mapping:**

- MongoDB is schemaless — infer schema from the first 1000 documents (configurable).
- Emit a schema warning if later documents deviate from the inferred schema.

### 6.3 Cassandra Adapter

**Reading:**

- Use `gocql` with token-aware routing.
- Retrieve token ring via `SELECT tokens FROM system.local` and `system.peers`.
- Assign each token range to a reader goroutine; each goroutine queries `SELECT … FROM … WHERE token(pk) >= ? AND token(pk) < ?`.
- Use `gocql`'s page-based iteration (`Query.PageSize`) to avoid OOM.

**Writing:**

- Use `gocql` batch statements. Cap batch size at ~100 statements (Cassandra default `batch_size_warn_threshold` is 5KB).
- Use `UNLOGGED BATCH` for same-partition writes; individual statements for cross-partition.
- Apply exponential backoff on `Timeout` and `Unavailable` errors.

**Schema Mapping:**

- Cassandra `text` → `string`
- Cassandra `uuid` → `string`
- Cassandra `timestamp` → `time.Time`
- Cassandra collections (`list`, `set`, `map`) → `[]any` / `map[string]any`
- Cassandra `frozen<>` → treat as opaque blob, serialize to JSON string for relational targets.

### 6.4 Iceberg Adapter

**Reading:**

- Connect to the Iceberg REST Catalog via HTTP (`catalog_url` in config).
- List table snapshots; pick the latest (or a specified snapshot ID for point-in-time).
- Retrieve manifest list → manifests → data file paths (Parquet on S3/GCS/local).
- Assign each Parquet file to a reader goroutine; read with `apache/arrow-go` (columnar → row conversion).

**Writing:**

- Write records into Parquet files (row-group size = `batch_size * 10`).
- On flush, upload the Parquet file to the data location and register with the catalog via `POST /v1/namespaces/{ns}/tables/{table}/snapshots`.
- Use optimistic concurrency: if the catalog commit fails due to a concurrent writer, retry with a new snapshot.

**Schema Mapping:**

- Iceberg `long` → `int64`
- Iceberg `string` → `string`
- Iceberg `timestamptz` → `time.Time`
- Iceberg `struct` → `map[string]any`
- Iceberg `list` → `[]any`
- Iceberg `map` → `map[string]any`

### 6.5 MySQL Adapter

**Reading:**

- Use `go-sql-driver/mysql` connection pool.
- Introspect `information_schema` for schema details.
- Partition via `WHERE id >= ? AND id < ?` on auto-incrementing primary key.
- Use `Streaming` results (via `sql.Rows`) to handle large result sets without OOM.
- For high-performance reads, use `SELECT ... INTO OUTFILE` for backups (if local access available).

**Writing:**

- Use `LOAD DATA LOCAL INFILE` for high-speed bulk inserts.
- Fallback to multi-row `INSERT INTO ... VALUES (...), (...), ...` for smaller batches or restricted environments.
- Use `INSERT ... ON DUPLICATE KEY UPDATE` for idempotent migrations.
- Wrap batches in transactions for atomicity.

**Schema Mapping:**

- `int` / `bigint` → `int64`
- `varchar` / `text` / `longtext` → `string`
- `datetime` / `timestamp` → `time.Time`
- `json` → `map[string]any`
- `decimal` → `float64` (or custom decimal type if precision is critical)
- `enum` → `string`

---

## 7. Migration Engine

### 7.1 Migration Lifecycle

```
PLAN → VALIDATE → SCHEMA_APPLY → MIGRATE → VERIFY → DONE
                                     ↑ resume from checkpoint
```

**PLAN:** The planner reads source schema, infers target schema (or reads an explicit mapping file), calculates partition count based on estimated row count and `num_readers`.

**VALIDATE:** Pre-flight checks:

- Source DB reachable, credentials valid, table exists.
- Target DB reachable, credentials valid, write permission.
- Estimated source size vs. available disk/network.
- Config parameters are within safe limits.

**SCHEMA_APPLY:** Call `TargetAdapter.ApplySchema()`. In `--dry-run` mode, print DDL without executing.

**MIGRATE:** Run the concurrent pipeline. Checkpoint is written after every committed batch.

**VERIFY:** After all partitions complete:

- Compare row counts (source vs. target).
- Optionally re-scan a random 1% sample and compare SHA-256 of each record's canonical JSON.

### 7.2 Schema Mapper

The schema mapper is a pluggable rules engine. Mapping rules are expressed in YAML:

```yaml
# configs/mappings/pg_to_cassandra.yaml
mappings:
  - source_column: user_id
    target_column: user_id
    transform: none

  - source_column: created_at
    target_column: created_at
    transform: to_unix_ms # timestamptz → bigint (ms since epoch)

  - source_column: metadata
    target_column: metadata_json
    transform: to_json_string # jsonb → text

  - source_column: tags
    target_column: tags
    transform: none # text[] → list<text>
```

Built-in transforms: `none`, `to_json_string`, `from_json_string`, `to_unix_ms`, `from_unix_ms`, `to_upper`, `to_lower`, `uuid_to_string`, `string_to_uuid`, `flatten_json`.

Custom transforms are Go plugins (`plugin.Open`) loaded from a directory.

### 7.3 Verifier

```go
type VerificationReport struct {
    SourceCount     int64
    TargetCount     int64
    SampledRecords  int64
    Mismatches      []RecordMismatch
    ChecksumMatch   bool
    Duration        time.Duration
}
```

Verification runs a parallel scan of both source and target for sampled IDs and compares the canonical SHA-256 of each record's normalized JSON.

---

## 8. Backup Engine

### 8.1 Backup Format

Each backup is a **directory** (local or object-store prefix) containing:

```
backup-2025-11-01T00:00:00Z-postgres-users/
├── manifest.json          ← index of all chunks + metadata
├── chunk-0000.parquet.zst
├── chunk-0001.parquet.zst
├── chunk-0002.parquet.zst
└── ...
```

`manifest.json`:

```json
{
  "version": 1,
  "source": { "type": "postgres", "table": "users", "db": "prod" },
  "created_at": "2025-11-01T00:00:00Z",
  "row_count": 100000000,
  "chunk_size_bytes": 536870912,
  "chunks": [
    { "index": 0, "file": "chunk-0000.parquet.zst", "rows": 1234567, "sha256": "abc123..." },
    { "index": 1, "file": "chunk-0001.parquet.zst", "rows": 1234567, "sha256": "def456..." }
  ],
  "schema_snapshot": { ... }
}
```

### 8.2 Backup Workflow

1. **Snapshot** the source (Postgres: `BEGIN ISOLATION LEVEL REPEATABLE READ`; Cassandra: record timestamp; MongoDB: start a session with consistent snapshot; Iceberg: record snapshot ID).
2. **Partition** the source table (same strategy as migration).
3. **Stream** each partition through the serializer (Parquet preferred; NDJSON as fallback) and compressor (zstd level 3 — good balance of speed vs. ratio).
4. **Chunk** output at configurable size (default 512 MB compressed).
5. **Checksum** each chunk (SHA-256).
6. **Upload** chunks to the storage backend concurrently (`num_writers` goroutines).
7. **Write** `manifest.json` atomically after all chunks are confirmed.

### 8.3 Restore Workflow

1. Read `manifest.json` from backup location.
2. Verify each chunk's SHA-256 before processing.
3. **Apply schema** to target DB.
4. **Decompress + deserialize** chunks (parallelized, one goroutine per chunk).
5. **Write** to target via `TargetAdapter.WriteBatch`.
6. **Verify** row count after restore.

### 8.4 Backup Scheduling

The tool ships with a `scheduler` subcommand that uses `robfig/cron` to run backups on a cron expression. Intended to run as a long-lived process (or Kubernetes CronJob with `gomigrate backup` as the command).

### 8.5 Retention Policy

```yaml
backup:
  retention:
    keep_last: 7 # always keep the 7 most recent
    keep_daily: 30 # keep one per day for 30 days
    keep_weekly: 12 # keep one per week for 12 weeks
    keep_monthly: 12 # keep one per month for 12 months
```

Expired backups are deleted from the storage backend. Manifests are kept as tombstones for audit purposes.

---

## 9. Checkpoint & Resume System

### 9.1 Checkpoint Store

`bbolt` (embedded key-value store, single-file, no external dependency) is used as the checkpoint store. One `.bolt` file per operation (migration or backup), stored locally alongside the binary or at a configurable path.

**Checkpoint key schema:**

```
operations/{operation_id}/meta         → OperationMeta (JSON)
operations/{operation_id}/partitions/{partition_id}  → PartitionCheckpoint (JSON)
```

**PartitionCheckpoint:**

```go
type PartitionCheckpoint struct {
    PartitionID   string
    Status        PartitionStatus  // Pending | InProgress | Done | Failed
    LastCommitted int64            // last primary key / token committed
    RowsWritten   int64
    ErrorCount    int64
    UpdatedAt     time.Time
}
```

### 9.2 Resume Logic

On startup, the orchestrator:

1. Checks for an existing checkpoint file for the operation.
2. Loads all partition checkpoints.
3. Skips partitions with `Status == Done`.
4. Re-queues partitions with `Status == InProgress` from `LastCommitted` (sends `WHERE pk > LastCommitted`).
5. Re-queues partitions with `Status == Pending` from the beginning.

### 9.3 Checkpoint Write Frequency

Checkpoint is written to bbolt after every **N committed batches** (default N=10, configurable). bbolt writes are transactional; there is no risk of a corrupt checkpoint file on crash.

---

## 10. Configuration & CLI

### 10.1 Config File Structure

```yaml
# configs/example.yaml

operation: migrate # migrate | backup | restore | verify

source:
  type: postgres
  host: prod-pg.internal
  port: 5432
  database: appdb
  user: migrator
  password: "${PG_PASSWORD}" # env var interpolation
  ssl_mode: require
  tables:
    - users
    - orders

target:
  type: cassandra
  hosts:
    - cass1.internal
    - cass2.internal
  keyspace: prod
  consistency: LOCAL_QUORUM

concurrency:
  num_readers: 16
  num_transformers: 8
  num_writers: 16
  batch_size: 1000
  batch_timeout: 5s
  rate_limit_rps: 0

migration:
  schema_mapping_file: configs/mappings/pg_to_cassandra.yaml
  conflict_strategy: upsert # upsert | skip | error
  verify_after: true
  verify_sample_pct: 1.0

backup:
  format: parquet # parquet | ndjson
  compression: zstd
  chunk_size_mb: 512
  storage:
    type: s3 # local | s3 | gcs
    bucket: company-db-backups
    prefix: prod/postgres/users
    region: us-east-1
  retention:
    keep_last: 7

checkpoint:
  path: /var/lib/gomigrate/checkpoints
  flush_every_n_batches: 10

telemetry:
  log_level: info # debug | info | warn | error
  log_format: json # json | console
  metrics_addr: ":9090" # Prometheus scrape endpoint
  tracing_endpoint: "http://otel-collector:4318"
```

### 10.2 CLI Commands

```
gomigrate migrate   --config <file> [--dry-run] [--resume] [--tables t1,t2]
gomigrate backup    --config <file> [--tables t1,t2] [--snapshot-id <id>]
gomigrate restore   --config <file> --manifest <path> [--target-table <name>]
gomigrate verify    --config <file> --manifest <path>
gomigrate status    --checkpoint <path>
gomigrate schema    --config <file> --source    # print inferred schema
gomigrate schema    --config <file> --diff      # diff source vs target schema
gomigrate version
```

### 10.3 Environment Variable Overrides

Any config key can be overridden with an environment variable using the pattern:

```
GOMIGRATE_SOURCE_HOST=new-host gomigrate migrate ...
GOMIGRATE_CONCURRENCY_NUM_READERS=32 gomigrate migrate ...
```

This enables Kubernetes-style config injection without modifying the config file.

---

## 11. Observability — Metrics, Logging & Tracing

### 11.1 Prometheus Metrics

Exposed on `metrics_addr` (default `:9090`):

| Metric                                   | Type      | Description                         |
| ---------------------------------------- | --------- | ----------------------------------- |
| `gomigrate_records_read_total`           | Counter   | Records read from source            |
| `gomigrate_records_written_total`        | Counter   | Records written to target           |
| `gomigrate_records_failed_total`         | Counter   | Records that failed (dead-lettered) |
| `gomigrate_partitions_total`             | Gauge     | Total partitions planned            |
| `gomigrate_partitions_done`              | Gauge     | Partitions completed                |
| `gomigrate_batch_write_duration_seconds` | Histogram | Write latency per batch             |
| `gomigrate_channel_fill_ratio`           | Gauge     | recordCh + batchCh fill ratios      |
| `gomigrate_reader_goroutines`            | Gauge     | Live reader goroutines              |
| `gomigrate_writer_goroutines`            | Gauge     | Live writer goroutines              |
| `gomigrate_bytes_written_total`          | Counter   | Bytes written (backup)              |
| `gomigrate_estimated_eta_seconds`        | Gauge     | ETA based on current throughput     |

### 11.2 Structured Logging

`go.uber.org/zap` in production (JSON) mode. Every log line carries:

```json
{
  "ts": "2025-11-01T00:01:23.456Z",
  "level": "info",
  "msg": "batch written",
  "operation_id": "mig-2025-001",
  "partition_id": "p-007",
  "batch_size": 1000,
  "rows_written": 1000,
  "duration_ms": 142,
  "total_written": 7000000
}
```

### 11.3 OpenTelemetry Tracing

A trace is created per operation. Child spans are created per partition. Span events mark batch writes. Exported via OTLP HTTP to a configurable endpoint (Jaeger, Tempo, etc.).

### 11.4 Progress Reporter

A goroutine running every 5 seconds logs a human-readable progress summary:

```
[gomigrate] Progress: 72,340,000 / 100,000,000 records (72.3%)
            Throughput: 185,000 rec/s  |  ETA: 2m 21s
            Partitions: 45/64 done     |  Failed: 0
```

---

## 12. Error Handling & Retry Strategy

### 12.1 Error Classification

| Class      | Examples                                            | Action                                 |
| ---------- | --------------------------------------------------- | -------------------------------------- |
| Transient  | Network timeout, lock timeout, connection reset     | Retry with backoff                     |
| Throttle   | Cassandra Overloaded, Postgres too many connections | Retry after delay + reduce concurrency |
| Data error | Type mismatch, constraint violation, invalid UTF-8  | Dead-letter the record, continue       |
| Fatal      | Auth failure, table not found, disk full            | Cancel context, exit non-zero          |

### 12.2 Retry Policy

```go
// internal/errs/retry.go
type RetryPolicy struct {
    MaxAttempts     int
    InitialInterval time.Duration
    MaxInterval     time.Duration
    Multiplier      float64
    Jitter          float64        // fraction of interval added randomly
}

var DefaultRetry = RetryPolicy{
    MaxAttempts:     5,
    InitialInterval: 100 * time.Millisecond,
    MaxInterval:     30 * time.Second,
    Multiplier:      2.0,
    Jitter:          0.2,
}
```

Retry is applied at the **batch** level (retry the whole batch), not the record level, to keep logic simple. After `MaxAttempts`, the batch is split in half and each half is retried independently. Records that still fail after split-and-retry are dead-lettered.

### 12.3 Dead-Letter Queue

Failed records are written to `{checkpoint_path}/{operation_id}_failed.ndjson`. Each line:

```json
{"record_id":"u-123","source_table":"users","error":"type mismatch on column age","attempt":5,"ts":"2025-11-01T00:12:00Z","data":{...}}
```

A `gomigrate replay --failed <file>` command can re-attempt dead-lettered records after fixing the mapping.

---

## 13. Security

### 13.1 Credentials

- Passwords and keys are **never** hardcoded. They are read from environment variables, a `.env` file (via `godotenv`), or a Vault agent token file.
- Add a `secrets.provider` config option: `env` | `vault` | `aws_secrets_manager` | `file`.
- Vault integration uses `github.com/hashicorp/vault/api` with AppRole or Kubernetes auth.

### 13.2 TLS

- All database connections use TLS where supported (Postgres `sslmode=require`, Cassandra `SslOptions`, MongoDB `tls=true`).
- CA cert paths are configurable; `insecure_skip_verify` is available but logged as a warning.

### 13.3 Audit Log

A separate append-only audit log (JSON lines) records:

- Who started the operation (OS user + hostname).
- Operation type, source, target, table list.
- Start time, end time, row counts, outcome (success / failure / partial).
- Config hash (SHA-256 of the resolved config, excluding secrets).

### 13.4 Data Masking (Optional)

A `masking` section in the mapping config allows PII columns to be hashed or redacted during migration/backup:

```yaml
masking:
  - column: email
    strategy: sha256 # replace value with SHA-256(value)
  - column: phone
    strategy: redact # replace with "REDACTED"
  - column: ssn
    strategy: tokenize # replace with a stable opaque token (requires token vault)
```

---

## 14. Testing Strategy

### 14.1 Unit Tests

- All `internal/` packages have `_test.go` files with ≥80% coverage target.
- Schema mapper and transformer are tested with table-driven tests covering all type combinations.
- Checkpoint store is tested with simulated crashes (write `N` checkpoints, kill, verify resume from correct position).
- Retry logic is tested with a mock that fails a configurable number of times before succeeding.

### 14.2 Integration Tests

- Use `testcontainers-go` to spin up real Docker containers for each database.
- Each adapter has integration tests that: create schema → seed 100K rows → read all → write to a second container → verify row count.
- Run with `make test-integration` (requires Docker).

### 14.3 End-to-End Tests

- Full migration path tests for each source→target pair (Postgres→Cassandra, Mongo→Postgres, etc.).
- Simulated crash-and-resume test: kill the process at 50% completion, restart, verify final row count equals source row count with no duplicates.
- Performance baseline test: 10M records, measure throughput (records/second), assert it meets a minimum threshold.

### 14.4 Chaos Tests (v2)

- Network partition simulation (via `toxiproxy`): validate graceful degradation and resume.
- Target DB going read-only mid-migration: validate error classification and clean shutdown.

---

## 15. Phase-by-Phase Delivery Plan

### Phase 0 — Scaffold (Week 1)

- [x] `go mod init`, cobra CLI skeleton, viper config loader.
- [x] `internal/record`, `internal/adapter` interfaces defined.
- [x] `internal/errs/retry.go` implemented and unit-tested.
- [x] `internal/checkpoint/store.go` implemented and unit-tested.
- [x] `Makefile` with `build`, `test`, `test-integration`, `lint` targets.
- [x] `docker-compose.yml` with Postgres, MongoDB, Cassandra, Iceberg REST catalog (using env vars).
- [x] `.env.example` created and `.gitignore` updated for `.env`.

**Deliverable:** `gomigrate --help` works; config loads; no actual DB connectivity yet.

---

### Phase 1 — PostgreSQL Adapter (Weeks 2–3)

- [ ] `adapter/postgres/reader.go`: PK-range partitioner + cursor-based streaming.
- [ ] `adapter/postgres/writer.go`: `COPY FROM` bulk writer.
- [ ] `adapter/postgres/schema.go`: `information_schema` introspection.
- [ ] `internal/pipeline/`: reader pool, transformer pool (identity), writer pool, batch assembler.
- [ ] `gomigrate migrate` works for Postgres → Postgres (same DB, different table).
- [ ] Integration test: 1M rows Postgres → Postgres.
- [ ] Prometheus metrics endpoint live.

**Deliverable:** Postgres-to-Postgres migration functional with checkpointing and metrics.

---

### Phase 2 — Backup Engine (Weeks 4–5)

- [x] `internal/backup/serializer.go`: Parquet writer (arrow-go).
- [x] `internal/backup/compressor.go`: zstd streaming wrapper.
- [x] `internal/backup/manifest.go`: chunk manifest writer/reader.
- [x] `internal/storage/local.go`, `internal/storage/s3.go`, and `internal/storage/gcs.go`.
- [x] `internal/backup/engine.go`: Backup workflow.
- [x] `internal/backup/restore.go`: Restore workflow.
- [x] Circuit breaker for error handling.
- [x] Edge case testing with chaos injection.

**Deliverable:** Full backup/restore cycle for PostgreSQL (and generic sources).

---

### Phase 3 — MongoDB Adapter (Week 6)

- [ ] `adapter/mongo/reader.go`: ObjectID-range partitioner + cursor.
- [ ] `adapter/mongo/writer.go`: BulkWrite upsert.
- [ ] `adapter/mongo/schema.go`: inferred schema from sample.
- [ ] Schema mapper: Mongo → Postgres type coercion.
- [ ] `gomigrate migrate` Mongo → Postgres and Postgres → Mongo.
- [ ] Backup/restore for MongoDB.
- [ ] Integration tests.

**Deliverable:** MongoDB fully supported as source and target.

---

### Phase 4 — Cassandra Adapter (Weeks 7–8)

- [ ] `adapter/cassandra/reader.go`: token-range partitioner.
- [ ] `adapter/cassandra/writer.go`: unlogged batch writer.
- [ ] `adapter/cassandra/schema.go`: CQL `DESCRIBE TABLE`.
- [ ] Type mapping for Cassandra collections.
- [ ] `gomigrate migrate` Postgres ↔ Cassandra, Mongo ↔ Cassandra.
- [ ] Backup/restore for Cassandra.
- [ ] Integration tests.

**Deliverable:** Cassandra fully supported as source and target.

---

### Phase 5 — Iceberg Adapter (Weeks 9–10)

- [ ] `adapter/iceberg/reader.go`: REST catalog + Parquet file scanner.
- [ ] `adapter/iceberg/writer.go`: Parquet file writer + snapshot commit.
- [ ] `adapter/iceberg/schema.go`: Iceberg schema type mapping.
- [ ] `gomigrate migrate` Postgres → Iceberg and Iceberg → Postgres.
- [ ] Backup/restore for Iceberg (snapshot-based).
- [ ] Integration tests (using local catalog + MinIO).

**Deliverable:** Iceberg fully supported as source and target.

---

### Phase 6 — Hardening & Performance (Weeks 11–12)

- [ ] Crash-and-resume e2e tests for all adapters.
- [ ] Load test: 100M synthetic rows Postgres → Cassandra; tune defaults.
- [ ] Dead-letter queue and `gomigrate replay` command.
- [ ] Data masking transforms.
- [ ] Vault secrets integration.
- [ ] Security audit of TLS configurations.
- [ ] Benchmark report (throughput per adapter pair, memory profile).

**Deliverable:** Production-ready v1.0.

---

### Phase 7 — Observability & Ops (Week 13)

- [ ] OpenTelemetry tracing with span-per-partition.
- [ ] Structured audit log.
- [ ] `gomigrate status` reads live checkpoint and prints progress table.
- [ ] Example Grafana dashboard JSON (Prometheus datasource).
- [ ] Helm chart / Kubernetes manifests for running as a Job or CronJob.
- [ ] Comprehensive README with quickstart, all CLI flags, config reference.

**Deliverable:** Fully observable; ready for ops handoff.

---

### Phase 8 — MySQL Support (Weeks 14–15)

- [ ] `adapter/mysql/reader.go`: Integer PK-range partitioner + streaming.
- [ ] `adapter/mysql/writer.go`: `LOAD DATA LOCAL INFILE` bulk writer.
- [ ] `adapter/mysql/schema.go`: `information_schema` introspection.
- [ ] Integration tests for MySQL ↔ Postgres, MySQL ↔ Mongo.
- [ ] Backup/restore support for MySQL.

**Deliverable:** MySQL fully supported as source and target.

---

## 16. Open Questions & Risk Register

| #   | Question / Risk                                                      | Impact | Mitigation                                                                          |
| --- | -------------------------------------------------------------------- | ------ | ----------------------------------------------------------------------------------- |
| 1   | Postgres `COPY` locks on large tables                                | High   | Use cursor-based read with `REPEATABLE READ`; COPY only for backup (no live writes) |
| 2   | Cassandra token ring changes mid-migration                           | Medium | Re-fetch ring at checkpoint resume; re-partition affected ranges                    |
| 3   | MongoDB schema variance > 5% across documents                        | Medium | Configurable `strict_schema: false`; dead-letter mismatches                         |
| 4   | Iceberg REST catalog rate limits                                     | Low    | Exponential backoff; batch catalog commits                                          |
| 5   | Memory pressure with 1M-record batches                               | High   | Enforce `batch_size` ceiling; profile with `pprof`; use streaming Parquet writer    |
| 6   | Clock skew between source and target in timestamp columns            | Medium | Normalize all timestamps to UTC at reader layer                                     |
| 7   | Character encoding issues (non-UTF-8 binary data in Postgres `text`) | Medium | Detect and base64-encode non-UTF-8 values; log warning                              |
| 8   | Network cost of reading 100M rows from cloud DB                      | High   | Run tool co-located with source DB; use VPC endpoints                               |
| 9   | Large BLOBs / binary columns                                         | Medium | Configurable `max_blob_size_mb`; skip or stream large objects separately            |
| 10  | Schema migration on live target (zero-downtime)                      | High   | Phase 2 work: dual-write + cutover strategy (out of scope for v1)                   |
| 11  | MySQL `LOAD DATA LOCAL INFILE` restricted by server                 | Medium | Provide multi-row `INSERT` fallback; document server-side `local_infile` requirement |

---

_Document version: 0.1 — Initial Plan_
_Last updated: 2025-11-01_
_Owner: Platform Engineering_
