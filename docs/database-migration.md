# Database Migration Process

This document provides an extreme-detail technical breakdown of how `gomigrate` handles database migrations. It covers the architectural pipeline, parallelization strategies, data transformation, and reliability mechanisms.

---

## 1. Architectural Overview

`gomigrate` utilizes a multi-stage, concurrent pipeline to move data from a source database to a target database. The process is coordinated by the **Orchestrator**, which manages the lifecycle of data as it flows through three distinct phases:

1.  **Extraction (Reader Phase)**: Pulling raw data from the source.
2.  **Transformation (Transformer Phase)**: Mapping and normalizing data.
3.  **Loading (Writer Phase)**: Pushing processed data into the target.

### Pipeline Diagram
```text
[Source DB] 
     ↓
[Reader 1] [Reader 2] ... [Reader N]  (Parallel Extraction)
     ↓          ↓              ↓
     +----------+--------------+
                ↓
          [recordCh]                  (Internal Record Format)
                ↓
    [Transformer 1] [Transformer 2]   (Schema Mapping & Coercion)
                ↓
           [batchCh]                  (Batched Records)
                ↓
[Writer 1] [Writer 2] ... [Writer M]  (Parallel Loading)
     ↓          ↓              ↓
[Target DB]
```

---

## 2. Phase 1: Partitioning (Parallelization Strategy)

Before data starts flowing, the Orchestrator asks the **Source Adapter** to partition the data. This allows multiple readers to work in parallel without overlapping.

### PostgreSQL Partitioning
- **Strategy**: Primary Key (PK) Range Splitting.
- **Mechanism**: 
  1.  The adapter finds the `MIN(id)` and `MAX(id)` for the table.
  2.  It divides the total ID range into `N` roughly equal segments.
  3.  Each partition is defined by an inclusive `Start` and exclusive `End` ID (e.g., `id >= 100 AND id < 200`).

### MongoDB Partitioning
- **Primary Strategy**: `splitVector`.
  - Uses the MongoDB internal `splitVector` command (if admin permissions are available) to get balanced chunk boundaries based on the `_id` index.
- **Fallback Strategy**: Aggregation Sampling.
  - If `splitVector` is unavailable (e.g., on MongoDB Atlas), the adapter uses a `$sample` aggregation pipeline to pick `N-1` random ObjectIDs, sorts them, and uses them as range boundaries.

### Database-Specific Partitioning Strategies

| Database | Strategy | Implementation Detail |
| :--- | :--- | :--- |
| **PostgreSQL** | PK Range / `ctid` | Divides integer PKs into segments. For tables without a PK, it uses `ctid` ranges. |
| **MySQL** | PK Range | Uses auto-incrementing integer PK ranges. |
| **MongoDB** | `splitVector` / Sample | Uses MongoDB internal metadata for chunk boundaries, or random `$sample` for Atlas. |
| **Cassandra** | Token Range | Introspects the token ring (`system.local`/`peers`) and assigns token ranges to workers. |
| **Iceberg** | File-level | Treats each Parquet file in the manifest as a separate partition. |
| **SQLite** | `ROWID` Range | Uses the internal `ROWID` or integer PK for range splitting. |

---

## 3. Phase 2: Data Extraction (Reading)

Each **Reader** goroutine is assigned a specific `Partition`.

- **Concurrency**: Governed by `NumReaders` in the configuration.
- **Streaming**: Data is streamed row-by-row (or document-by-document) from the source using server-side cursors (Postgres) or page-based iteration (Cassandra/Mongo).
- **Normalization**: The raw database response is immediately converted into a universal **Record** format.

### Internal Record Format (`record.Record`)
```go
type Record struct {
    ID       string         // Logical ID (e.g., PK value or ObjectID)
    Data     map[string]any // Key-value pairs of the actual data
    Metadata RecordMetadata // Tracking: SourceTable, PartitionID, Offset, Checksum
}
```

---

## 4. Phase 3: Transformation & Mapping

Records from all readers are funneled into `recordCh`, where **Transformer** goroutines process them.

- **Schema Mapping**: The `SchemaMapper` applies rules to align source fields with target requirements.
- **Built-in Transforms**:
  - `to_unix_ms`: Converts `time.Time` to milliseconds since epoch.
  - `to_json_string`: Serializes maps/slices to a JSON string (useful for moving NoSQL to SQL).
  - `uuid_to_string`: Normalizes UUID formats.
  - `flatten_json`: Flattens nested documents into dotted-path keys.
- **Type Coercion**: Handles differences between database types (e.g., converting a Mongo `primitive.ObjectID` to a Postgres `string`).
- **Batching**: Transformers accumulate individual records into batches (defined by `BatchSize`).
- **Flush Timeout**: If a batch doesn't fill up within `BatchTimeout`, it is flushed anyway.

---

## 5. Phase 4: Data Loading (Writing)

**Writer** goroutines consume batches from `batchCh`.

- **Schema Application**: Before writing, the `TargetAdapter` can call `ApplySchema` to ensure the target table/collection exists.
- **Bulk Loading**:
  - **Postgres**: Uses `COPY FROM` (Binary format) for massive throughput.
  - **MySQL**: Uses `LOAD DATA LOCAL INFILE`.
  - **MongoDB**: Uses `BulkWrite` with `ordered: false`.
  - **Cassandra**: Uses `UNLOGGED BATCH` for same-partition writes.
- **Checkpoints**: Every `FlushEveryNBatches`, the Writer saves progress (highest `Offset` per partition) into the **bbolt** Checkpoint Store.

---

## 6. Reliability & Resilience

`gomigrate` is designed for 100M+ record workloads where interruptions are expected:

### Error Handling & Retries
- **Retry Policy**: Transient errors (network blips) use exponential backoff with jitter.
- **Batch Splitting**: If a batch fails after all retries, it is split in half and retried again to isolate the problematic record.
- **Dead-Letter Queue (DLQ)**: Records that still fail are written to a `.ndjson` file for later inspection and replay via `gomigrate replay`.

### Checkpoint & Resume
- If a migration crashes, it can be resumed with the `--resume` flag.
- The Orchestrator reads the bbolt file and skips all `Done` partitions, resuming `InProgress` ones from the `LastCommitted` offset.

### Monitoring
Real-time visibility is provided via **Prometheus Metrics**:
- `gomigrate_records_read_total`: Throughput from source.
- `gomigrate_records_written_total`: Throughput to target.
- `gomigrate_batch_write_duration_seconds`: Performance monitoring of target DB.
- `gomigrate_channel_fill_ratio`: Monitors backpressure in the pipeline.

---

## 7. Migration Lifecycle Summary

1.  **PLAN**: Fetch source schema, calculate partitions based on row count.
2.  **VALIDATE**: Pre-flight checks on connectivity and permissions.
3.  **PREPARE**: Apply schema to the target database.
4.  **EXECUTE**: Run the parallel Reader → Transformer → Writer pipeline.
5.  **VERIFY**: Compare row counts and (optionally) checksum-verify a sample.
6.  **DONE**: Close connections and output final report.
