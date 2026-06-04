# Database Backup & Restore: Deep Dive

This document provides a technical explanation of how `gomigrate` handles database backups, how to configure them for maximum performance, and how to recover from failures.

---

## 1. Internal Architecture

The backup process is designed as a streaming pipeline to ensure minimal memory footprint even when handling terabytes of data.

```text
[ Source DB ] -> [ Reader Pool ] -> [ Chunk Manager ] -> [ Serializer ] -> [ Compressor ] -> [ Storage ]
```

1.  **Partitioning**: `gomigrate` first analyzes the table (e.g., using primary keys or row IDs) to split it into logical "partitions".
2.  **Parallel Reads**: Multiple readers fetch data from these partitions concurrently.
3.  **Streaming Serialization**: As records arrive, they are passed to the `Serializer` (Parquet or NDJSON).
4.  **Chunking**: Once a chunk reaches a certain size (e.g., 512MB), it is flushed to the storage backend.
5.  **Manifesting**: A final JSON file is generated containing the roadmap for restoration.

---

## 2. Advanced Configuration

### Backup Strategy

```yaml
operation: backup
backup:
  format: parquet # Columnar, highly compressed, best for large data.
  compression: snappy # snappy, gzip, or zstd.
  chunk_size_mb: 512 # Size of each uploaded file.
```

### Storage Backends

#### Local Storage

Fastest for local testing, but lacks redundancy.

- **Config**: `path: "/data/backups"`
- **Common Error**: `no space left on device`. Ensure your target partition has at least 1.5x the DB size available (for temporary buffers).

#### Amazon S3

- **Required Env**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`.
- **Troubleshooting**:
  - `403 Forbidden`: Check IAM policy. It needs `s3:PutObject`, `s3:GetObject`, and `s3:ListBucket`.
  - `RequestTimeTooSkewed`: Your system clock is out of sync with AWS. Run `ntpdate`.

#### Google Cloud Storage (GCS)

- **Required Env**: `GOOGLE_APPLICATION_CREDENTIALS` (Path to JSON).
- **Troubleshooting**:
  - `Project ID mismatch`: Ensure the JSON key matches the bucket's project.

---

## 3. The Checkpoint System (Resumability)

`gomigrate` uses an embedded **BoltDB** database to track progress. If a backup fails, you can restart it, and it will pick up from the last successful partition.

### How it works:

- Every time a partition is finished, a record is written to `checkpoint.bolt`.
- On restart, `gomigrate` reads this file and skips already completed partitions.

### Troubleshooting Checkpoints:

- **Corrupted BoltDB**: If the process crashes violently, the `.bolt` file might be locked.
  - **Fix**: Delete the `.bolt` file to start fresh (Caution: this restarts the entire backup).
- **Stale Checkpoints**: If you change the source table schema but use an old checkpoint, the backup might fail.
  - **Fix**: Always use a clean checkpoint path for new major schema changes.

---

## 4. Performance Tuning

| Parameter       | Recommended Value | Impact                                                               |
| :-------------- | :---------------- | :------------------------------------------------------------------- |
| `num_readers`   | 2x - 4x CPU Cores | Increases DB read throughput.                                        |
| `batch_size`    | 1000 - 5000       | Balances memory usage vs. network roundtrips.                        |
| `chunk_size_mb` | 128 - 1024        | Larger chunks are more efficient but require more RAM during upload. |

---

## 5. What Can Go Wrong & How to Fix It

### Error: `failed to get schema`

- **Cause**: The database user lacks permission to read metadata tables (e.g., `information_schema`).
- **Fix**: Grant `SELECT` on the target tables and `USAGE` on the schema.

### Error: `serialize error: unexpected type`

- **Cause**: A column contains data that doesn't map to Parquet/JSON (e.g., custom Postgres types).
- **Fix**: Use the `migration.schema_mapper` to define custom type mappings.

### Error: `upload failed: slow network`

- **Cause**: Network timeout during chunk upload to S3/GCS.
- **Fix**: Decrease `chunk_size_mb` to 64MB or increase the `timeout` in your storage config.

### Error: `checksum mismatch during verify`

- **Cause**: Data was corrupted during transit or the storage provider altered the file.
- **Fix**: Check for transparent proxy/firewall interference. Re-run the backup for that specific chunk.

---

## 6. Restoration Guide

Restoration is the inverse of backup:

1.  Read `manifest.json`.
2.  Download chunks.
3.  Deserialize and write to target database.

**Pro-tip**: When restoring to a fresh database, `gomigrate` will attempt to create the tables for you based on the schema snapshot saved in the manifest. Ensure the target user has `CREATE TABLE` permissions.
