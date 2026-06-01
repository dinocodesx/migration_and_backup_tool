# Migration and Backup Tool

A high-performance, concurrent database migration and backup tool written in Go.

## Overview

The Migration and Backup Tool is designed to handle large-scale database operations with high efficiency. It supports migrating data between different database engines, performing backups to multiple storage backends, and restoring data from those backups.

## Key Features

- **High Concurrency**: Utilizes Go's concurrency model for fast data transfer with configurable reader, transformer, and writer pools.
- **Multiple Operations**: Supports `migrate`, `backup`, `restore`, and `verify`.
- **Database Support**: 
    - PostgreSQL
    - MySQL
    - MongoDB (Adapter structure present)
    - Cassandra (Adapter structure present)
- **Backup Formats**:
    - Parquet (High-performance columnar storage)
    - NDJSON (Line-delimited JSON)
- **Storage Backends**:
    - Local File System
    - Amazon S3
    - Google Cloud Storage (GCS)
- **Reliability**: 
    - Checkpointing for resumable operations.
    - Retry logic with circuit breakers.
    - Post-migration/backup verification.
- **Telemetry**: Prometheus metrics and structured logging.

## Installation

### Prerequisites

- Go 1.26 or later

### Build from Source

```bash
git clone https://github.com/dinocodesx/migration_and_backup_tool.git
cd migration_and_backup_tool
make build
```

This will produce a binary named `gomigrate`.

## Usage

Run the tool using a configuration file:

```bash
./gomigrate --config configs/example.yaml
```

## Configuration

The tool is configured via a YAML file. See `configs/example.yaml` for a comprehensive example of all available options.

### Basic Configuration Structure

```yaml
operation: migrate

source:
  type: postgres
  # ... source details

target:
  type: mysql
  # ... target details

concurrency:
  num_readers: 16
  num_writers: 16
  batch_size: 1000
```

## Development

### Running Tests

```bash
# Run unit tests
make test

# Run integration tests
make test-integration
```

### Linting

```bash
make lint
```

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
