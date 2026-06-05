Start the Postgres container:

```bash
docker-compose up -d postgres
```

Seed the database with sample data:

```bash
./scripts/seed_postgres.sh
docker exec -it $(docker-compose ps -q postgres) psql -U user -d testdb -c "SELECT COUNT(*) FROM source_users;"
```

Add the environment variables to the `.env` file and write a proper `config.yaml` file:

### Backup Configuration

To backup your Postgres database to S3, use the following `config.yaml`:

```yaml
operation: backup-to-s3

source:
  type: postgres
  host: localhost
  port: 5432
  user: user
  password: password
  database: testdb
  tables:
    - users
  params:
    sslmode: disable

backup:
  format: parquet # Use 'parquet' for high compression or 'ndjson' for simplicity
  compression: zstd # Recommended compression for Parquet
  chunk_size_mb: 512
  storage:
    type: s3
    bucket: your-bucket-name
    region: us-east-1
    prefix: backups/
```

### Restore Configuration

To restore the backup from S3 into a fresh Postgres instance, update your `config.yaml`:

```yaml
operation: restore-from-s3

target:
  type: postgres
  host: localhost
  port: 5432
  user: user
  password: password
  database: testdb
  params:
    sslmode: disable

backup:
  storage:
    type: s3
    bucket: your-bucket-name
    region: us-east-1
    prefix: backups/
```

### Running the commands

**For Backup:**

```bash
export $(grep -v '^#' .env | xargs)
./gomigrate backup --config config.yaml
```

**For Restore:**

```bash
# Note: --manifest path is relative to the storage prefix
export $(grep -v '^#' .env | xargs)
./gomigrate restore --config config.yaml --manifest manifest.json
```
