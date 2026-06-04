// Package config defines the internal configuration schema for gomigrate.
// It uses YAML tags for serialization/deserialization, allowing users to
// define complex migration and backup jobs via configuration files.
package config

import "time"

// Config is the root object containing all settings for a gomigrate operation.
type Config struct {
	// Operation is a descriptive name for the current job.
	Operation string `yaml:"operation"`
	// Source contains connection details for the database being read.
	Source DBConfig `yaml:"source"`
	// Target contains connection details for the destination database.
	Target DBConfig `yaml:"target"`
	// Concurrency defines parameters for worker pools and batch sizes.
	Concurrency ConcurrencyConfig `yaml:"concurrency"`
	// Migration contains logic settings for database-to-database transfers.
	Migration MigrationConfig `yaml:"migration"`
	// Backup contains settings for database-to-storage transfers.
	Backup BackupConfig `yaml:"backup"`
	// Checkpoint defines progress tracking and resume settings.
	Checkpoint CheckpointConfig `yaml:"checkpoint"`
	// Telemetry configures logging, metrics, and tracing.
	Telemetry TelemetryConfig `yaml:"telemetry"`
}

// DBConfig encapsulates all possible connection parameters for supported databases.
type DBConfig struct {
	// Type is the database engine identifier (e.g., "postgres", "cassandra").
	Type string `yaml:"type"`
	// Host is the hostname or IP of the database server.
	Host string `yaml:"host"`
	// Hosts is a list of server addresses (specific to Cassandra clusters).
	Hosts []string `yaml:"hosts"`
	// Port is the listening port of the database server.
	Port int `yaml:"port"`
	// User is the username for authentication.
	User string `yaml:"user"`
	// Password is the password for authentication.
	Password string `yaml:"password"`
	// Database is the name of the specific database to use.
	Database string `yaml:"database"`
	// Keyspace is the Cassandra-specific organizational unit.
	Keyspace string `yaml:"keyspace"`
	// Tables is a list of specific tables/collections to include in the operation.
	Tables []string `yaml:"tables"`
	// Params provides a mechanism for passing engine-specific driver options.
	Params map[string]string `yaml:"params"`
}

// ConcurrencyConfig controls the internal worker pool dynamics.
type ConcurrencyConfig struct {
	// NumReaders is the number of concurrent database extraction workers.
	NumReaders int `yaml:"num_readers"`
	// NumTransformers is the number of concurrent data transformation workers.
	NumTransformers int `yaml:"num_transformers"`
	// NumWriters is the number of concurrent data ingestion workers.
	NumWriters int `yaml:"num_writers"`
	// BatchSize is the number of records to group before processing/writing.
	BatchSize int `yaml:"batch_size"`
	// BatchTimeout is the maximum time to wait before flushing a partial batch.
	BatchTimeout time.Duration `yaml:"batch_timeout"`
	// RateLimitRPS limits the number of records processed per second.
	RateLimitRPS int `yaml:"rate_limit_rps"`
	// FlushEveryNBatches determines how often progress is committed to the checkpoint store.
	FlushEveryNBatches int `yaml:"flush_every_n_batches"`
}

// MigrationConfig contains logic-specific settings for direct database transfers.
type MigrationConfig struct {
	// SchemaMappingFile is a path to a configuration defining field transformations.
	SchemaMappingFile string `yaml:"schema_mapping_file"`
	// ConflictStrategy defines behavior when duplicate keys are encountered (e.g., "ignore", "overwrite").
	ConflictStrategy string `yaml:"conflict_strategy"`
	// VerifyAfter enables post-migration data integrity checks.
	VerifyAfter bool `yaml:"verify_after"`
	// VerifySamplePct is the percentage of records to sample for verification.
	VerifySamplePct float64 `yaml:"verify_sample_pct"`
}

// BackupConfig contains settings for exporting data to cloud or local storage.
type BackupConfig struct {
	// Format is the output serialization format (e.g., "parquet", "ndjson").
	Format string `yaml:"format"`
	// Compression is the compression algorithm to use (e.g., "zstd").
	Compression string `yaml:"compression"`
	// ChunkSizeMB is the target size for individual backup files.
	ChunkSizeMB int `yaml:"chunk_size_mb"`
	// Storage defines the destination backend.
	Storage StorageConfig `yaml:"storage"`
	// Retention defines the lifecycle policy for backups.
	Retention RetentionConfig `yaml:"retention"`
}

// StorageConfig configures the interface with a storage provider.
type StorageConfig struct {
	// Type is the provider identifier (e.g., "s3", "gcs", "local").
	Type string `yaml:"type"`
	// Bucket is the name of the cloud storage bucket.
	Bucket string `yaml:"bucket"`
	// Prefix is the base path for backup artifacts.
	Prefix string `yaml:"prefix"`
	// Region is the cloud region (specific to AWS S3).
	Region string `yaml:"region"`
}

// RetentionConfig defines how many historical backups to preserve.
type RetentionConfig struct {
	KeepLast    int `yaml:"keep_last"`
	KeepDaily   int `yaml:"keep_daily"`
	KeepWeekly  int `yaml:"keep_weekly"`
	KeepMonthly int `yaml:"keep_monthly"`
}

// CheckpointConfig configures the resumability features of gomigrate.
type CheckpointConfig struct {
	// Path is the filesystem path to the persistent checkpoint database.
	Path string `yaml:"path"`
	// FlushEveryNBatches determines the frequency of progress persistence.
	FlushEveryNBatches int `yaml:"flush_every_n_batches"`
}

// TelemetryConfig configures observability for the migration process.
type TelemetryConfig struct {
	// LogLevel is the minimum severity to log (e.g., "debug", "info").
	LogLevel string `yaml:"log_level"`
	// LogFormat is the format of log lines (e.g., "json", "console").
	LogFormat string `yaml:"log_format"`
	// MetricsAddr is the address where Prometheus metrics are exposed.
	MetricsAddr string `yaml:"metrics_addr"`
	// TracingEndpoint is the OTLP/Jaeger endpoint for distributed tracing.
	TracingEndpoint string `yaml:"tracing_endpoint"`
}
