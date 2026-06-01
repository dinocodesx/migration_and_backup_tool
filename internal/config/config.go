package config

import "time"

// Config is the root configuration object for gomigrate.
type Config struct {
	Operation   string            `yaml:"operation"`
	Source      DBConfig          `yaml:"source"`
	Target      DBConfig          `yaml:"target"`
	Concurrency ConcurrencyConfig `yaml:"concurrency"`
	Migration   MigrationConfig   `yaml:"migration"`
	Backup      BackupConfig      `yaml:"backup"`
	Checkpoint  CheckpointConfig  `yaml:"checkpoint"`
	Telemetry   TelemetryConfig   `yaml:"telemetry"`
}

// DBConfig holds connection details for a database.
type DBConfig struct {
	Type     string            `yaml:"type"`
	Host     string            `yaml:"host"`
	Hosts    []string          `yaml:"hosts"` // For Cassandra
	Port     int               `yaml:"port"`
	User     string            `yaml:"user"`
	Password string            `yaml:"password"`
	Database string            `yaml:"database"`
	Keyspace string            `yaml:"keyspace"` // For Cassandra
	Tables   []string          `yaml:"tables"`
	Params   map[string]string `yaml:"params"`
}

// ConcurrencyConfig defines the worker pool parameters.
type ConcurrencyConfig struct {
	NumReaders          int           `yaml:"num_readers"`
	NumTransformers     int           `yaml:"num_transformers"`
	NumWriters          int           `yaml:"num_writers"`
	BatchSize           int           `yaml:"batch_size"`
	BatchTimeout        time.Duration `yaml:"batch_timeout"`
	RateLimitRPS        int           `yaml:"rate_limit_rps"`
	FlushEveryNBatches  int           `yaml:"flush_every_n_batches"` // Checkpoint flush frequency
}

// MigrationConfig defines migration-specific settings.
type MigrationConfig struct {
	SchemaMappingFile string  `yaml:"schema_mapping_file"`
	ConflictStrategy  string  `yaml:"conflict_strategy"`
	VerifyAfter       bool    `yaml:"verify_after"`
	VerifySamplePct   float64 `yaml:"verify_sample_pct"`
}

// BackupConfig defines backup-specific settings.
type BackupConfig struct {
	Format      string          `yaml:"format"`
	Compression string          `yaml:"compression"`
	ChunkSizeMB int             `yaml:"chunk_size_mb"`
	Storage     StorageConfig   `yaml:"storage"`
	Retention   RetentionConfig `yaml:"retention"`
}

// StorageConfig defines where backups are stored.
type StorageConfig struct {
	Type   string `yaml:"type"`
	Bucket string `yaml:"bucket"`
	Prefix string `yaml:"prefix"`
	Region string `yaml:"region"`
}

// RetentionConfig defines how many backups to keep.
type RetentionConfig struct {
	KeepLast    int `yaml:"keep_last"`
	KeepDaily   int `yaml:"keep_daily"`
	KeepWeekly  int `yaml:"keep_weekly"`
	KeepMonthly int `yaml:"keep_monthly"`
}

// CheckpointConfig defines progress tracking settings.
type CheckpointConfig struct {
	Path               string `yaml:"path"`
	FlushEveryNBatches int    `yaml:"flush_every_n_batches"`
}

// TelemetryConfig defines observability settings.
type TelemetryConfig struct {
	LogLevel        string `yaml:"log_level"`
	LogFormat       string `yaml:"log_format"`
	MetricsAddr     string `yaml:"metrics_addr"`
	TracingEndpoint string `yaml:"tracing_endpoint"`
}
