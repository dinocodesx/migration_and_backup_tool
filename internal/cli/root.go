// Package cli implements the command-line interface for the gomigrate tool.
// It uses Cobra for command-line parsing and Viper for flexible configuration
// management (supporting environment variables and YAML/JSON files).
package cli

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"text/tabwriter"
	"time"

	"github.com/dinocodesx/gomigrate/internal/adapter"
	"github.com/dinocodesx/gomigrate/internal/adapter/factory"
	"github.com/dinocodesx/gomigrate/internal/backup"
	"github.com/dinocodesx/gomigrate/internal/checkpoint"
	"github.com/dinocodesx/gomigrate/internal/config"
	"github.com/dinocodesx/gomigrate/internal/errs"
	"github.com/dinocodesx/gomigrate/internal/metrics"
	"github.com/dinocodesx/gomigrate/internal/migration"
	"github.com/dinocodesx/gomigrate/internal/pipeline"
	"github.com/dinocodesx/gomigrate/internal/record"
	"github.com/dinocodesx/gomigrate/internal/schema"
	"github.com/dinocodesx/gomigrate/internal/storage"
	"github.com/dinocodesx/gomigrate/internal/telemetry"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// Version is the current semantic version of the gomigrate application.
const Version = "v0.0.1-alpha"

var (
	cfgFile      string
	manifestFile string
	cfg          config.Config
	logger       *zap.Logger

	// tracerShutdown is called on process exit to flush OTel spans.
	tracerShutdown telemetry.ShutdownFunc

	// auditLog is the global structured audit trail.
	auditLog *telemetry.AuditLog
)

// rootCmd is the entry point for the gomigrate CLI. It defines global flags
// and persistent pre-run logic for all subcommands.
var rootCmd = &cobra.Command{
	Use:     "gomigrate",
	Version: Version,
	Short:   "A production-grade database migration and backup tool",
	Long: `GoMigrate is a concurrent, resumable tool for migrating and backing up
	large-scale database workloads. It supports multiple database
	engines and storage backends with built-in checkpointing.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if err := viper.Unmarshal(&cfg); err != nil {
			return fmt.Errorf("failed to unmarshal config: %w", err)
		}
		logger = initLogger(cfg.Telemetry)

		// Initialise the OpenTelemetry tracer (no-op if endpoint is empty).
		telemetry.SetVersion(Version)
		var err error
		tracerShutdown, err = telemetry.InitTracer(
			cmd.Context(),
			cfg.Telemetry.TracingEndpoint,
			"gomigrate",
		)
		if err != nil {
			logger.Warn("failed to initialise tracer", zap.Error(err))
			tracerShutdown = func(_ context.Context) error { return nil }
		}

		// Initialise the audit log alongside the checkpoint file.
		cpPath := cfg.Checkpoint.Path
		if cpPath == "" {
			cpPath = "checkpoint.bolt"
		}
		auditPath := filepath.Join(filepath.Dir(cpPath), "audit.jsonl")
		al, err := telemetry.NewAuditLog(auditPath)
		if err != nil {
			logger.Warn("failed to open audit log", zap.Error(err), zap.String("path", auditPath))
		} else {
			auditLog = al
		}

		return nil
	},
}

// Execute triggers the Cobra command execution pipeline.
func Execute() {
	ctx := context.Background()
	defer func() {
		if tracerShutdown != nil {
			_ = tracerShutdown(ctx)
		}
		if auditLog != nil {
			_ = auditLog.Close()
		}
	}()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./config.yaml)")

	rootCmd.AddCommand(migrateCmd)
	rootCmd.AddCommand(backupCmd)
	rootCmd.AddCommand(restoreCmd)
	rootCmd.AddCommand(verifyCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(replayCmd)
	rootCmd.AddCommand(versionCmd)

	restoreCmd.Flags().StringVar(&manifestFile, "manifest", "manifest.json", "path to backup manifest file")
	verifyCmd.Flags().StringVar(&manifestFile, "manifest", "manifest.json", "path to backup manifest file")
	replayCmd.Flags().String("failed-file", "", "path to the .ndjson file containing failed records")
	statusCmd.Flags().String("checkpoint", "", "path to checkpoint bolt file (overrides config)")
}

// initConfig reads in the configuration file and maps environment variables.
func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		viper.AddConfigPath(".")
		viper.SetConfigName("config")
	}

	viper.AutomaticEnv()
	viper.SetEnvPrefix("GOMIGRATE")

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			fmt.Fprintf(os.Stderr, "Error reading config file (%s): %v\n", viper.ConfigFileUsed(), err)
			os.Exit(1)
		}
	} else {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

// configHash returns a hex SHA-256 of the marshalled config (secrets redacted).
func configHash() string {
	redacted := cfg
	redacted.Source.Password = "[REDACTED]"
	redacted.Target.Password = "[REDACTED]"
	b, _ := json.Marshal(redacted)
	sum := sha256.Sum256(b)
	return fmt.Sprintf("%x", sum)
}

// initStorage initializes the storage provider based on user configuration.
func initStorage(ctx context.Context, sc config.StorageConfig) (storage.Storage, error) {
	switch sc.Type {
	case "local":
		prefix := sc.Prefix
		if prefix == "" {
			prefix = "backups/"
		}
		return storage.NewLocalStorage(prefix)
	case "s3":
		return storage.NewS3Storage(ctx, sc.Bucket, sc.Prefix, sc.Region)
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", sc.Type)
	}
}

// initSerializer initializes the requested data serializer.
func initSerializer(format string, s *schema.Schema, batchSize int) (backup.Serializer, error) {
	switch format {
	case "parquet":
		return backup.NewParquetSerializer(s, batchSize)
	case "ndjson", "":
		return backup.NewNDJSONSerializer(), nil
	default:
		return nil, fmt.Errorf("unsupported backup format: %s", format)
	}
}

// initLogger initializes the global structured logger.
func initLogger(tc config.TelemetryConfig) *zap.Logger {
	l, err := telemetry.NewLogger(tc.LogLevel, tc.LogFormat)
	if err != nil {
		return zap.NewNop()
	}
	return l
}

// initSourceAdapter creates and connects to the source database.
func initSourceAdapter(ctx context.Context) (adapter.SourceAdapter, error) {
	src, err := factory.NewSourceAdapter(cfg.Source.Type)
	if err != nil {
		return nil, err
	}
	if err := src.Connect(ctx, cfg.Source); err != nil {
		return nil, fmt.Errorf("source connect failed: %w", err)
	}
	return src, nil
}

// initTargetAdapter creates and connects to the destination database.
func initTargetAdapter(ctx context.Context) (adapter.TargetAdapter, error) {
	dst, err := factory.NewTargetAdapter(cfg.Target.Type)
	if err != nil {
		return nil, err
	}
	if err := dst.Connect(ctx, cfg.Target); err != nil {
		return nil, fmt.Errorf("target connect failed: %w", err)
	}
	return dst, nil
}

// writeAuditEntry writes a start/end audit entry, logging errors but not
// propagating them so that an audit failure never blocks the operation.
func writeAuditEntry(entry telemetry.AuditEntry) {
	if auditLog == nil {
		return
	}
	if err := auditLog.Write(entry); err != nil {
		logger.Warn("failed to write audit entry", zap.Error(err))
	}
}

// migrateCmd implements the database-to-database migration command.
var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Migrate data between databases",
	RunE:  runMigrate,
}

// runMigrate orchestrates the migration execution flow.
func runMigrate(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	defer logger.Sync() //nolint:errcheck

	cpPath := cfg.Checkpoint.Path
	if cpPath == "" {
		cpPath = "checkpoint.bolt"
	}
	store, err := checkpoint.NewStore(cpPath)
	if err != nil {
		return err
	}
	defer store.Close()

	dlqPath := fmt.Sprintf("%s_failed.ndjson", cpPath)
	dlq, err := errs.NewDLQ(dlqPath)
	if err != nil {
		return err
	}
	defer dlq.Close()

	src, err := initSourceAdapter(ctx)
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := initTargetAdapter(ctx)
	if err != nil {
		return err
	}
	defer dst.Close()

	if len(cfg.Source.Tables) == 0 {
		return fmt.Errorf("no tables specified in source config")
	}
	table := cfg.Source.Tables[0]

	startTime := time.Now()
	writeAuditEntry(telemetry.AuditEntry{
		Operation:  "migrate",
		User:       telemetry.CurrentUser(),
		Hostname:   telemetry.CurrentHostname(),
		Source:     fmt.Sprintf("%s/%s", cfg.Source.Type, cfg.Source.Host),
		Target:     fmt.Sprintf("%s/%s", cfg.Target.Type, cfg.Target.Host),
		Tables:     cfg.Source.Tables,
		StartTime:  startTime,
		ConfigHash: configHash(),
	})

	s, err := src.Schema(ctx, table)
	if err != nil {
		return fmt.Errorf("failed to get source schema: %w", err)
	}
	if err := dst.ApplySchema(ctx, s); err != nil {
		return fmt.Errorf("failed to apply schema to target: %w", err)
	}

	mapper := migration.NewSchemaMapper(src.Type(), dst.Type(), cfg.Migration.Masking)
	orch := pipeline.NewOrchestrator(cfg.Concurrency, store, mapper, dlq, logger)

	if cfg.Telemetry.MetricsAddr != "" {
		go func() {
			if err := metrics.StartMetricsServer(cfg.Telemetry.MetricsAddr); err != nil {
				logger.Warn("metrics server failed", zap.Error(err))
			}
		}()
		logger.Info("metrics server started", zap.String("addr", cfg.Telemetry.MetricsAddr))
	}

	opID := fmt.Sprintf("mig-%d", os.Getpid())
	logger.Info("starting migration",
		zap.String("table", table),
		zap.String("operation_id", opID),
	)

	migErr := orch.Migrate(ctx, opID, src, dst, table)

	outcome := "success"
	errMsg := ""
	if migErr != nil {
		outcome = "failure"
		errMsg = migErr.Error()
	}
	writeAuditEntry(telemetry.AuditEntry{
		Operation: "migrate",
		User:      telemetry.CurrentUser(),
		Hostname:  telemetry.CurrentHostname(),
		Source:    fmt.Sprintf("%s/%s", cfg.Source.Type, cfg.Source.Host),
		Target:    fmt.Sprintf("%s/%s", cfg.Target.Type, cfg.Target.Host),
		Tables:    cfg.Source.Tables,
		StartTime: startTime,
		EndTime:   time.Now(),
		Outcome:   outcome,
		Error:     errMsg,
	})

	if migErr != nil {
		return fmt.Errorf("migration failed: %w", migErr)
	}

	logger.Info("migration completed successfully")
	return nil
}

// backupCmd implements the database-to-storage export command.
var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Backup database to storage",
	RunE:  runBackup,
}

// runBackup orchestrates the export execution flow.
func runBackup(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	defer logger.Sync() //nolint:errcheck

	src, err := initSourceAdapter(ctx)
	if err != nil {
		return err
	}
	defer src.Close()

	st, err := initStorage(ctx, cfg.Backup.Storage)
	if err != nil {
		return err
	}

	if len(cfg.Source.Tables) == 0 {
		return fmt.Errorf("no tables specified in source config")
	}
	table := cfg.Source.Tables[0]

	startTime := time.Now()
	writeAuditEntry(telemetry.AuditEntry{
		Operation:  "backup",
		User:       telemetry.CurrentUser(),
		Hostname:   telemetry.CurrentHostname(),
		Source:     fmt.Sprintf("%s/%s", cfg.Source.Type, cfg.Source.Host),
		Tables:     cfg.Source.Tables,
		StartTime:  startTime,
		ConfigHash: configHash(),
	})

	s, err := src.Schema(ctx, table)
	if err != nil {
		return fmt.Errorf("failed to get source schema: %w", err)
	}

	batchSize := cfg.Concurrency.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}
	ser, err := initSerializer(cfg.Backup.Format, s, batchSize)
	if err != nil {
		return err
	}

	numReaders := cfg.Concurrency.NumReaders
	engine := backup.NewEngine(st, ser, logger, numReaders)
	opID := fmt.Sprintf("bak-%d", os.Getpid())
	logger.Info("starting backup", zap.String("table", table), zap.String("operation_id", opID))

	chunkSize := int64(cfg.Backup.ChunkSizeMB) * 1024 * 1024

	manifest, backupErr := engine.Backup(ctx, opID, src, table, chunkSize)

	outcome := "success"
	errMsg := ""
	var rowCount int64
	if backupErr != nil {
		outcome = "failure"
		errMsg = backupErr.Error()
	} else {
		rowCount = manifest.RowCount
	}
	writeAuditEntry(telemetry.AuditEntry{
		Operation: "backup",
		User:      telemetry.CurrentUser(),
		Hostname:  telemetry.CurrentHostname(),
		Source:    fmt.Sprintf("%s/%s", cfg.Source.Type, cfg.Source.Host),
		Tables:    cfg.Source.Tables,
		StartTime: startTime,
		EndTime:   time.Now(),
		Outcome:   outcome,
		RowCount:  rowCount,
		Error:     errMsg,
	})

	if backupErr != nil {
		return fmt.Errorf("backup failed: %w", backupErr)
	}

	logger.Info("backup completed",
		zap.Int64("row_count", manifest.RowCount),
		zap.Int("chunks", len(manifest.Chunks)),
	)
	return nil
}

// restoreCmd implements the storage-to-database import command.
var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore database from backup",
	RunE:  runRestore,
}

// runRestore orchestrates the import execution flow.
func runRestore(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	defer logger.Sync() //nolint:errcheck

	dst, err := initTargetAdapter(ctx)
	if err != nil {
		return err
	}
	defer dst.Close()

	st, err := initStorage(ctx, cfg.Backup.Storage)
	if err != nil {
		return err
	}

	startTime := time.Now()
	writeAuditEntry(telemetry.AuditEntry{
		Operation:  "restore",
		User:       telemetry.CurrentUser(),
		Hostname:   telemetry.CurrentHostname(),
		Target:     fmt.Sprintf("%s/%s", cfg.Target.Type, cfg.Target.Host),
		StartTime:  startTime,
		ConfigHash: configHash(),
	})

	engine := backup.NewRestoreEngine(st, dst, logger)
	logger.Info("starting restore", zap.String("manifest", manifestFile))

	restoreErr := engine.Restore(ctx, manifestFile)

	outcome := "success"
	errMsg := ""
	if restoreErr != nil {
		outcome = "failure"
		errMsg = restoreErr.Error()
	}
	writeAuditEntry(telemetry.AuditEntry{
		Operation: "restore",
		User:      telemetry.CurrentUser(),
		Hostname:  telemetry.CurrentHostname(),
		Target:    fmt.Sprintf("%s/%s", cfg.Target.Type, cfg.Target.Host),
		StartTime: startTime,
		EndTime:   time.Now(),
		Outcome:   outcome,
		Error:     errMsg,
	})

	if restoreErr != nil {
		return fmt.Errorf("restore failed: %w", restoreErr)
	}

	logger.Info("restore completed successfully")
	return nil
}

// verifyCmd implements the backup integrity verification command.
var verifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "Verify backup integrity",
	RunE:  runVerify,
}

// runVerify performs consistency checks against a backup manifest.
func runVerify(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	st, err := initStorage(ctx, cfg.Backup.Storage)
	if err != nil {
		return err
	}

	fmt.Printf("Verifying backup at %s...\n", manifestFile)
	reader, err := st.Get(ctx, manifestFile)
	if err != nil {
		return err
	}
	defer reader.Close()

	var manifest backup.Manifest
	if err := json.NewDecoder(reader).Decode(&manifest); err != nil {
		return err
	}

	fmt.Printf("Manifest valid. Backup created at: %s, Rows: %d, Chunks: %d\n",
		manifest.CreatedAt.Format(time.RFC3339), manifest.RowCount, len(manifest.Chunks))

	for _, chunk := range manifest.Chunks {
		fmt.Printf("  Checking chunk %d (%s)... ", chunk.Index, chunk.File)
		exists, err := st.Exists(ctx, chunk.File)
		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
			continue
		}
		if !exists {
			fmt.Println("MISSING")
			continue
		}
		fmt.Println("OK")
	}

	return nil
}

// replayCmd implements the dead-letter queue re-ingestion command.
var replayCmd = &cobra.Command{
	Use:   "replay",
	Short: "Replay failed records from a DLQ file",
	RunE:  runReplay,
}

func runReplay(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	defer logger.Sync() //nolint:errcheck

	failedFile, _ := cmd.Flags().GetString("failed-file")
	if failedFile == "" {
		return fmt.Errorf("--failed-file is required")
	}

	f, err := os.Open(failedFile)
	if err != nil {
		return err
	}
	defer f.Close()

	dst, err := initTargetAdapter(ctx)
	if err != nil {
		return err
	}
	defer dst.Close()

	logger.Info("replaying failed records", zap.String("file", failedFile))

	dec := json.NewDecoder(f)
	var batch []*record.Record
	batchSize := cfg.Concurrency.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	for dec.More() {
		var dlqRec errs.DLQRecord
		if err := dec.Decode(&dlqRec); err != nil {
			return err
		}

		rec := &record.Record{
			ID:   dlqRec.RecordID,
			Data: dlqRec.Payload,
			Metadata: record.RecordMetadata{
				SourceTable: dlqRec.Table,
			},
		}
		batch = append(batch, rec)

		if len(batch) >= batchSize {
			if _, err := dst.WriteBatch(ctx, batch); err != nil {
				logger.Error("failed to replay batch", zap.Error(err))
			}
			batch = nil
		}
	}

	if len(batch) > 0 {
		if _, err := dst.WriteBatch(ctx, batch); err != nil {
			logger.Error("failed to replay final batch", zap.Error(err))
		}
	}

	logger.Info("replay completed")
	return nil
}

// statusCmd reads a checkpoint file and renders a human-readable progress table.
var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Check status of an operation checkpoint",
	RunE:  runStatus,
}

// runStatus prints a progress table from a bbolt checkpoint file.
func runStatus(cmd *cobra.Command, args []string) error {
	cpPath, _ := cmd.Flags().GetString("checkpoint")
	if cpPath == "" {
		cpPath = cfg.Checkpoint.Path
	}
	if cpPath == "" {
		cpPath = "checkpoint.bolt"
	}

	store, err := checkpoint.NewStore(cpPath)
	if err != nil {
		return fmt.Errorf("cannot open checkpoint at %q: %w", cpPath, err)
	}
	defer store.Close()

	// We list all partitions by scanning a synthetic prefix. Since we don't
	// know the operation IDs up front, we scan with an empty prefix.
	// The bbolt store's ListPartitions needs an opID; use an empty string
	// to trigger a full bucket scan (all keys start with "").
	partitions, err := store.ListPartitions("")
	if err != nil {
		return fmt.Errorf("failed to list partitions: %w", err)
	}

	if len(partitions) == 0 {
		fmt.Println("No checkpoint data found.")
		return nil
	}

	// Group by a synthetic "operation" (partition ID prefix up to first ':')
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "PARTITION ID\tSTATUS\tROWS WRITTEN\tERRORS\tLAST UPDATED")
	fmt.Fprintln(w, "────────────\t──────\t────────────\t──────\t────────────")
	for _, p := range partitions {
		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%s\n",
			p.PartitionID,
			p.Status,
			p.RowsWritten,
			p.ErrorCount,
			p.UpdatedAt.Format(time.RFC3339),
		)
	}
	_ = w.Flush()
	return nil
}

// versionCmd displays the current version information.
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of gomigrate",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("gomigrate %s\n", Version)
	},
}
