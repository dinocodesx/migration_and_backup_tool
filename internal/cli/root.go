// Package cli implements the command-line interface for the gomigrate tool.
// It uses Cobra for command-line parsing and Viper for flexible configuration
// management (supporting environment variables and YAML/JSON files).
package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/dinocodesx/gomigrate/internal/adapter"
	"github.com/dinocodesx/gomigrate/internal/adapter/factory"
	"github.com/dinocodesx/gomigrate/internal/backup"
	"github.com/dinocodesx/gomigrate/internal/checkpoint"
	"github.com/dinocodesx/gomigrate/internal/config"
	"github.com/dinocodesx/gomigrate/internal/metrics"
	"github.com/dinocodesx/gomigrate/internal/migration"
	"github.com/dinocodesx/gomigrate/internal/pipeline"
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
		return nil
	},
}

// Execute triggers the Cobra command execution pipeline.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
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
	rootCmd.AddCommand(versionCmd)

	restoreCmd.Flags().StringVar(&manifestFile, "manifest", "manifest.json", "path to backup manifest file")
	verifyCmd.Flags().StringVar(&manifestFile, "manifest", "manifest.json", "path to backup manifest file")
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

// migrateCmd implements the database-to-database migration command.
var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Migrate data between databases",
	RunE:  runMigrate,
}

// runMigrate orchestrates the migration execution flow.
func runMigrate(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	defer logger.Sync()

	cpPath := cfg.Checkpoint.Path
	if cpPath == "" {
		cpPath = "checkpoint.bolt"
	}
	store, err := checkpoint.NewStore(cpPath)
	if err != nil {
		return err
	}
	defer store.Close()

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

	s, err := src.Schema(ctx, table)
	if err != nil {
		return fmt.Errorf("failed to get source schema: %w", err)
	}
	if err := dst.ApplySchema(ctx, s); err != nil {
		return fmt.Errorf("failed to apply schema to target: %w", err)
	}

	mapper := migration.NewSchemaMapper(src.Type(), dst.Type())
	orch := pipeline.NewOrchestrator(cfg.Concurrency, store, mapper, logger)

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

	if err := orch.Migrate(ctx, opID, src, dst, table); err != nil {
		return fmt.Errorf("migration failed: %w", err)
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
	defer logger.Sync()

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

	manifest, err := engine.Backup(ctx, opID, src, table, chunkSize)
	if err != nil {
		return fmt.Errorf("backup failed: %w", err)
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
	defer logger.Sync()

	dst, err := initTargetAdapter(ctx)
	if err != nil {
		return err
	}
	defer dst.Close()

	st, err := initStorage(ctx, cfg.Backup.Storage)
	if err != nil {
		return err
	}

	engine := backup.NewRestoreEngine(st, dst, logger)
	logger.Info("starting restore", zap.String("manifest", manifestFile))

	if err := engine.Restore(ctx, manifestFile); err != nil {
		return fmt.Errorf("restore failed: %w", err)
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

// statusCmd is a reserved subcommand for future progress monitoring features.
var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Check status of a checkpoint",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("status called")
	},
}

// versionCmd displays the current version information.
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of gomigrate",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("gomigrate %s\n", Version)
	},
}
