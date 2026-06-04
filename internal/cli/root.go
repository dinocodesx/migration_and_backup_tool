// Package cli provides the command-line interface for gomigrate.
// It uses cobra for command management and viper for configuration.
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

// Version follows semantic versioning for releases.
const Version = "v0.0.1-alpha"

var (
	cfgFile      string        // Path to the configuration file provided via flag.
	manifestFile string        // Path to the manifest file for restore/verify operations.
	cfg          config.Config // Global configuration object populated by Viper.
	logger       *zap.Logger   // Global structured logger.
)

// rootCmd represents the base command when called without any subcommands.
var rootCmd = &cobra.Command{
	Use:     "gomigrate",
	Version: Version,
	Short:   "A production-grade database migration and backup tool",
	Long: `GoMigrate is a concurrent, resumable tool for migrating and backing up
	large-scale database workloads (100M+ records). It supports multiple database
	engines and storage backends with built-in checkpointing.`,
	// PersistentPreRunE executes after flags are parsed but before subcommands run.
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// Unmarshal the viper-loaded config into our internal config struct.
		if err := viper.Unmarshal(&cfg); err != nil {
			return fmt.Errorf("failed to unmarshal config: %w", err)
		}
		// Initialize the logger based on telemetry settings.
		logger = initLogger(cfg.Telemetry)
		return nil
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	// Initialize configuration loading.
	cobra.OnInitialize(initConfig)

	// Global persistent flags.
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./config.yaml)")

	// Register subcommands.
	rootCmd.AddCommand(migrateCmd)
	rootCmd.AddCommand(backupCmd)
	rootCmd.AddCommand(restoreCmd)
	rootCmd.AddCommand(verifyCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(versionCmd)

	// Flag registration for specific subcommands.
	restoreCmd.Flags().StringVar(&manifestFile, "manifest", "manifest.json", "path to backup manifest file")
	verifyCmd.Flags().StringVar(&manifestFile, "manifest", "manifest.json", "path to backup manifest file")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Search for "config.yaml" or "config.json" in the current directory.
		viper.AddConfigPath(".")
		viper.SetConfigName("config")
	}

	viper.AutomaticEnv() // Read in environment variables that match.
	viper.SetEnvPrefix("GOMIGRATE")

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			fmt.Fprintf(os.Stderr, "Error reading config file (%s): %v\n", viper.ConfigFileUsed(), err)
			os.Exit(1)
		}
	} else {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

// initStorage initializes the storage backend (local, s3, gcs) based on configuration.
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

// initSerializer initializes the backup data serializer (parquet, ndjson).
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

// initLogger builds a zap logger from the telemetry config.
func initLogger(tc config.TelemetryConfig) *zap.Logger {
	l, err := telemetry.NewLogger(tc.LogLevel, tc.LogFormat)
	if err != nil {
		// Fallback to a no-op logger to ensure the application continues.
		return zap.NewNop()
	}
	return l
}

// initSourceAdapter creates and connects a source database adapter.
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

// initTargetAdapter creates and connects a target database adapter.
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

// migrateCmd handles database-to-database migrations.
var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Migrate data between databases",
	RunE:  runMigrate,
}

// runMigrate orchestrates the migration process.
func runMigrate(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	defer logger.Sync() //nolint:errcheck

	// 1. Initialize checkpoint store (BoltDB) to track progress.
	cpPath := cfg.Checkpoint.Path
	if cpPath == "" {
		cpPath = "checkpoint.bolt"
	}
	store, err := checkpoint.NewStore(cpPath)
	if err != nil {
		return err
	}
	defer store.Close()

	// 2. Initialize database adapters.
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

	// 3. Select the table to migrate (Phase 1 currently supports one table at a time).
	if len(cfg.Source.Tables) == 0 {
		return fmt.Errorf("no tables specified in source config")
	}
	table := cfg.Source.Tables[0]

	// 4. Fetch source schema and apply it to the target (Table Creation).
	s, err := src.Schema(ctx, table)
	if err != nil {
		return fmt.Errorf("failed to get source schema: %w", err)
	}
	if err := dst.ApplySchema(ctx, s); err != nil {
		return fmt.Errorf("failed to apply schema to target: %w", err)
	}

	// 5. Initialize schema mapper for type conversion between different DB engines.
	mapper := migration.NewSchemaMapper(src.Type(), dst.Type())

	// 6. Setup the orchestrator with concurrency and retry logic.
	orch := pipeline.NewOrchestrator(cfg.Concurrency, store, mapper, logger)

	// 7. Start the metrics server if enabled.
	if cfg.Telemetry.MetricsAddr != "" {
		go func() {
			if err := metrics.StartMetricsServer(cfg.Telemetry.MetricsAddr); err != nil {
				logger.Warn("metrics server failed", zap.Error(err))
			}
		}()
		logger.Info("metrics server started", zap.String("addr", cfg.Telemetry.MetricsAddr))
	}

	// 8. Execute the migration.
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

// backupCmd handles exporting database tables to storage (S3/GCS/Local).
var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Backup database to storage",
	RunE:  runBackup,
}

// runBackup orchestrates the backup process.
func runBackup(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	defer logger.Sync() //nolint:errcheck

	// 1. Initialize source and storage.
	src, err := initSourceAdapter(ctx)
	if err != nil {
		return err
	}
	defer src.Close()

	st, err := initStorage(ctx, cfg.Backup.Storage)
	if err != nil {
		return err
	}

	// 2. Select table and fetch schema.
	if len(cfg.Source.Tables) == 0 {
		return fmt.Errorf("no tables specified in source config")
	}
	table := cfg.Source.Tables[0]

	s, err := src.Schema(ctx, table)
	if err != nil {
		return fmt.Errorf("failed to get source schema: %w", err)
	}

	// 3. Initialize serializer (Parquet/NDJSON).
	batchSize := cfg.Concurrency.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}
	ser, err := initSerializer(cfg.Backup.Format, s, batchSize)
	if err != nil {
		return err
	}

	// 4. Run the backup engine.
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

// restoreCmd handles importing data from storage back into a database.
var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore database from backup",
	RunE:  runRestore,
}

// runRestore orchestrates the restoration process.
func runRestore(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	defer logger.Sync() //nolint:errcheck

	// 1. Initialize target database and storage source.
	dst, err := initTargetAdapter(ctx)
	if err != nil {
		return err
	}
	defer dst.Close()

	st, err := initStorage(ctx, cfg.Backup.Storage)
	if err != nil {
		return err
	}

	// 2. Execute restoration using the manifest roadmap.
	engine := backup.NewRestoreEngine(st, dst, logger)
	logger.Info("starting restore", zap.String("manifest", manifestFile))

	if err := engine.Restore(ctx, manifestFile); err != nil {
		return fmt.Errorf("restore failed: %w", err)
	}

	logger.Info("restore completed successfully")
	return nil
}

// verifyCmd checks if the backup files in storage match the manifest metadata.
var verifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "Verify backup integrity",
	RunE:  runVerify,
}

// runVerify performs integrity checks on existing backups.
func runVerify(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	st, err := initStorage(ctx, cfg.Backup.Storage)
	if err != nil {
		return err
	}

	// 1. Load and decode the manifest.
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

	// 2. Check existence of every chunk file listed in the manifest.
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

// statusCmd is a placeholder for checking operation checkpoints.
var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Check status of a checkpoint",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("status called")
	},
}

// versionCmd prints the application version.
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of gomigrate",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("gomigrate %s\n", Version)
	},
}
