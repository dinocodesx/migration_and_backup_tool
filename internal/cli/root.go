package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/dinocodesx/gomigrate/internal/adapter/factory"
	"github.com/dinocodesx/gomigrate/internal/backup"
	"github.com/dinocodesx/gomigrate/internal/checkpoint"
	"github.com/dinocodesx/gomigrate/internal/config"
	"github.com/dinocodesx/gomigrate/internal/metrics"
	"github.com/dinocodesx/gomigrate/internal/migration"
	"github.com/dinocodesx/gomigrate/internal/pipeline"
	"github.com/dinocodesx/gomigrate/internal/schema"
	"github.com/dinocodesx/gomigrate/internal/storage"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const Version = "v0.0.1-alpha"

var (
	cfgFile      string
	manifestFile string
	cfg          config.Config
)

var rootCmd = &cobra.Command{
	Use:     "gomigrate",
	Version: Version,
	Short:   "A production-grade database migration and backup tool",
	Long: `GoMigrate is a concurrent, resumable tool for migrating and backing up
large-scale database workloads (100M+ records).`,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./config.yaml)")

	// Add subcommands
	rootCmd.AddCommand(migrateCmd)
	rootCmd.AddCommand(backupCmd)
	rootCmd.AddCommand(restoreCmd)
	rootCmd.AddCommand(verifyCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(versionCmd)

	restoreCmd.Flags().StringVar(&manifestFile, "manifest", "manifest.json", "path to backup manifest file")
	verifyCmd.Flags().StringVar(&manifestFile, "manifest", "manifest.json", "path to backup manifest file")
}

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

func initSerializer(format string, s *schema.Schema) (backup.Serializer, error) {
	switch format {
	case "parquet":
		return backup.NewParquetSerializer(s)
	case "ndjson":
		return backup.NewNDJSONSerializer(), nil
	default:
		return nil, fmt.Errorf("unsupported backup format: %s", format)
	}
}

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Migrate data between databases",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := viper.Unmarshal(&cfg); err != nil {
			return fmt.Errorf("failed to unmarshal config: %w", err)
		}

		ctx := context.Background()

		// Initialize checkpoint store
		cpPath := cfg.Checkpoint.Path
		if cpPath == "" {
			cpPath = "checkpoint.bolt"
		}
		store, err := checkpoint.NewStore(cpPath)
		if err != nil {
			return err
		}
		defer store.Close()

		src, err := factory.NewSourceAdapter(cfg.Source.Type)
		if err != nil {
			return err
		}
		if err := src.Connect(ctx, cfg.Source); err != nil {
			return fmt.Errorf("source connect failed: %w", err)
		}
		defer src.Close()

		dst, err := factory.NewTargetAdapter(cfg.Target.Type)
		if err != nil {
			return err
		}
		if err := dst.Connect(ctx, cfg.Target); err != nil {
			return fmt.Errorf("target connect failed: %w", err)
		}
		defer dst.Close()

		// For Phase 1, we assume we're migrating the first table in the list
		if len(cfg.Source.Tables) == 0 {
			return fmt.Errorf("no tables specified in source config")
		}
		table := cfg.Source.Tables[0]

		// Apply schema to target
		s, err := src.Schema(ctx, table)
		if err != nil {
			return fmt.Errorf("failed to get source schema: %w", err)
		}
		if err := dst.ApplySchema(ctx, s); err != nil {
			return fmt.Errorf("failed to apply schema to target: %w", err)
		}

		// Initialize mapper
		mapper := migration.NewSchemaMapper(src.Type(), dst.Type())

		orch := pipeline.NewOrchestrator(cfg.Concurrency, store, mapper)

		// Start metrics server
		if cfg.Telemetry.MetricsAddr != "" {
			go func() {
				if err := metrics.StartMetricsServer(cfg.Telemetry.MetricsAddr); err != nil {
					fmt.Printf("Warning: metrics server failed: %v\n", err)
				}
			}()
			fmt.Printf("Metrics server started at %s\n", cfg.Telemetry.MetricsAddr)
		}

		opID := fmt.Sprintf("mig-%d", os.Getpid())
		fmt.Printf("Starting migration of table %s (OpID: %s)...\n", table, opID)

		if err := orch.Migrate(ctx, opID, src, dst, table); err != nil {
			return fmt.Errorf("migration failed: %w", err)
		}

		fmt.Println("Migration completed successfully!")
		return nil
	},
}

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Backup database to storage",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := viper.Unmarshal(&cfg); err != nil {
			return fmt.Errorf("failed to unmarshal config: %w", err)
		}

		ctx := context.Background()

		src, err := factory.NewSourceAdapter(cfg.Source.Type)
		if err != nil {
			return err
		}
		if err := src.Connect(ctx, cfg.Source); err != nil {
			return fmt.Errorf("source connect failed: %w", err)
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

		ser, err := initSerializer(cfg.Backup.Format, s)
		if err != nil {
			return err
		}

		engine := backup.NewEngine(st, ser)
		opID := fmt.Sprintf("bak-%d", os.Getpid())
		fmt.Printf("Starting backup of table %s (OpID: %s)...\n", table, opID)

		chunkSize := int64(cfg.Backup.ChunkSizeMB) * 1024 * 1024
		if chunkSize == 0 {
			chunkSize = 512 * 1024 * 1024 // Default 512MB
		}

		manifest, err := engine.Backup(ctx, opID, src, table, chunkSize)
		if err != nil {
			return fmt.Errorf("backup failed: %w", err)
		}

		fmt.Printf("Backup completed successfully! Row count: %d, Chunks: %d\n", manifest.RowCount, len(manifest.Chunks))
		return nil
	},
}

var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore database from backup",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := viper.Unmarshal(&cfg); err != nil {
			return fmt.Errorf("failed to unmarshal config: %w", err)
		}

		ctx := context.Background()

		dst, err := factory.NewTargetAdapter(cfg.Target.Type)
		if err != nil {
			return err
		}
		if err := dst.Connect(ctx, cfg.Target); err != nil {
			return fmt.Errorf("target connect failed: %w", err)
		}
		defer dst.Close()

		st, err := initStorage(ctx, cfg.Backup.Storage)
		if err != nil {
			return err
		}

		engine := backup.NewRestoreEngine(st, dst)
		fmt.Printf("Starting restore from %s...\n", manifestFile)

		if err := engine.Restore(ctx, manifestFile); err != nil {
			return fmt.Errorf("restore failed: %w", err)
		}

		fmt.Println("Restore completed successfully!")
		return nil
	},
}

var verifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "Verify backup integrity",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := viper.Unmarshal(&cfg); err != nil {
			return fmt.Errorf("failed to unmarshal config: %w", err)
		}

		ctx := context.Background()
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
	},
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Check status of a checkpoint",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("status called")
	},
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of gomigrate",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("gomigrate %s\n", Version)
	},
}
