package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/dinocodesx/migration_and_backup_tool/internal/adapter/postgres"
	"github.com/dinocodesx/migration_and_backup_tool/internal/checkpoint"
	"github.com/dinocodesx/migration_and_backup_tool/internal/config"
	"github.com/dinocodesx/migration_and_backup_tool/internal/metrics"
	"github.com/dinocodesx/migration_and_backup_tool/internal/pipeline"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile string
	cfg     config.Config
)

var rootCmd = &cobra.Command{
	Use:   "gomigrate",
	Short: "A production-grade database migration and backup tool",
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

		// Initialize adapters (Phase 1: PostgreSQL)
		src := postgres.NewPostgresAdapter()
		if err := src.Connect(ctx, cfg.Source); err != nil {
			return fmt.Errorf("source connect failed: %w", err)
		}
		defer src.Close()

		dst := postgres.NewPostgresAdapter()
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

		orch := pipeline.NewOrchestrator(cfg.Concurrency, store)
		
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
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("backup called")
	},
}

var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore database from backup",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("restore called")
	},
}

var verifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "Verify backup integrity",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("verify called")
	},
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Check status of a checkpoint",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("status called")
	},
}
