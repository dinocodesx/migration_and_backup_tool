package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

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
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("migrate called")
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
