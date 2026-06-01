package integration

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/dinocodesx/migration_and_backup_tool/internal/adapter/postgres"
	"github.com/dinocodesx/migration_and_backup_tool/internal/checkpoint"
	"github.com/dinocodesx/migration_and_backup_tool/internal/config"
	"github.com/dinocodesx/migration_and_backup_tool/internal/pipeline"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	pgmodule "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestPostgresToPostgresMigration(t *testing.T) {
	ctx := context.Background()

	// 1. Spin up Postgres container
	pgContainer, err := pgmodule.Run(ctx,
		"postgres:15-alpine",
		pgmodule.WithDatabase("testdb"),
		pgmodule.WithUsername("user"),
		pgmodule.WithPassword("password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second)),
	)
	if err != nil {
		t.Fatalf("failed to start container: %v", err)
	}
	defer pgContainer.Terminate(ctx)

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	db, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("failed to connect to db: %v", err)
	}
	defer db.Close()

	// 2. Seed source table
	_, err = db.Exec(ctx, `
		CREATE TABLE source_users (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT UNIQUE NOT NULL,
			created_at TIMESTAMPTZ DEFAULT NOW()
		)`)
	if err != nil {
		t.Fatalf("failed to create source table: %v", err)
	}

	numRows := 1000
	for i := 1; i <= numRows; i++ {
		_, err = db.Exec(ctx, "INSERT INTO source_users (name, email) VALUES ($1, $2)",
			fmt.Sprintf("User %d", i), fmt.Sprintf("user%d@example.com", i))
		if err != nil {
			t.Fatalf("failed to seed data: %v", err)
		}
	}

	// 3. Run migration
	store, err := checkpoint.NewStore("test_checkpoint.bolt")
	if err != nil {
		t.Fatalf("failed to create checkpoint store: %v", err)
	}
	defer store.Close()

	cfg := config.Config{
		Source: config.DBConfig{
			Type:     "postgres",
			Database: "testdb",
			User:     "user",
			Password: "password",
			Tables:   []string{"source_users"},
		},
		Target: config.DBConfig{
			Type:     "postgres",
			Database: "testdb",
			User:     "user",
			Password: "password",
		},
		Concurrency: config.ConcurrencyConfig{
			NumReaders:      4,
			NumTransformers: 2,
			NumWriters:      4,
			BatchSize:       100,
			BatchTimeout:    1 * time.Second,
		},
	}

	// Update host/port for container
	host, _ := pgContainer.Host(ctx)
	port, _ := pgContainer.MappedPort(ctx, "5432")
	p, _ := strconv.Atoi(port.Port())
	cfg.Source.Host = host
	cfg.Source.Port = p
	cfg.Target.Host = host
	cfg.Target.Port = p

	src := postgres.NewPostgresAdapter()
	if err := src.Connect(ctx, cfg.Source); err != nil {
		t.Fatalf("failed to connect src: %v", err)
	}
	dst := postgres.NewPostgresAdapter()
	if err := dst.Connect(ctx, cfg.Target); err != nil {
		t.Fatalf("failed to connect dst: %v", err)
	}

	// Apply schema manually to target table 'target_users'
	s, _ := src.Schema(ctx, "source_users")
	s.Name = "target_users"
	if err := dst.ApplySchema(ctx, s); err != nil {
		t.Fatalf("failed to apply schema: %v", err)
	}

	orch := pipeline.NewOrchestrator(cfg.Concurrency, store)
	if err := orch.Migrate(ctx, "test-op", src, dst, "source_users"); err != nil {
		t.Fatalf("migration failed: %v", err)
	}

	// 4. Verify
	var count int
	err = db.QueryRow(ctx, "SELECT COUNT(*) FROM target_users").Scan(&count)
	if err != nil {
		t.Fatalf("failed to count target rows: %v", err)
	}

	if count != numRows {
		t.Errorf("expected %d rows, got %d", numRows, count)
	}
}
