//go:build e2e

// Package e2e implements end-to-end tests for the gomigrate pipeline.
// These tests require a running Docker daemon and spin up real containers
// via testcontainers-go.
package e2e

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	pgadapter "github.com/dinocodesx/gomigrate/internal/adapter/postgres"
	"github.com/dinocodesx/gomigrate/internal/checkpoint"
	"github.com/dinocodesx/gomigrate/internal/config"
	"github.com/dinocodesx/gomigrate/internal/errs"
	"github.com/dinocodesx/gomigrate/internal/migration"
	"github.com/dinocodesx/gomigrate/internal/pipeline"
	pgcontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/zap"
)

const (
	e2eTable  = "e2e_users"
	totalRows = 10_000
)

// TestCrashAndResume validates that a migration interrupted at ~40% completion
// correctly resumes from the checkpoint and produces the full expected row count
// without duplicates in the target.
func TestCrashAndResume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e crash-and-resume test in short mode")
	}

	ctx := context.Background()
	logger, _ := zap.NewDevelopment()

	// ── 1. Spin up two Postgres containers ───────────────────────────────────
	srcContainer, err := pgcontainer.Run(ctx,
		"postgres:16-alpine",
		pgcontainer.WithDatabase("src"),
		pgcontainer.WithUsername("gomigrate"),
		pgcontainer.WithPassword("secret"),
	)
	if err != nil {
		t.Fatalf("failed to start source container: %v", err)
	}
	t.Cleanup(func() { _ = srcContainer.Terminate(ctx) })

	dstContainer, err := pgcontainer.Run(ctx,
		"postgres:16-alpine",
		pgcontainer.WithDatabase("dst"),
		pgcontainer.WithUsername("gomigrate"),
		pgcontainer.WithPassword("secret"),
	)
	if err != nil {
		t.Fatalf("failed to start target container: %v", err)
	}
	t.Cleanup(func() { _ = dstContainer.Terminate(ctx) })

	srcDSN, err := srcContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("source DSN: %v", err)
	}
	dstDSN, err := dstContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("target DSN: %v", err)
	}

	// ── 2. Seed source table ─────────────────────────────────────────────────
	if err := seedTable(ctx, srcDSN, e2eTable, totalRows); err != nil {
		t.Fatalf("seed: %v", err)
	}
	t.Logf("seeded %d rows into source table %q", totalRows, e2eTable)

	// ── 3. Build source / target adapters ────────────────────────────────────
	// The postgres adapter accepts a DSN via the Params["url"] key.
	srcCfg := config.DBConfig{Type: "postgres", Params: map[string]string{"url": srcDSN}}
	dstCfg := config.DBConfig{Type: "postgres", Params: map[string]string{"url": dstDSN}}

	buildAdapters := func() (*pgadapter.PostgresAdapter, *pgadapter.PostgresAdapter, error) {
		src := pgadapter.NewPostgresAdapter()
		if err := src.Connect(ctx, srcCfg); err != nil {
			return nil, nil, fmt.Errorf("src connect: %w", err)
		}
		dst := pgadapter.NewPostgresAdapter()
		if err := dst.Connect(ctx, dstCfg); err != nil {
			_ = src.Close()
			return nil, nil, fmt.Errorf("dst connect: %w", err)
		}
		return src, dst, nil
	}

	concCfg := config.ConcurrencyConfig{
		NumReaders:         2,
		NumTransformers:    2,
		NumWriters:         2,
		BatchSize:          100,
		BatchTimeout:       500 * time.Millisecond,
		FlushEveryNBatches: 1,
	}
	opID := "e2e-cr-test"
	cpPath := t.TempDir() + "/e2e.bolt"
	dlqPath := cpPath + "_failed.ndjson"

	// ── 4. First run: simulate a crash ───────────────────────────────────────
	t.Log("first run: starting (expected to be cancelled early)")
	{
		store, _ := checkpoint.NewStore(cpPath)
		dlq, _ := errs.NewDLQ(dlqPath)
		src, dst, err := buildAdapters()
		if err != nil {
			t.Fatal(err)
		}

		// Apply schema before the first run.
		s, err := src.Schema(ctx, e2eTable)
		if err != nil {
			t.Fatalf("schema: %v", err)
		}
		if err := dst.ApplySchema(ctx, s); err != nil {
			t.Fatalf("apply schema: %v", err)
		}

		// Cancel the context after 3 seconds to simulate a mid-run crash.
		crashCtx, cancelCrash := context.WithTimeout(ctx, 3*time.Second)
		defer cancelCrash()

		mapper := migration.NewSchemaMapper("postgres", "postgres", nil)
		orch := pipeline.NewOrchestrator(concCfg, store, mapper, dlq, logger)

		// We expect an error (deadline exceeded / context cancelled).
		_ = orch.Migrate(crashCtx, opID, src, dst, e2eTable)

		_ = src.Close()
		_ = dst.Close()
		_ = dlq.Close()
		_ = store.Close()

		t.Log("first run: completed (crashed / cancelled)")
	}

	// ── 5. Resume run ────────────────────────────────────────────────────────
	t.Log("second run: resuming from checkpoint")
	{
		// Open the same checkpoint file — the orchestrator will skip Done partitions.
		store, _ := checkpoint.NewStore(cpPath)
		dlq, _ := errs.NewDLQ(dlqPath)
		src, dst, err := buildAdapters()
		if err != nil {
			t.Fatal(err)
		}

		resumeCfg := concCfg
		resumeCfg.FlushEveryNBatches = 5
		mapper := migration.NewSchemaMapper("postgres", "postgres", nil)
		orch := pipeline.NewOrchestrator(resumeCfg, store, mapper, dlq, logger)

		if resumeErr := orch.Migrate(ctx, opID, src, dst, e2eTable); resumeErr != nil {
			t.Fatalf("resume migration failed: %v", resumeErr)
		}

		// ── 6. Verify row count ────────────────────────────────────────────
		count, err := countRows(ctx, dstDSN, e2eTable)
		if err != nil {
			t.Fatalf("count rows: %v", err)
		}
		if count != totalRows {
			t.Errorf("expected %d rows in target, got %d", totalRows, count)
		} else {
			t.Logf("✓ row count matches: %d", count)
		}

		// ── 7. Check for duplicates (upsert idempotency) ──────────────────
		dupes, err := countDuplicates(ctx, dstDSN, e2eTable)
		if err != nil {
			t.Fatalf("count duplicates: %v", err)
		}
		if dupes > 0 {
			t.Errorf("found %d duplicate rows after crash-and-resume", dupes)
		} else {
			t.Log("✓ no duplicates found")
		}

		_ = src.Close()
		_ = dst.Close()
		_ = dlq.Close()
		_ = store.Close()
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// helpers
// ─────────────────────────────────────────────────────────────────────────────

// seedTable creates and seeds a simple integer-PK table in the given database.
func seedTable(ctx context.Context, dsn, table string, n int) error {
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			created_at TIMESTAMPTZ DEFAULT NOW()
		);`, table)
	if err := psql(ctx, dsn, createSQL); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("INSERT INTO %s (name) VALUES ", table))
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(fmt.Sprintf("('user-%d')", i))
	}
	sb.WriteString(";")
	return psql(ctx, dsn, sb.String())
}

// psql executes a SQL statement against a Postgres DSN via the psql CLI.
func psql(ctx context.Context, dsn, sql string) error {
	cmd := exec.CommandContext(ctx, "psql", dsn, "-c", sql)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%w: %s", err, out)
	}
	return nil
}

// countRows returns the number of rows in the given table.
func countRows(ctx context.Context, dsn, table string) (int, error) {
	out, err := exec.CommandContext(ctx, "psql", dsn, "-At", "-c",
		fmt.Sprintf("SELECT COUNT(*) FROM %s;", table)).Output()
	if err != nil {
		return 0, err
	}
	var n int
	_, err = fmt.Sscanf(strings.TrimSpace(string(out)), "%d", &n)
	return n, err
}

// countDuplicates returns the number of IDs that appear more than once.
func countDuplicates(ctx context.Context, dsn, table string) (int, error) {
	sql := fmt.Sprintf(
		`SELECT COUNT(*) FROM (SELECT id, COUNT(*) FROM %s GROUP BY id HAVING COUNT(*) > 1) sub;`,
		table,
	)
	out, err := exec.CommandContext(ctx, "psql", dsn, "-At", "-c", sql).Output()
	if err != nil {
		return 0, err
	}
	var n int
	_, err = fmt.Sscanf(strings.TrimSpace(string(out)), "%d", &n)
	return n, err
}
