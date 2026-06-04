// Package postgres provides a production-grade PostgreSQL implementation of
// the gomigrate adapter interfaces. It utilizes pgx/v5 for high-performance
// connection pooling and bulk data operations.
package postgres

import (
	"context"
	"fmt"

	"github.com/dinocodesx/gomigrate/internal/adapter"
	"github.com/dinocodesx/gomigrate/internal/config"
	"github.com/dinocodesx/gomigrate/internal/record"
	"github.com/dinocodesx/gomigrate/internal/schema"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresAdapter implements both adapter.SourceAdapter and adapter.TargetAdapter.
// it acts as a coordinator, delegating specific reading and writing tasks to
// specialized Reader and Writer components while managing the connection pool.
type PostgresAdapter struct {
	// pool is the pgx connection pool shared by readers and writers.
	pool *pgxpool.Pool
	// reader handles partition discovery and data extraction logic.
	reader *Reader
	// writer handles schema application and bulk data insertion.
	writer *Writer
	// tableSchema is the PostgreSQL schema name (e.g., "public").
	tableSchema string
}

// NewPostgresAdapter returns an uninitialized PostgresAdapter instance.
// Default configuration sets the table schema to "public".
func NewPostgresAdapter() *PostgresAdapter {
	return &PostgresAdapter{tableSchema: "public"}
}

// Type returns the adapter identifier "postgres".
func (a *PostgresAdapter) Type() string { return "postgres" }

// Connect establishes a connection pool to the PostgreSQL database using
// the provided configuration. It verifies connectivity with a ping.
func (a *PostgresAdapter) Connect(ctx context.Context, cfg config.DBConfig) error {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("failed to create postgres pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("failed to ping postgres at %s:%d: %w", cfg.Host, cfg.Port, err)
	}

	a.pool = pool
	a.reader = NewReader(pool)
	return nil
}

// Partitions calculates logical primary key ranges for parallel reading.
func (a *PostgresAdapter) Partitions(ctx context.Context, table string, n int) ([]adapter.Partition, error) {
	return a.reader.Partitions(ctx, table, n)
}

// ReadPartition extracts all records from a specific PK range and sends them to a channel.
func (a *PostgresAdapter) ReadPartition(ctx context.Context, p adapter.Partition, ch chan<- *record.Record) error {
	return a.reader.ReadPartition(ctx, p, ch)
}

// Schema introspects the PostgreSQL system catalogs to retrieve table metadata.
func (a *PostgresAdapter) Schema(ctx context.Context, table string) (*schema.Schema, error) {
	return GetSchema(ctx, a.pool, table, a.tableSchema)
}

// WriteBatch performs a bulk insert of records using the PostgreSQL COPY protocol.
// It lazily initializes the internal Writer if one does not exist.
func (a *PostgresAdapter) WriteBatch(ctx context.Context, batch []*record.Record) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}
	if a.writer == nil {
		a.writer = NewWriter(a.pool, batch[0].Metadata.SourceTable)
	}
	return a.writer.WriteBatch(ctx, batch)
}

// ApplySchema creates the target table based on the provided canonical schema.
func (a *PostgresAdapter) ApplySchema(ctx context.Context, s *schema.Schema) error {
	a.writer = NewWriter(a.pool, s.Name)
	return a.writer.ApplySchema(ctx, s)
}

// Close gracefully closes the connection pool.
func (a *PostgresAdapter) Close() error {
	if a.pool != nil {
		a.pool.Close()
		a.pool = nil
	}
	return nil
}

// Ensure interface compliance at compile time.
var _ adapter.SourceAdapter = (*PostgresAdapter)(nil)
var _ adapter.TargetAdapter = (*PostgresAdapter)(nil)
