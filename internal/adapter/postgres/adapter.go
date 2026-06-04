// Package postgres provides a PostgreSQL implementation of the gomigrate adapter interfaces.
// It supports both reading from (source) and writing to (target) PostgreSQL databases,
// utilizing the pgx driver for high-performance operations including the COPY protocol.
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
// It manages a connection pool and coordinates reading and writing operations
// through dedicated Reader and Writer components.
type PostgresAdapter struct {
	pool        *pgxpool.Pool
	reader      *Reader
	writer      *Writer
	tableSchema string // e.g. "public"
}

// NewPostgresAdapter creates an unconnected PostgresAdapter with default settings.
// The default table schema is set to "public".
func NewPostgresAdapter() *PostgresAdapter {
	return &PostgresAdapter{tableSchema: "public"}
}

// Type returns the adapter's database identifier "postgres".
func (a *PostgresAdapter) Type() string { return "postgres" }

// Connect opens a connection pool and validates connectivity to the PostgreSQL database.
// It initializes the underlying Reader. The Writer is lazily initialized during
// ApplySchema or WriteBatch.
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
	// Writer is created once per target table; it is initialised in ApplySchema or
	// on first WriteBatch if ApplySchema has not been called yet.
	return nil
}

// Partitions splits a table into multiple PK-range partitions for parallel reading.
// It delegates the partitioning logic to the internal Reader.
func (a *PostgresAdapter) Partitions(ctx context.Context, table string, n int) ([]adapter.Partition, error) {
	return a.reader.Partitions(ctx, table, n)
}

// ReadPartition streams records from a specific partition into the provided channel.
// It delegates the streaming logic to the internal Reader.
func (a *PostgresAdapter) ReadPartition(ctx context.Context, p adapter.Partition, ch chan<- *record.Record) error {
	return a.reader.ReadPartition(ctx, p, ch)
}

// Schema introspects the given table within the configured PostgreSQL schema
// to retrieve its canonical column definitions and primary key information.
func (a *PostgresAdapter) Schema(ctx context.Context, table string) (*schema.Schema, error) {
	return GetSchema(ctx, a.pool, table, a.tableSchema)
}

// WriteBatch writes a batch of records to the target table using the COPY protocol.
// If ApplySchema has not been called previously, it lazily initializes the Writer
// using the source table name from the first record in the batch.
func (a *PostgresAdapter) WriteBatch(ctx context.Context, batch []*record.Record) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}
	if a.writer == nil {
		// Lazily initialise writer from the source table name.
		a.writer = NewWriter(a.pool, batch[0].Metadata.SourceTable)
	}
	return a.writer.WriteBatch(ctx, batch)
}

// ApplySchema ensures the target table exists with the correct structure and
// initializes/caches the Writer for subsequent bulk write operations.
func (a *PostgresAdapter) ApplySchema(ctx context.Context, s *schema.Schema) error {
	a.writer = NewWriter(a.pool, s.Name)
	return a.writer.ApplySchema(ctx, s)
}

// Close gracefully releases all connections in the connection pool.
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
