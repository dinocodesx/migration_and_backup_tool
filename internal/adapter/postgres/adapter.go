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
type PostgresAdapter struct {
	pool        *pgxpool.Pool
	reader      *Reader
	writer      *Writer
	tableSchema string // e.g. "public"
}

// NewPostgresAdapter creates an unconnected PostgresAdapter.
func NewPostgresAdapter() *PostgresAdapter {
	return &PostgresAdapter{tableSchema: "public"}
}

// Type returns the adapter's database identifier.
func (a *PostgresAdapter) Type() string { return "postgres" }

// Connect opens a connection pool and validates connectivity.
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

// Partitions delegates to the Reader.
func (a *PostgresAdapter) Partitions(ctx context.Context, table string, n int) ([]adapter.Partition, error) {
	return a.reader.Partitions(ctx, table, n)
}

// ReadPartition delegates to the Reader.
func (a *PostgresAdapter) ReadPartition(ctx context.Context, p adapter.Partition, ch chan<- *record.Record) error {
	return a.reader.ReadPartition(ctx, p, ch)
}

// Schema introspects the given table using the configured tableSchema.
func (a *PostgresAdapter) Schema(ctx context.Context, table string) (*schema.Schema, error) {
	return GetSchema(ctx, a.pool, table, a.tableSchema)
}

// WriteBatch writes a batch of records to the target table.
// If ApplySchema has not been called, the target table is inferred from the
// first record's SourceTable metadata.
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

// ApplySchema creates (or ensures) the target table and caches the writer.
func (a *PostgresAdapter) ApplySchema(ctx context.Context, s *schema.Schema) error {
	a.writer = NewWriter(a.pool, s.Name)
	return a.writer.ApplySchema(ctx, s)
}

// Close releases all connections.
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
