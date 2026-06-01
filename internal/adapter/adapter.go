package adapter

import (
	"context"

	"github.com/dinocodesx/migration_and_backup_tool/internal/config"
	"github.com/dinocodesx/migration_and_backup_tool/internal/record"
	"github.com/dinocodesx/migration_and_backup_tool/internal/schema"
)

// Partition represents a slice of a table or collection that can be read in parallel.
type Partition struct {
	ID    string
	Table string
	Start any
	End   any
}

// SourceAdapter defines the interface for databases we read from.
type SourceAdapter interface {
	// Connect validates credentials and opens a connection pool.
	Connect(ctx context.Context, cfg config.DBConfig) error

	// Partitions splits the source table/collection into N roughly equal
	// partitions for parallel reading. Returns partition descriptors.
	Partitions(ctx context.Context, table string, n int) ([]Partition, error)

	// ReadPartition streams records from a single partition into ch.
	// It must respect ctx cancellation and send on errCh on fatal errors.
	ReadPartition(ctx context.Context, p Partition, ch chan<- *record.Record, errCh chan<- error)

	// Schema returns the canonical schema for a table/collection.
	Schema(ctx context.Context, table string) (*schema.Schema, error)

	// Close releases all connections.
	Close() error
}

// TargetAdapter defines the interface for databases we write to.
type TargetAdapter interface {
	// Connect validates credentials and opens a connection pool.
	Connect(ctx context.Context, cfg config.DBConfig) error

	// WriteBatch atomically (best-effort) writes a batch of records.
	// Returns the count of successfully written records.
	WriteBatch(ctx context.Context, batch []*record.Record) (int, error)

	// ApplySchema creates or alters the target table to match s.
	ApplySchema(ctx context.Context, s *schema.Schema) error

	// Close releases all connections.
	Close() error
}
