package adapter

import (
	"context"

	"github.com/dinocodesx/gomigrate/internal/config"
	"github.com/dinocodesx/gomigrate/internal/record"
	"github.com/dinocodesx/gomigrate/internal/schema"
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
	// Type returns the database type (e.g., "postgres", "mongo").
	Type() string

	// Connect validates credentials and opens a connection pool.
	Connect(ctx context.Context, cfg config.DBConfig) error

	// Partitions splits the source table/collection into N roughly equal
	// partitions for parallel reading. Returns partition descriptors.
	Partitions(ctx context.Context, table string, n int) ([]Partition, error)

	// ReadPartition reads all records from a single partition, sending each
	// to ch. It MUST close ch when done (success or error). It returns an
	// error if the read fails fatally; non-fatal per-record errors should be
	// sent to the dead-letter channel by the caller.
	//
	// The implementation MUST respect ctx cancellation and return promptly
	// when ctx is Done (without closing ch if possible, though closing is
	// acceptable — the caller handles both cases).
	ReadPartition(ctx context.Context, p Partition, ch chan<- *record.Record) error

	// Schema returns the canonical schema for a table/collection.
	Schema(ctx context.Context, table string) (*schema.Schema, error)

	// Close releases all connections.
	Close() error
}

// TargetAdapter defines the interface for databases we write to.
type TargetAdapter interface {
	// Type returns the database type.
	Type() string

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
