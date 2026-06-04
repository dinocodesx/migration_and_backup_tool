// Package adapter defines the core interfaces and types for database connectivity
// within the gomigrate system. It provides a common abstraction layer that allows
// the migration engine to interact with different database engines (e.g., PostgreSQL,
// MongoDB, Cassandra) in a uniform way.
package adapter

import (
	"context"

	"github.com/dinocodesx/gomigrate/internal/config"
	"github.com/dinocodesx/gomigrate/internal/record"
	"github.com/dinocodesx/gomigrate/internal/schema"
)

// Partition represents a logical slice of a database table or collection that
// can be processed independently and in parallel. This is the unit of work
// for parallel data extraction.
type Partition struct {
	// ID is a unique identifier for this partition within the scope of a table.
	ID string
	// Table is the name of the source table or collection this partition belongs to.
	Table string
	// Start is the starting boundary of the partition (e.g., a primary key value or token).
	Start any
	// End is the ending boundary (exclusive) of the partition.
	End any
}

// SourceAdapter defines the mandatory interface for database engines from which
// data is being extracted. Implementations handle connection management,
// schema introspection, and parallel data reading.
type SourceAdapter interface {
	// Type returns a string identifier for the database engine (e.g., "postgres", "mongo").
	Type() string

	// Connect initializes the database connection pool using the provided configuration.
	// It should validate connectivity before returning.
	Connect(ctx context.Context, cfg config.DBConfig) error

	// Partitions analyzes the source table and returns a slice of Partition objects.
	// The number of partitions 'n' is a hint for the desired level of parallelism.
	Partitions(ctx context.Context, table string, n int) ([]Partition, error)

	// ReadPartition streams all records belonging to the given partition into the
	// provided channel. It is responsible for closing the channel when extraction
	// is complete or a fatal error occurs. It must respect context cancellation.
	ReadPartition(ctx context.Context, p Partition, ch chan<- *record.Record) error

	// Schema returns the canonical schema definition for the specified table,
	// used for cross-database type mapping and target table creation.
	Schema(ctx context.Context, table string) (*schema.Schema, error)

	// Close gracefully terminates all database connections and releases resources.
	Close() error
}

// TargetAdapter defines the mandatory interface for database engines into which
// data is being loaded. Implementations handle schema application and efficient
// bulk data insertion.
type TargetAdapter interface {
	// Type returns a string identifier for the database engine.
	Type() string

	// Connect initializes the database connection pool using the provided configuration.
	Connect(ctx context.Context, cfg config.DBConfig) error

	// WriteBatch inserts a batch of records into the target database. It should
	// use the most efficient method available for the engine (e.g., COPY in Postgres).
	// Returns the number of successfully written records.
	WriteBatch(ctx context.Context, batch []*record.Record) (int, error)

	// ApplySchema ensures that the target table exists and matches the provided
	// schema definition. It should be idempotent.
	ApplySchema(ctx context.Context, s *schema.Schema) error

	// Close gracefully terminates all database connections and releases resources.
	Close() error
}
