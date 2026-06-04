// Package factory provides a centralized factory for creating database-specific adapters.
// It abstracts the instantiation logic for both source and target adapters, allowing
// the rest of the system to remain agnostic of the underlying database implementations.
package factory

import (
	"fmt"

	"github.com/dinocodesx/gomigrate/internal/adapter"
	"github.com/dinocodesx/gomigrate/internal/adapter/cassandra"
	"github.com/dinocodesx/gomigrate/internal/adapter/iceberg"
	"github.com/dinocodesx/gomigrate/internal/adapter/mongo"
	"github.com/dinocodesx/gomigrate/internal/adapter/postgres"
)

// NewSourceAdapter returns a concrete implementation of SourceAdapter based on
// the provided database type string. It serves as a dispatcher to instantiate
// the correct adapter for extraction operations.
//
// Supported types: "postgres", "mongo" (or "mongodb"), "cassandra", and "iceberg".
func NewSourceAdapter(dbType string) (adapter.SourceAdapter, error) {
	switch dbType {
	case "postgres":
		return postgres.NewPostgresAdapter(), nil
	case "mongo", "mongodb":
		return mongo.NewMongoAdapter(), nil
	case "cassandra":
		return cassandra.NewCassandraAdapter(), nil
	case "iceberg":
		return iceberg.NewIcebergAdapter(), nil
	default:
		return nil, fmt.Errorf("unsupported source database type: %s", dbType)
	}
}

// NewTargetAdapter returns a concrete implementation of TargetAdapter based on
// the provided database type string. It serves as a dispatcher to instantiate
// the correct adapter for loading operations.
//
// Supported types: "postgres", "mongo" (or "mongodb"), "cassandra", and "iceberg".
func NewTargetAdapter(dbType string) (adapter.TargetAdapter, error) {
	switch dbType {
	case "postgres":
		return postgres.NewPostgresAdapter(), nil
	case "mongo", "mongodb":
		return mongo.NewMongoAdapter(), nil
	case "cassandra":
		return cassandra.NewCassandraAdapter(), nil
	case "iceberg":
		return iceberg.NewIcebergAdapter(), nil
	default:
		return nil, fmt.Errorf("unsupported target database type: %s", dbType)
	}
}
