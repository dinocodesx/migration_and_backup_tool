// Package factory provides a centralized factory for creating database-specific adapters.
// It abstracts the instantiation logic for both source and target adapters, allowing
// the rest of the system to remain agnostic of the underlying database implementations.
package factory

import (
	"fmt"

	"github.com/dinocodesx/gomigrate/internal/adapter"
	"github.com/dinocodesx/gomigrate/internal/adapter/cassandra"
	"github.com/dinocodesx/gomigrate/internal/adapter/mongo"
	"github.com/dinocodesx/gomigrate/internal/adapter/postgres"
)

// NewSourceAdapter returns a SourceAdapter based on the provided database type.
// It supports "postgres", "mongo" (or "mongodb"), and "cassandra".
// If the dbType is unsupported, it returns an error.
func NewSourceAdapter(dbType string) (adapter.SourceAdapter, error) {
	switch dbType {
	case "postgres":
		return postgres.NewPostgresAdapter(), nil
	case "mongo", "mongodb":
		return mongo.NewMongoAdapter(), nil
	case "cassandra":
		return cassandra.NewCassandraAdapter(), nil
	default:
		return nil, fmt.Errorf("unsupported source database type: %s", dbType)
	}
}

// NewTargetAdapter returns a TargetAdapter based on the provided database type.
// It supports "postgres", "mongo" (or "mongodb"), and "cassandra".
// If the dbType is unsupported, it returns an error.
func NewTargetAdapter(dbType string) (adapter.TargetAdapter, error) {
	switch dbType {
	case "postgres":
		return postgres.NewPostgresAdapter(), nil
	case "mongo", "mongodb":
		return mongo.NewMongoAdapter(), nil
	case "cassandra":
		return cassandra.NewCassandraAdapter(), nil
	default:
		return nil, fmt.Errorf("unsupported target database type: %s", dbType)
	}
}
