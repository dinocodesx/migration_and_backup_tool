package factory

import (
	"fmt"

	"github.com/dinocodesx/gomigrate/internal/adapter"
	"github.com/dinocodesx/gomigrate/internal/adapter/mongo"
	"github.com/dinocodesx/gomigrate/internal/adapter/postgres"
)

// NewSourceAdapter returns a SourceAdapter based on the database type.
func NewSourceAdapter(dbType string) (adapter.SourceAdapter, error) {
	switch dbType {
	case "postgres":
		return postgres.NewPostgresAdapter(), nil
	case "mongo", "mongodb":
		return mongo.NewMongoAdapter(), nil
	default:
		return nil, fmt.Errorf("unsupported source database type: %s", dbType)
	}
}

// NewTargetAdapter returns a TargetAdapter based on the database type.
func NewTargetAdapter(dbType string) (adapter.TargetAdapter, error) {
	switch dbType {
	case "postgres":
		return postgres.NewPostgresAdapter(), nil
	case "mongo", "mongodb":
		return mongo.NewMongoAdapter(), nil
	default:
		return nil, fmt.Errorf("unsupported target database type: %s", dbType)
	}
}
