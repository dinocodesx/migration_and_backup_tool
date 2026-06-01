package mongo

import (
	"context"
	"fmt"

	"github.com/dinocodesx/gomigrate/internal/adapter"
	"github.com/dinocodesx/gomigrate/internal/config"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// MongoAdapter implements both adapter.SourceAdapter and adapter.TargetAdapter for MongoDB.
type MongoAdapter struct {
	client *mongo.Client
	config config.DBConfig
}

// NewMongoAdapter creates a new MongoDB adapter instance.
func NewMongoAdapter() *MongoAdapter {
	return &MongoAdapter{}
}

// Type returns the database type.
func (a *MongoAdapter) Type() string {
	return "mongo"
}

// Connect validates credentials and opens a connection pool.
func (a *MongoAdapter) Connect(ctx context.Context, cfg config.DBConfig) error {
	a.config = cfg

	// Build connection string
	// Format: mongodb://[user:password@]host[:port]/[database][?options]
	var uri string
	if cfg.User != "" && cfg.Password != "" {
		uri = fmt.Sprintf("mongodb://%s:%s@%s:%d/%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
	} else {
		uri = fmt.Sprintf("mongodb://%s:%d/%s", cfg.Host, cfg.Port, cfg.Database)
	}

	// For Cassandra/distributed Mongo, cfg.Hosts might be used.
	// If Hosts is provided, we use those instead of Host.
	if len(cfg.Hosts) > 0 {
		// URI construction for multiple hosts would go here if needed.
		// For now, we follow the simple case.
	}

	clientOptions := options.Client().ApplyURI(uri)

	// Set connection pool limits based on concurrency if applicable
	// (Optional: tune based on cfg and orchestrator needs)

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return fmt.Errorf("failed to connect to mongodb: %w", err)
	}

	// Ping the primary to verify connectivity
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return fmt.Errorf("failed to ping mongodb: %w", err)
	}

	a.client = client
	return nil
}

// Close releases all connections.
func (a *MongoAdapter) Close() error {
	if a.client != nil {
		return a.client.Disconnect(context.Background())
	}
	return nil
}

// Ensure interface compliance
var _ adapter.SourceAdapter = (*MongoAdapter)(nil)
var _ adapter.TargetAdapter = (*MongoAdapter)(nil)

// Partitions, ReadPartition, Schema (SourceAdapter)
// WriteBatch, ApplySchema (TargetAdapter)
// These will be implemented in subsequent files.
