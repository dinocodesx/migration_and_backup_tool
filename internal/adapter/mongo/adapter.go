// Package mongo provides a MongoDB implementation of the adapter interfaces.
// It supports both reading from (source) and writing to (target) MongoDB clusters,
// including support for replica sets and sharded clusters via URI configuration.
package mongo

import (
	"context"
	"fmt"
	"time"

	"github.com/dinocodesx/gomigrate/internal/adapter"
	"github.com/dinocodesx/gomigrate/internal/config"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// MongoAdapter implements both adapter.SourceAdapter and adapter.TargetAdapter
// for MongoDB. It manages the underlying mongo.Client and tracks connection
// configuration.
type MongoAdapter struct {
	client *mongo.Client
	config config.DBConfig
}

// NewMongoAdapter creates a new, unconnected MongoAdapter.
// Use Connect() to initialize the connection before use.
func NewMongoAdapter() *MongoAdapter {
	return &MongoAdapter{}
}

// Type returns the adapter's database identifier, which is "mongo".
func (a *MongoAdapter) Type() string { return "mongo" }

// Connect builds a MongoDB connection URI from the provided configuration,
// creates a new client, and verifies connectivity by pinging the primary.
// It supports both single-host and multi-host (comma-separated) configurations.
func (a *MongoAdapter) Connect(ctx context.Context, cfg config.DBConfig) error {
	a.config = cfg

	var uri string
	if cfg.User != "" && cfg.Password != "" {
		uri = fmt.Sprintf("mongodb://%s:%s@%s:%d/%s",
			cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
	} else {
		uri = fmt.Sprintf("mongodb://%s:%d/%s", cfg.Host, cfg.Port, cfg.Database)
	}

	// When multiple hosts are provided (replica set / sharded), join them.
	if len(cfg.Hosts) > 0 {
		// The mongo driver accepts a comma-separated host list in the URI.
		hostStr := cfg.Hosts[0]
		for _, h := range cfg.Hosts[1:] {
			hostStr += "," + h
		}
		if cfg.User != "" && cfg.Password != "" {
			uri = fmt.Sprintf("mongodb://%s:%s@%s/%s",
				cfg.User, cfg.Password, hostStr, cfg.Database)
		} else {
			uri = fmt.Sprintf("mongodb://%s/%s", hostStr, cfg.Database)
		}
	}

	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return fmt.Errorf("failed to connect to mongodb: %w", err)
	}

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		_ = client.Disconnect(ctx)
		return fmt.Errorf("failed to ping mongodb primary: %w", err)
	}

	a.client = client
	return nil
}

// Close disconnects the MongoDB client with a graceful timeout (5 seconds).
// If the client is nil, it returns immediately with no error.
func (a *MongoAdapter) Close() error {
	if a.client == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return a.client.Disconnect(ctx)
}

// Compile-time interface compliance checks.
var _ adapter.SourceAdapter = (*MongoAdapter)(nil)
var _ adapter.TargetAdapter = (*MongoAdapter)(nil)
