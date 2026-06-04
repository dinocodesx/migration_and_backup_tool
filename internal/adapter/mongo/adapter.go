// Package mongo provides a production-grade MongoDB implementation of the
// gomigrate adapter interfaces. It supports single-node, replica sets, and
// sharded clusters using the official MongoDB Go driver.
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

// MongoAdapter implements both adapter.SourceAdapter and adapter.TargetAdapter.
// It manages a mongo.Client and provides a unified interface for data
// extraction and ingestion.
type MongoAdapter struct {
	// client is the connected MongoDB client.
	client *mongo.Client
	// config stores the database configuration used for connections.
	config config.DBConfig
}

// NewMongoAdapter returns an uninitialized MongoAdapter.
func NewMongoAdapter() *MongoAdapter {
	return &MongoAdapter{}
}

// Type returns the adapter identifier "mongo".
func (a *MongoAdapter) Type() string { return "mongo" }

// Connect establishes a connection to the MongoDB cluster. It automatically
// constructs a connection URI from the config (supporting multiple hosts for
// high availability) and verifies the connection with a ping.
func (a *MongoAdapter) Connect(ctx context.Context, cfg config.DBConfig) error {
	a.config = cfg

	var uri string
	if cfg.User != "" && cfg.Password != "" {
		uri = fmt.Sprintf("mongodb://%s:%s@%s:%d/%s",
			cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
	} else {
		uri = fmt.Sprintf("mongodb://%s:%d/%s", cfg.Host, cfg.Port, cfg.Database)
	}

	if len(cfg.Hosts) > 0 {
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

	// Append extra parameters
	if len(cfg.Params) > 0 {
		uri += "?"
		for k, v := range cfg.Params {
			uri += fmt.Sprintf("%s=%s&", k, v)
		}
		uri = uri[:len(uri)-1]
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

// Close gracefully disconnects from the MongoDB cluster with a 5-second timeout.
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
