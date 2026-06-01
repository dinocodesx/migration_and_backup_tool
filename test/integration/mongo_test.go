package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dinocodesx/gomigrate/internal/adapter/mongo"
	"github.com/dinocodesx/gomigrate/internal/config"
	"github.com/dinocodesx/gomigrate/internal/record"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
)

func TestMongoAdapter(t *testing.T) {
	ctx := context.Background()

	// Start MongoDB container
	mongoContainer, err := mongodb.Run(ctx, "mongo:6.0")
	if err != nil {
		t.Fatalf("failed to start mongodb container: %s", err)
	}
	defer mongoContainer.Terminate(ctx)

	host, err := mongoContainer.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get host: %s", err)
	}
	natPort, err := mongoContainer.MappedPort(ctx, "27017")
	if err != nil {
		t.Fatalf("failed to get mapped port: %s", err)
	}
	
	var port int
	fmt.Sscanf(natPort.Port(), "%d", &port)

	cfg := config.DBConfig{
		Type:     "mongo",
		Host:     host,
		Port:     port,
		Database: "testdb",
	}

	adapter := mongo.NewMongoAdapter()
	err = adapter.Connect(ctx, cfg)
	assert.NoError(t, err)
	defer adapter.Close()

	table := "users"

	// 1. Write some data
	records := []*record.Record{
		{
			ID:   "1",
			Data: map[string]any{"name": "Alice", "age": 30},
			Metadata: record.RecordMetadata{
				SourceTable: table,
			},
		},
		{
			ID:   "2",
			Data: map[string]any{"name": "Bob", "age": 25},
			Metadata: record.RecordMetadata{
				SourceTable: table,
			},
		},
	}

	n, err := adapter.WriteBatch(ctx, records)
	assert.NoError(t, err)
	assert.Equal(t, 2, n)

	// 2. Read Schema
	s, err := adapter.Schema(ctx, table)
	assert.NoError(t, err)
	assert.Equal(t, table, s.Name)
	assert.GreaterOrEqual(t, len(s.Columns), 3) // _id, name, age

	// 3. Partitions
	partitions, err := adapter.Partitions(ctx, table, 2)
	assert.NoError(t, err)
	assert.NotEmpty(t, partitions)

	// 4. Read Partitions
	recordCh := make(chan *record.Record, 10)
	errCh := make(chan error, 1)
	
	go adapter.ReadPartition(ctx, partitions[0], recordCh, errCh)

	var readCount int
	for i := 0; i < 2; i++ {
		select {
		case rec := <-recordCh:
			assert.NotNil(t, rec)
			readCount++
		case err := <-errCh:
			t.Fatalf("read partition failed: %s", err)
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for records")
		}
	}
	assert.Equal(t, 2, readCount)
}
