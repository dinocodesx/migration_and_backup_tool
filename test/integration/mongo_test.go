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

	mongoAdapter := mongo.NewMongoAdapter()
	err = mongoAdapter.Connect(ctx, cfg)
	assert.NoError(t, err)
	defer mongoAdapter.Close()

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

	// 2. Write some data
	n, err := mongoAdapter.WriteBatch(ctx, records)
	assert.NoError(t, err)
	assert.Equal(t, 2, n)

	// 3. Read Schema
	s, err := mongoAdapter.Schema(ctx, table)
	assert.NoError(t, err)
	assert.Equal(t, table, s.Name)
	assert.GreaterOrEqual(t, len(s.Columns), 3) // _id, name, age

	// 4. Partitions
	partitions, err := mongoAdapter.Partitions(ctx, table, 2)
	assert.NoError(t, err)
	assert.NotEmpty(t, partitions)

	// 4. Read Partition using new synchronous interface
	recordCh := make(chan *record.Record, 10)

	readErrCh := make(chan error, 1)
	go func() {
		readErrCh <- mongoAdapter.ReadPartition(ctx, partitions[0], recordCh)
	}()

	var readCount int
	for {
		select {
		case rec, ok := <-recordCh:
			if !ok {
				goto done
			}
			assert.NotNil(t, rec)
			readCount++
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for records")
		}
	}
done:
	if err := <-readErrCh; err != nil {
		t.Fatalf("read partition failed: %s", err)
	}
	assert.Equal(t, 2, readCount)
}
