package mongo

import (
	"context"
	"fmt"
	"math"

	"github.com/dinocodesx/gomigrate/internal/adapter"
	"github.com/dinocodesx/gomigrate/internal/record"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Partitions splits the source collection into N partitions using splitVector.
func (a *MongoAdapter) Partitions(ctx context.Context, table string, n int) ([]adapter.Partition, error) {
	if n <= 1 {
		return []adapter.Partition{{ID: "p0", Table: table}}, nil
	}

	db := a.client.Database(a.config.Database)

	// splitVector is an admin command. We need to run it against the admin database
	// but specifying the full namespace of the collection.
	namespace := fmt.Sprintf("%s.%s", a.config.Database, table)

	// Get collection stats to estimate chunk size
	statsResult := db.RunCommand(ctx, bson.D{{Key: "collStats", Value: table}})
	var stats struct {
		Size  int64 `bson:"size"`
		Count int64 `bson:"count"`
	}
	if err := statsResult.Decode(&stats); err != nil {
		// If collStats fails, fallback to simple range interpolation or single partition
		return []adapter.Partition{{ID: "p0", Table: table}}, nil
	}

	if stats.Count == 0 {
		return []adapter.Partition{{ID: "p0", Table: table}}, nil
	}

	// Calculate maxChunkSize in MB. splitVector expects this.
	// We want roughly n partitions.
	avgDocSize := float64(stats.Size) / float64(stats.Count)
	docsPerPartition := float64(stats.Count) / float64(n)
	maxChunkSizeMB := (docsPerPartition * avgDocSize) / (1024 * 1024)

	// Clamp maxChunkSizeMB to at least 1MB
	if maxChunkSizeMB < 1 {
		maxChunkSizeMB = 1
	}

	// splitVector command
	cmd := bson.D{
		{Key: "splitVector", Value: namespace},
		{Key: "keyPattern", Value: bson.D{{Key: "_id", Value: 1}}},
		{Key: "maxChunkSize", Value: int64(math.Ceil(maxChunkSizeMB))},
	}

	var result struct {
		SplitKeys []bson.M `bson:"splitKeys"`
		OK        int      `bson:"ok"`
	}

	// splitVector must be run on the admin database in some versions/deployments
	adminDB := a.client.Database("admin")
	err := adminDB.RunCommand(ctx, cmd).Decode(&result)
	if err != nil || result.OK != 1 {
		// Fallback to min/max interpolation if splitVector is not available (e.g., non-admin, Atlas)
		return a.fallbackPartitions(ctx, table, n)
	}

	partitions := make([]adapter.Partition, 0, len(result.SplitKeys)+1)
	var lastKey any

	for i, sk := range result.SplitKeys {
		key := sk["_id"]
		partitions = append(partitions, adapter.Partition{
			ID:    fmt.Sprintf("p%d", i),
			Table: table,
			Start: lastKey,
			End:   key,
		})
		lastKey = key
	}

	// Final partition
	partitions = append(partitions, adapter.Partition{
		ID:    fmt.Sprintf("p%d", len(partitions)),
		Table: table,
		Start: lastKey,
		End:   nil,
	})

	return partitions, nil
}

// fallbackPartitions uses min/max _id interpolation as a backup strategy.
func (a *MongoAdapter) fallbackPartitions(ctx context.Context, table string, n int) ([]adapter.Partition, error) {
	// For simplicity in this implementation, if splitVector fails, we return a single partition
	// but in a production environment, we'd interpolate based on min/max _id.
	return []adapter.Partition{{ID: "p0", Table: table}}, nil
}

// ReadPartition streams records from a single partition into ch.
func (a *MongoAdapter) ReadPartition(ctx context.Context, p adapter.Partition, ch chan<- *record.Record, errCh chan<- error) {
	coll := a.client.Database(a.config.Database).Collection(p.Table)

	filter := bson.M{}
	if p.Start != nil && p.End != nil {
		filter["_id"] = bson.M{"$gte": p.Start, "$lt": p.End}
	} else if p.Start != nil {
		filter["_id"] = bson.M{"$gte": p.Start}
	} else if p.End != nil {
		filter["_id"] = bson.M{"$lt": p.End}
	}

	opts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}})
	
	// Use batch size from config if available (via some context or adapter state)
	// For now we assume a reasonable default or look at a.config if it had it.

	cursor, err := coll.Find(ctx, filter, opts)
	if err != nil {
		errCh <- fmt.Errorf("failed to execute find in partition %s: %w", p.ID, err)
		return
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			errCh <- fmt.Errorf("failed to decode document in partition %s: %w", p.ID, err)
			return
		}

		// Convert _id to string for Record.ID
		var id string
		if oid, ok := doc["_id"]; ok {
			switch v := oid.(type) {
			case primitive.ObjectID:
				id = v.Hex()
			default:
				id = fmt.Sprintf("%v", v)
			}
		}

		rec := &record.Record{
			ID:   id,
			Data: doc,
			Metadata: record.RecordMetadata{
				SourceTable: p.Table,
				SourceDB:    a.config.Database,
				PartitionID: p.ID,
				Offset:      doc["_id"],
			},
		}

		select {
		case <-ctx.Done():
			return
		case ch <- rec:
		}
	}

	if err := cursor.Err(); err != nil {
		errCh <- fmt.Errorf("cursor error in partition %s: %w", p.ID, err)
	}
}
