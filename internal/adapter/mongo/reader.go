package mongo

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/dinocodesx/gomigrate/internal/adapter"
	"github.com/dinocodesx/gomigrate/internal/record"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Partitions analyzes a MongoDB collection and returns a slice of Partition
// boundaries for parallel processing. It attempts to use the high-performance
// 'splitVector' command for balanced chunks.
//
// If 'splitVector' is restricted (common in Atlas or non-admin roles), it
// falls back to statistical sampling of ObjectIDs to determine ranges.
func (a *MongoAdapter) Partitions(ctx context.Context, table string, n int) ([]adapter.Partition, error) {
	if n <= 1 {
		return []adapter.Partition{{ID: "p0", Table: table}}, nil
	}

	db := a.client.Database(a.config.Database)
	namespace := fmt.Sprintf("%s.%s", a.config.Database, table)

	statsResult := db.RunCommand(ctx, bson.D{{Key: "collStats", Value: table}})
	var stats struct {
		Size  int64 `bson:"size"`
		Count int64 `bson:"count"`
	}
	if err := statsResult.Decode(&stats); err != nil || stats.Count == 0 {
		return []adapter.Partition{{ID: "p0", Table: table}}, nil
	}

	avgDocSize := float64(stats.Size) / float64(stats.Count)
	docsPerPartition := float64(stats.Count) / float64(n)
	maxChunkSizeMB := math.Max(1, (docsPerPartition*avgDocSize)/(1024*1024))

	cmd := bson.D{
		{Key: "splitVector", Value: namespace},
		{Key: "keyPattern", Value: bson.D{{Key: "_id", Value: 1}}},
		{Key: "maxChunkSize", Value: int64(math.Ceil(maxChunkSizeMB))},
	}

	var splitResult struct {
		SplitKeys []bson.M `bson:"splitKeys"`
		OK        int      `bson:"ok"`
	}

	adminDB := a.client.Database("admin")
	err := adminDB.RunCommand(ctx, cmd).Decode(&splitResult)
	if err != nil || splitResult.OK != 1 {
		return a.fallbackPartitions(ctx, table, n)
	}

	if len(splitResult.SplitKeys) == 0 {
		return []adapter.Partition{{ID: "p0", Table: table}}, nil
	}

	partitions := make([]adapter.Partition, 0, len(splitResult.SplitKeys)+1)
	var lastKey any

	for i, sk := range splitResult.SplitKeys {
		key := sk["_id"]
		partitions = append(partitions, adapter.Partition{
			ID:    fmt.Sprintf("p%d", i),
			Table: table,
			Start: lastKey,
			End:   key,
		})
		lastKey = key
	}
	partitions = append(partitions, adapter.Partition{
		ID:    fmt.Sprintf("p%d", len(partitions)),
		Table: table,
		Start: lastKey,
		End:   nil,
	})

	return partitions, nil
}

// fallbackPartitions calculates partition boundaries by sampling random
// documents from the collection. The sampled _id values are used as splitting points.
func (a *MongoAdapter) fallbackPartitions(ctx context.Context, table string, n int) ([]adapter.Partition, error) {
	coll := a.client.Database(a.config.Database).Collection(table)

	sampleSize := n - 1
	if sampleSize <= 0 {
		return []adapter.Partition{{ID: "p0", Table: table}}, nil
	}

	pipeline := bson.A{
		bson.D{{Key: "$sample", Value: bson.D{{Key: "size", Value: sampleSize}}}},
		bson.D{{Key: "$project", Value: bson.D{{Key: "_id", Value: 1}}}},
		bson.D{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
	}

	cursor, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		return []adapter.Partition{{ID: "p0", Table: table}}, nil
	}
	defer cursor.Close(ctx)

	var boundaries []any
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		if id, ok := doc["_id"]; ok {
			boundaries = append(boundaries, id)
		}
	}

	if len(boundaries) == 0 {
		return []adapter.Partition{{ID: "p0", Table: table}}, nil
	}

	partitions := make([]adapter.Partition, 0, len(boundaries)+1)
	var lastKey any
	for i, key := range boundaries {
		partitions = append(partitions, adapter.Partition{
			ID:    fmt.Sprintf("p%d", i),
			Table: table,
			Start: lastKey,
			End:   key,
		})
		lastKey = key
	}
	partitions = append(partitions, adapter.Partition{
		ID:    fmt.Sprintf("p%d", len(partitions)),
		Table: table,
		Start: lastKey,
		End:   nil,
	})

	return partitions, nil
}

// ReadPartition streams documents from a specific _id range into a channel.
// It uses range-based filters ($gte, $lt) and sorts by _id to ensure efficient
// scanning and avoidance of memory-intensive sorting.
func (a *MongoAdapter) ReadPartition(ctx context.Context, p adapter.Partition, ch chan<- *record.Record) error {
	defer close(ch)

	coll := a.client.Database(a.config.Database).Collection(p.Table)

	filter := bson.M{}
	if p.Start != nil && p.End != nil {
		filter["_id"] = bson.M{"$gte": p.Start, "$lt": p.End}
	} else if p.Start != nil {
		filter["_id"] = bson.M{"$gte": p.Start}
	} else if p.End != nil {
		filter["_id"] = bson.M{"$lt": p.End}
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "_id", Value: 1}}).
		SetBatchSize(1000)

	cursor, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return fmt.Errorf("failed to execute find in partition %s: %w", p.ID, err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return fmt.Errorf("failed to decode document in partition %s: %w", p.ID, err)
		}

		var id string
		rawID := doc["_id"]
		switch v := rawID.(type) {
		case primitive.ObjectID:
			id = v.Hex()
		default:
			id = fmt.Sprintf("%v", v)
		}

		rec := &record.Record{
			ID:   id,
			Data: doc,
			Metadata: record.RecordMetadata{
				SourceTable:   p.Table,
				SourceDB:      a.config.Database,
				PartitionID:   p.ID,
				Offset:        rawID,
				IngestionTime: time.Now(),
			},
		}

		select {
		case ch <- rec:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor error in partition %s: %w", p.ID, err)
	}

	return nil
}
