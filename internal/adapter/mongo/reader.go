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

// Partitions splits the source collection into n partitions.
// It first attempts to use MongoDB's internal `splitVector` command, which is the
// most efficient way to get balanced chunks but requires admin privileges and
// direct access to shards (won't work on Atlas or non-privileged connections).
//
// If `splitVector` fails or is unavailable, it automatically falls back to
// fallbackPartitions(), which uses statistical sampling to find boundaries.
func (a *MongoAdapter) Partitions(ctx context.Context, table string, n int) ([]adapter.Partition, error) {
	if n <= 1 {
		return []adapter.Partition{{ID: "p0", Table: table}}, nil
	}

	db := a.client.Database(a.config.Database)
	namespace := fmt.Sprintf("%s.%s", a.config.Database, table)

	// Get collection stats to estimate per-chunk size.
	statsResult := db.RunCommand(ctx, bson.D{{Key: "collStats", Value: table}})
	var stats struct {
		Size  int64 `bson:"size"`
		Count int64 `bson:"count"`
	}
	if err := statsResult.Decode(&stats); err != nil || stats.Count == 0 {
		// Collection is empty or collStats unavailable — single partition.
		return []adapter.Partition{{ID: "p0", Table: table}}, nil
	}

	// Calculate maxChunkSize in MB for splitVector.
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
		// splitVector not available (Atlas, non-admin) — use min/max fallback.
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
	// Final open-ended partition.
	partitions = append(partitions, adapter.Partition{
		ID:    fmt.Sprintf("p%d", len(partitions)),
		Table: table,
		Start: lastKey,
		End:   nil,
	})

	return partitions, nil
}

// fallbackPartitions creates n partitions by sampling boundary ObjectIDs using
// the $sample aggregation stage. This works on MongoDB Atlas and any deployment
// where splitVector access is restricted.
//
// It samples n-1 documents, sorts them by _id, and uses their _id values as the
// range boundaries for the resulting partitions.
func (a *MongoAdapter) fallbackPartitions(ctx context.Context, table string, n int) ([]adapter.Partition, error) {
	coll := a.client.Database(a.config.Database).Collection(table)

	// Sample n-1 boundary documents to get n roughly equal ranges.
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
		// If aggregation fails, return a single partition as a safe fallback.
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

// ReadPartition streams all records from a single partition onto ch.
// It uses the Start and End keys of the partition to filter the range of _ids
// to read. It sorts by _id to ensure efficient scanning and consistent results.
//
// Records are fetched in batches (default 1000) to optimize throughput.
// The channel ch is closed when the reading is complete (either success or error).
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

		// Convert _id to string for Record.ID.
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
