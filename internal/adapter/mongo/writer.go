package mongo

import (
	"context"
	"fmt"

	"github.com/dinocodesx/gomigrate/internal/record"
	"github.com/dinocodesx/gomigrate/internal/schema"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// WriteBatch writes a batch of records to MongoDB using an unordered BulkWrite
// with upsert semantics, making each write idempotent.
func (a *MongoAdapter) WriteBatch(ctx context.Context, batch []*record.Record) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}

	// All records in a batch are expected to belong to the same collection.
	table := batch[0].Metadata.SourceTable
	coll := a.client.Database(a.config.Database).Collection(table)

	models := make([]mongo.WriteModel, 0, len(batch))
	for _, rec := range batch {
		// Use rec.ID as the _id. Remove _id from Data to prevent duplicate key
		// errors if the source document already carried _id in its fields.
		docData := make(bson.M, len(rec.Data))
		for k, v := range rec.Data {
			if k != "_id" {
				docData[k] = v
			}
		}

		model := mongo.NewReplaceOneModel().
			SetFilter(bson.M{"_id": rec.ID}).
			SetReplacement(docData).
			SetUpsert(true)

		models = append(models, model)
	}

	opts := options.BulkWrite().SetOrdered(false)
	result, err := coll.BulkWrite(ctx, models, opts)
	if err != nil {
		// BulkWriteException can carry partial results — report them.
		written := 0
		if result != nil {
			// UpsertedCount: new docs; ModifiedCount: existing docs changed.
			// Do NOT include MatchedCount (matched ≠ written).
			written = int(result.UpsertedCount + result.ModifiedCount + result.InsertedCount)
		}
		return written, fmt.Errorf("bulk write failed: %w", err)
	}

	// MatchedCount is deliberately excluded — it counts documents that matched
	// the filter but may not have been modified (identical content).
	return int(result.UpsertedCount + result.ModifiedCount + result.InsertedCount), nil
}

// ApplySchema ensures the target collection exists and creates any required
// indexes. MongoDB creates collections lazily, but pre-creating guarantees the
// collection is ready before any writes arrive.
func (a *MongoAdapter) ApplySchema(ctx context.Context, s *schema.Schema) error {
	db := a.client.Database(a.config.Database)

	collections, err := db.ListCollectionNames(ctx, bson.M{"name": s.Name})
	if err != nil {
		return fmt.Errorf("failed to list collections: %w", err)
	}

	if len(collections) == 0 {
		if err := db.CreateCollection(ctx, s.Name); err != nil {
			return fmt.Errorf("failed to create collection %q: %w", s.Name, err)
		}
	}

	// Create unique indexes for non-_id primary key columns.
	for _, col := range s.Columns {
		if col.PrimaryKey && col.Name != "_id" {
			indexModel := mongo.IndexModel{
				Keys:    bson.D{{Key: col.Name, Value: 1}},
				Options: options.Index().SetUnique(true),
			}
			if _, err := db.Collection(s.Name).Indexes().CreateOne(ctx, indexModel); err != nil {
				return fmt.Errorf("failed to create unique index for %q: %w", col.Name, err)
			}
		}
	}

	return nil
}
