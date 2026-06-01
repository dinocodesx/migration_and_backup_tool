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

// WriteBatch atomically (best-effort) writes a batch of records to MongoDB.
func (a *MongoAdapter) WriteBatch(ctx context.Context, batch []*record.Record) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}

	// Assuming all records in the batch belong to the same collection (standard for gomigrate)
	table := batch[0].Metadata.SourceTable
	coll := a.client.Database(a.config.Database).Collection(table)

	models := make([]mongo.WriteModel, len(batch))
	for i, rec := range batch {
		// Use Record.ID as the _id for the document
		// Note: we might need to preserve the original _id if it's already in rec.Data
		// but typically we map the source ID to the target ID.

		filter := bson.M{"_id": rec.ID}

		// If rec.Data already contains _id, we should ensure it matches rec.ID or remove it
		// to avoid duplicate key errors if the driver tries to insert it as well.
		delete(rec.Data, "_id")

		model := mongo.NewReplaceOneModel().
			SetFilter(filter).
			SetReplacement(rec.Data).
			SetUpsert(true)

		models[i] = model
	}

	opts := options.BulkWrite().SetOrdered(false)
	result, err := coll.BulkWrite(ctx, models, opts)
	if err != nil {
		count := 0
		if result != nil {
			count = int(result.UpsertedCount + result.ModifiedCount + result.InsertedCount)
		}
		return count, fmt.Errorf("bulk write failed: %w", err)
	}

	return int(result.UpsertedCount + result.ModifiedCount + result.InsertedCount + result.MatchedCount), nil
}

// ApplySchema ensures the target collection exists and optionally sets up indexes.
func (a *MongoAdapter) ApplySchema(ctx context.Context, s *schema.Schema) error {
	// MongoDB creates collections on the fly, but we can pre-create it and set indexes.
	db := a.client.Database(a.config.Database)

	// Check if collection exists (optional, but good for reporting)
	collections, err := db.ListCollectionNames(ctx, bson.M{"name": s.Name})
	if err != nil {
		return fmt.Errorf("failed to list collection names: %w", err)
	}

	if len(collections) == 0 {
		if err := db.CreateCollection(ctx, s.Name); err != nil {
			return fmt.Errorf("failed to create collection %s: %w", s.Name, err)
		}
	}

	// Setup primary key index if not _id (though in gomigrate we usually map PK to _id)
	for _, col := range s.Columns {
		if col.PrimaryKey && col.Name != "_id" {
			indexModel := mongo.IndexModel{
				Keys:    bson.D{{Key: col.Name, Value: 1}},
				Options: options.Index().SetUnique(true),
			}
			_, err := db.Collection(s.Name).Indexes().CreateOne(ctx, indexModel)
			if err != nil {
				return fmt.Errorf("failed to create index for primary key %s: %w", col.Name, err)
			}
		}
	}

	return nil
}
