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

// WriteBatch ingests a slice of records into MongoDB using an unordered
// BulkWrite operation. It employs upsert semantics ('ReplaceOne' with
// 'Upsert: true') to ensure idempotency; records with existing _ids are
// overwritten, while new ones are created.
func (a *MongoAdapter) WriteBatch(ctx context.Context, batch []*record.Record) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}

	table := batch[0].Metadata.SourceTable
	coll := a.client.Database(a.config.Database).Collection(table)

	models := make([]mongo.WriteModel, 0, len(batch))
	for _, rec := range batch {
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
		written := 0
		if result != nil {
			written = int(result.UpsertedCount + result.ModifiedCount + result.InsertedCount)
		}
		return written, fmt.Errorf("bulk write failed: %w", err)
	}

	return int(result.UpsertedCount + result.ModifiedCount + result.InsertedCount), nil
}

// ApplySchema ensures the target collection exists and creates unique indexes
// for any fields marked as PrimaryKey that are not the standard '_id' field.
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
