package mongo

import (
	"context"
	"fmt"
	"reflect"

	"github.com/dinocodesx/gomigrate/internal/schema"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Schema infers the schema of a MongoDB collection by sampling a subset of documents.
// Since MongoDB is schema-less, it scans up to the first 1000 documents to
// identify the union of all fields and their types.
//
// All fields are marked as Nullable since MongoDB does not enforce field presence.
// The "_id" field is always identified as the PrimaryKey.
func (a *MongoAdapter) Schema(ctx context.Context, table string) (*schema.Schema, error) {
	coll := a.client.Database(a.config.Database).Collection(table)

	// Sample the first 1000 documents
	opts := options.Find().SetLimit(1000)
	cursor, err := coll.Find(ctx, bson.M{}, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to sample documents for schema inference: %w", err)
	}
	defer cursor.Close(ctx)

	columnsMap := make(map[string]schema.Column)

	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("failed to decode sample document: %w", err)
		}

		for k, v := range doc {
			if _, exists := columnsMap[k]; !exists {
				col := schema.Column{
					Name:       k,
					Type:       inferType(v),
					Nullable:   true, // In Mongo, any field can be missing/null
					PrimaryKey: k == "_id",
				}
				columnsMap[k] = col
			}
		}
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error during schema inference: %w", err)
	}

	columns := make([]schema.Column, 0, len(columnsMap))
	// Ensure _id is first if it exists
	if idCol, ok := columnsMap["_id"]; ok {
		columns = append(columns, idCol)
		delete(columnsMap, "_id")
	}

	for _, col := range columnsMap {
		columns = append(columns, col)
	}

	return &schema.Schema{
		Name:    table,
		Columns: columns,
	}, nil
}

// inferType maps a BSON value to a standard gomigrate type string.
// It handles common BSON types like ObjectID, DateTime, and Decimal128,
// mapping them to their closest counterparts in the gomigrate type system.
func inferType(v any) string {
	if v == nil {
		return "null"
	}

	switch v.(type) {
	case string:
		return "string"
	case int32, int64:
		return "int64"
	case float64:
		return "float64"
	case bool:
		return "bool"
	case primitive.DateTime:
		return "timestamp"
	case primitive.ObjectID:
		return "string" // Standardized to primitive
	case primitive.Decimal128:
		return "float64" // Standardized to primitive (potential precision loss, but keeps compatibility)
	case bson.M, bson.D:
		return "map"
	case bson.A:
		return "array"
	case []byte:
		return "blob"
	default:
		// Use reflection as a fallback
		t := reflect.TypeOf(v)
		if t != nil {
			return t.Name()
		}
		return "any"
	}
}
