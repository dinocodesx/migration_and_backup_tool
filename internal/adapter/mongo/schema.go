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

// Schema infers the structure of a MongoDB collection by sampling the first
// 1000 documents. Since MongoDB is schema-less, it performs a union of all
// observed fields and their types to create a representative canonical schema.
//
// Fields are marked as Nullable by default, and the '_id' field is always
// identified as the PrimaryKey.
func (a *MongoAdapter) Schema(ctx context.Context, table string) (*schema.Schema, error) {
	coll := a.client.Database(a.config.Database).Collection(table)

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
					Nullable:   true,
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

// inferType maps a BSON value to its canonical gomigrate type representation.
// It handles specialized BSON types like ObjectID and DateTime, normalizing
// them for cross-database compatibility.
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
		return "string"
	case primitive.Decimal128:
		return "float64"
	case bson.M, bson.D:
		return "map"
	case bson.A:
		return "array"
	case []byte:
		return "blob"
	default:
		t := reflect.TypeOf(v)
		if t != nil {
			return t.Name()
		}
		return "any"
	}
}
