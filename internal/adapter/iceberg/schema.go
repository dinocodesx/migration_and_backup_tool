package iceberg

import (
	"github.com/dinocodesx/gomigrate/internal/schema"
)

// mapIcebergType translates Iceberg types to canonical gomigrate types.
func mapIcebergType(icebergType any) string {
	switch t := icebergType.(type) {
	case string:
		switch t {
		case "boolean":
			return "bool"
		case "int", "long":
			return "int64"
		case "float", "double":
			return "float64"
		case "date", "time", "timestamp", "timestamptz":
			return "timestamp"
		case "string", "uuid":
			return "string"
		case "binary", "fixed":
			return "blob"
		default:
			return "string"
		}
	case map[string]any:
		if typeVal, ok := t["type"].(string); ok {
			switch typeVal {
			case "struct":
				return "map"
			case "list":
				return "array"
			case "map":
				return "map"
			case "decimal":
				return "float64"
			}
		}
	}
	return "string"
}

// ConvertToCanonicalSchema converts an Iceberg table schema to the gomigrate internal representation.
func ConvertToCanonicalSchema(is IcebergSchema, tableName string) *schema.Schema {
	s := &schema.Schema{Name: tableName}
	for _, field := range is.Fields {
		s.Columns = append(s.Columns, schema.Column{
			Name:     field.Name,
			Type:     mapIcebergType(field.Type),
			Nullable: !field.Required,
		})
	}
	return s
}

// mapToIcebergType translates canonical gomigrate types to Iceberg-compatible type definitions.
func mapToIcebergType(canonicalType string) any {
	switch canonicalType {
	case "int64":
		return "long"
	case "float64":
		return "double"
	case "bool":
		return "boolean"
	case "timestamp":
		return "timestamptz"
	case "blob":
		return "binary"
	case "map":
		// Simplified struct representation for map types
		return "string" // Fallback for simplicity in v1
	case "array":
		// Simplified list representation for array types
		return "string" // Fallback for simplicity in v1
	default:
		return "string"
	}
}

// CreateIcebergSchema creates an Iceberg-compatible schema definition from a canonical schema.
func CreateIcebergSchema(s *schema.Schema) IcebergSchema {
	is := IcebergSchema{
		Type:     "struct",
		SchemaID: 0,
		Fields:   make([]IcebergField, len(s.Columns)),
	}

	for i, col := range s.Columns {
		is.Fields[i] = IcebergField{
			ID:       i + 1,
			Name:     col.Name,
			Type:     mapToIcebergType(col.Type),
			Required: !col.Nullable,
		}
	}

	return is
}
