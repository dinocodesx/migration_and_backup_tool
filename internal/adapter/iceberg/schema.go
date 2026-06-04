package iceberg

import (
	"github.com/dinocodesx/gomigrate/internal/schema"
)

// mapIcebergType translates Iceberg primitive and complex types to canonical
// gomigrate internal data types. This enables the system to map Iceberg
// columns to relational database columns or NoSQL document fields.
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
		// Handle complex Iceberg types (structs, lists, maps, decimals).
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

// ConvertToCanonicalSchema transforms an Iceberg-specific schema definition
// into the gomigrate universal schema model.
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

// mapToIcebergType converts a canonical gomigrate type back to its closest
// Iceberg-compatible representation for table creation and ingestion.
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
		// Note: complex nesting is simplified to string in v1.
		return "string"
	case "array":
		// Note: complex nesting is simplified to string in v1.
		return "string"
	default:
		return "string"
	}
}

// CreateIcebergSchema generates an Iceberg-compatible schema definition from
// a canonical gomigrate schema. It assigns sequential field IDs as required
// by the Iceberg specification.
func CreateIcebergSchema(s *schema.Schema) IcebergSchema {
	is := IcebergSchema{
		Type:     "struct",
		SchemaID: 0,
		Fields:   make([]IcebergField, len(s.Columns)),
	}

	for i, col := range s.Columns {
		// Field IDs must be unique and stable for Iceberg schema evolution.
		is.Fields[i] = IcebergField{
			ID:       i + 1,
			Name:     col.Name,
			Type:     mapToIcebergType(col.Type),
			Required: !col.Nullable,
		}
	}

	return is
}
