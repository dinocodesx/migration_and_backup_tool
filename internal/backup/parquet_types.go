package backup

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/dinocodesx/gomigrate/internal/schema"
)

// convertToArrowSchema maps a gomigrate table schema to an Apache Arrow schema.
// This is used for creating typed Parquet files.
func convertToArrowSchema(s *schema.Schema) (*arrow.Schema, error) {
	fields := make([]arrow.Field, len(s.Columns))
	for i, col := range s.Columns {
		dt, err := getArrowType(col.Type)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", col.Name, err)
		}
		fields[i] = arrow.Field{Name: col.Name, Type: dt, Nullable: col.Nullable}
	}
	return arrow.NewSchema(fields, nil), nil
}

// getArrowType maps internal gomigrate type strings to Arrow data types.
func getArrowType(t string) (arrow.DataType, error) {
	switch t {
	case "int64", "integer", "bigint":
		return arrow.PrimitiveTypes.Int64, nil
	case "string", "text", "varchar":
		return arrow.BinaryTypes.String, nil
	case "float64", "double":
		return arrow.PrimitiveTypes.Float64, nil
	case "bool", "boolean":
		return arrow.FixedWidthTypes.Boolean, nil
	case "timestamp", "datetime", "timestamptz":
		// We use microsecond precision for timestamps to match most DB resolutions.
		return arrow.FixedWidthTypes.Timestamp_us, nil
	case "map", "array", "blob", "null", "any":
		// Complex or unsupported types are currently stringified (JSON) for compatibility.
		return arrow.BinaryTypes.String, nil
	default:
		// Fallback to string for unknown types to prevent data loss.
		return arrow.BinaryTypes.String, nil
	}
}

// appendValue handles the type-safe insertion of Go values into Arrow builders.
// It includes coercion logic to handle minor type mismatches from source databases.
func appendValue(b array.Builder, val any, t string) error {
	switch b := b.(type) {
	case *array.Int64Builder:
		switch v := val.(type) {
		case int64:
			b.Append(v)
		case int:
			b.Append(int64(v))
		case int32:
			b.Append(int64(v))
		case float64:
			b.Append(int64(v)) // Coerce float to int if schema expects int.
		default:
			return fmt.Errorf("cannot coerce %T to int64", val)
		}
	case *array.StringBuilder:
		switch v := val.(type) {
		case string:
			b.Append(v)
		default:
			// Automatically stringify complex objects (maps, slices) or numbers.
			b.Append(fmt.Sprintf("%v", v))
		}
	case *array.Float64Builder:
		switch v := val.(type) {
		case float64:
			b.Append(v)
		case float32:
			b.Append(float64(v))
		case int64:
			b.Append(float64(v))
		default:
			return fmt.Errorf("cannot coerce %T to float64", val)
		}
	case *array.BooleanBuilder:
		v, ok := val.(bool)
		if !ok {
			return fmt.Errorf("expected bool, got %T", val)
		}
		b.Append(v)
	case *array.TimestampBuilder:
		switch v := val.(type) {
		case time.Time:
			// Arrow timestamps are represented as int64 units from epoch.
			b.Append(arrow.Timestamp(v.UTC().UnixMicro()))
		case int64:
			b.Append(arrow.Timestamp(v))
		default:
			return fmt.Errorf("cannot coerce %T to timestamp", val)
		}
	default:
		return fmt.Errorf("unsupported Arrow builder type %T for column type %q", b, t)
	}
	return nil
}
