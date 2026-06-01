package backup

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/dinocodesx/gomigrate/internal/schema"
)

// convertToArrowSchema converts a source schema into an Arrow schema.
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
		return arrow.FixedWidthTypes.Timestamp_us, nil
	case "map", "array", "blob", "null", "any":
		// Fallback: store as JSON string.
		return arrow.BinaryTypes.String, nil
	default:
		return arrow.BinaryTypes.String, nil
	}
}

// appendValue appends a single value to the appropriate Arrow builder.
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
			b.Append(int64(v))
		default:
			return fmt.Errorf("cannot coerce %T to int64", val)
		}
	case *array.StringBuilder:
		switch v := val.(type) {
		case string:
			b.Append(v)
		default:
			// Stringify anything else (maps, arrays, etc.).
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
