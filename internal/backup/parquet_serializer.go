package backup

import (
	"fmt"
	"io"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/dinocodesx/gomigrate/internal/record"
	"github.com/dinocodesx/gomigrate/internal/schema"
)

// ParquetSerializer implements the Serializer interface for Parquet format.
type ParquetSerializer struct {
	schema      *schema.Schema
	arrowSchema *arrow.Schema
	pool        memory.Allocator
	rows        []*record.Record
	writer      *pqarrow.FileWriter
	batchSize   int
}

// NewParquetSerializer creates a new ParquetSerializer for the given schema.
func NewParquetSerializer(s *schema.Schema) (*ParquetSerializer, error) {
	arrowSchema, err := convertToArrowSchema(s)
	if err != nil {
		return nil, err
	}

	return &ParquetSerializer{
		schema:      s,
		arrowSchema: arrowSchema,
		pool:        memory.NewGoAllocator(),
		batchSize:   1000, // Default batch size for flushing
	}, nil
}

func (s *ParquetSerializer) Serialize(w io.Writer, rec *record.Record) error {
	if s.writer == nil {
		writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd))
		arrowProps := pqarrow.DefaultWriterProps()
		var err error
		s.writer, err = pqarrow.NewFileWriter(s.arrowSchema, w, writerProps, arrowProps)
		if err != nil {
			return fmt.Errorf("failed to create parquet writer: %w", err)
		}
	}

	s.rows = append(s.rows, rec)

	if len(s.rows) >= s.batchSize {
		return s.Flush(w)
	}

	return nil
}

func (s *ParquetSerializer) Flush(w io.Writer) error {
	if len(s.rows) == 0 {
		return nil
	}
	if s.writer == nil {
		return fmt.Errorf("writer not initialized")
	}

	rb := array.NewRecordBuilder(s.pool, s.arrowSchema)
	defer rb.Release()

	for i, col := range s.schema.Columns {
		fieldBuilder := rb.Field(i)
		for _, rec := range s.rows {
			val := rec.Data[col.Name]
			if val == nil {
				fieldBuilder.AppendNull()
				continue
			}
			if err := appendValue(fieldBuilder, val, col.Type); err != nil {
				return fmt.Errorf("failed to append value for column %s: %w", col.Name, err)
			}
		}
	}

	arrowRec := rb.NewRecord()
	defer arrowRec.Release()

	if err := s.writer.Write(arrowRec); err != nil {
		return fmt.Errorf("failed to write arrow record: %w", err)
	}

	s.rows = nil
	return nil
}

func (s *ParquetSerializer) Close(w io.Writer) error {
	if err := s.Flush(w); err != nil {
		return err
	}
	if s.writer != nil {
		if err := s.writer.Close(); err != nil {
			return fmt.Errorf("failed to close parquet writer: %w", err)
		}
		s.writer = nil
	}
	return nil
}

func convertToArrowSchema(s *schema.Schema) (*arrow.Schema, error) {
	fields := make([]arrow.Field, len(s.Columns))
	for i, col := range s.Columns {
		dt, err := getArrowType(col.Type)
		if err != nil {
			return nil, err
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
	default:
		return nil, fmt.Errorf("unsupported type: %s", t)
	}
}

func appendValue(b array.Builder, val any, t string) error {
	switch b := b.(type) {
	case *array.Int64Builder:
		v, ok := val.(int64)
		if !ok {
			// Try conversion from other int types
			switch i := val.(type) {
			case int:
				v = int64(i)
			case int32:
				v = int64(i)
			default:
				return fmt.Errorf("expected int64, got %T", val)
			}
		}
		b.Append(v)
	case *array.StringBuilder:
		v, ok := val.(string)
		if !ok {
			return fmt.Errorf("expected string, got %T", val)
		}
		b.Append(v)
	case *array.Float64Builder:
		v, ok := val.(float64)
		if !ok {
			return fmt.Errorf("expected float64, got %T", val)
		}
		b.Append(v)
	case *array.BooleanBuilder:
		v, ok := val.(bool)
		if !ok {
			return fmt.Errorf("expected bool, got %T", val)
		}
		b.Append(v)
	case *array.TimestampBuilder:
		var v arrow.Timestamp
		switch t := val.(type) {
		case time.Time:
			v = arrow.Timestamp(t.UnixMicro())
		case int64:
			v = arrow.Timestamp(t)
		default:
			return fmt.Errorf("expected time.Time or int64 for timestamp, got %T", val)
		}
		b.Append(v)
	default:
		return fmt.Errorf("unsupported builder type: %T", b)
	}
	return nil
}
