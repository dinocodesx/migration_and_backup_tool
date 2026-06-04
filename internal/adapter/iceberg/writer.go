package iceberg

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/dinocodesx/gomigrate/internal/record"
	"github.com/dinocodesx/gomigrate/internal/schema"
	"github.com/dinocodesx/gomigrate/internal/storage"
)

// Writer handles ingestion of record batches into Iceberg tables.
// It serializes data to Parquet files and commits snapshots to the catalog.
type Writer struct {
	// catalog is the client for interacting with the Iceberg REST Catalog.
	catalog   *CatalogClient
	// namespace is the Iceberg namespace of the target table.
	namespace string
	// table is the name of the target Iceberg table.
	table     string
	// storage is the backend (e.g., S3, Local) where Parquet files will be uploaded.
	storage   storage.Storage
	// schema is the cached canonical schema used for Arrow RecordBatch construction.
	schema    *schema.Schema
}

// NewWriter initializes a new Iceberg writer instance.
func NewWriter(catalog *CatalogClient, namespace, table string, s storage.Storage) *Writer {
	return &Writer{
		catalog:   catalog,
		namespace: namespace,
		table:     table,
		storage:   s,
	}
}

// ApplySchema ensures the target table exists in the Iceberg catalog.
// If not found, it creates the table with a schema derived from the canonical input.
func (w *Writer) ApplySchema(ctx context.Context, s *schema.Schema) error {
	w.schema = s
	_, err := w.catalog.GetTable(ctx, w.namespace, w.table)
	if err == nil {
		return nil // Table already exists, idempotency achieved.
	}

	// Create a new Iceberg schema from the canonical definition.
	is := CreateIcebergSchema(s)
	request := map[string]any{
		"name":     w.table,
		"schema":   is,
		"location": fmt.Sprintf("s3://bucket/%s/%s", w.namespace, w.table),
	}

	if err := w.catalog.CreateTable(ctx, w.namespace, request); err != nil {
		return fmt.Errorf("failed to create Iceberg table %s: %w", w.table, err)
	}

	return nil
}

// WriteBatch serializes the provided records into a Parquet file using Arrow,
// uploads it to the storage backend, and commits the file as a new snapshot in the catalog.
func (w *Writer) WriteBatch(ctx context.Context, batch []*record.Record) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}

	if w.schema == nil {
		return 0, fmt.Errorf("ApplySchema must be called before WriteBatch")
	}

	// 1. Map canonical schema to Arrow schema.
	arrowFields := make([]arrow.Field, len(w.schema.Columns))
	for i, col := range w.schema.Columns {
		dt, _ := getArrowType(col.Type)
		arrowFields[i] = arrow.Field{Name: col.Name, Type: dt, Nullable: col.Nullable}
	}
	arrowSchema := arrow.NewSchema(arrowFields, nil)

	// 2. Build Arrow RecordBatch from records.
	pool := memory.NewGoAllocator()
	rb := array.NewRecordBuilder(pool, arrowSchema)
	defer rb.Release()

	for i, col := range w.schema.Columns {
		fb := rb.Field(i)
		for _, rec := range batch {
			val := rec.Data[col.Name]
			if val == nil {
				fb.AppendNull()
				continue
			}
			if err := appendValue(fb, val, col.Type); err != nil {
				return 0, err
			}
		}
	}
	arrowRec := rb.NewRecordBatch()
	defer arrowRec.Release()

	// 3. Write Arrow RecordBatch to an in-memory Parquet buffer.
	var buf bytes.Buffer
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd))
	arrowProps := pqarrow.DefaultWriterProps()
	fw, err := pqarrow.NewFileWriter(arrowSchema, &buf, writerProps, arrowProps)
	if err != nil {
		return 0, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	if err := fw.Write(arrowRec); err != nil {
		return 0, fmt.Errorf("failed to write arrow record: %w", err)
	}
	if err := fw.Close(); err != nil {
		return 0, fmt.Errorf("failed to close parquet writer: %w", err)
	}

	// 4. Upload the serialized Parquet file to the data storage location.
	fileName := fmt.Sprintf("%s/data/%d.parquet", w.table, time.Now().UnixNano())
	if err := w.storage.Put(ctx, fileName, &buf); err != nil {
		return 0, fmt.Errorf("failed to upload data file: %w", err)
	}

	// 5. Commit the new data file to the catalog as an atomic snapshot update.
	commit := map[string]any{
		"action": "append",
		"updates": []map[string]any{
			{
				"action": "add-data-file",
				"file": map[string]any{
					"file-path":          fileName,
					"file-format":        "PARQUET",
					"record-count":       len(batch),
					"file-size-in-bytes": buf.Len(),
				},
			},
		},
	}

	if err := w.catalog.CommitSnapshot(ctx, w.namespace, w.table, commit); err != nil {
		return 0, fmt.Errorf("failed to commit snapshot: %w", err)
	}

	return len(batch), nil
}

// Close gracefully shuts down the writer.
func (w *Writer) Close() error {
	return nil
}

// getArrowType translates a canonical gomigrate type to an Arrow type for ingestion.
func getArrowType(t string) (arrow.DataType, error) {
	switch t {
	case "int64":
		return arrow.PrimitiveTypes.Int64, nil
	case "float64":
		return arrow.PrimitiveTypes.Float64, nil
	case "bool":
		return arrow.FixedWidthTypes.Boolean, nil
	case "timestamp":
		return arrow.FixedWidthTypes.Timestamp_us, nil
	default:
		return arrow.BinaryTypes.String, nil
	}
}

// appendValue safely appends a Go value to an Arrow builder with necessary coercions.
func appendValue(b array.Builder, val any, t string) error {
	switch b := b.(type) {
	case *array.Int64Builder:
		switch v := val.(type) {
		case int64:
			b.Append(v)
		case int:
			b.Append(int64(v))
		default:
			return fmt.Errorf("cannot coerce %T to int64", val)
		}
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
		v, ok := val.(time.Time)
		if !ok {
			return fmt.Errorf("expected time.Time, got %T", val)
		}
		b.Append(arrow.Timestamp(v.UnixMicro()))
	case *array.StringBuilder:
		b.Append(fmt.Sprintf("%v", val))
	}
	return nil
}
