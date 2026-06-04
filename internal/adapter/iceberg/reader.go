package iceberg

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/dinocodesx/gomigrate/internal/adapter"
	"github.com/dinocodesx/gomigrate/internal/record"
	"github.com/dinocodesx/gomigrate/internal/storage"
)

// Reader handles parallel data extraction from Iceberg tables by reading Parquet files.
type Reader struct {
	// catalog is the client for fetching table metadata and data locations.
	catalog *CatalogClient
	// namespace is the Iceberg namespace of the target table.
	namespace string
	// storage is the backend (e.g., S3, Local) where Parquet files reside.
	storage storage.Storage
}

// NewReader initializes a new Iceberg reader instance.
func NewReader(catalog *CatalogClient, namespace string, s storage.Storage) *Reader {
	return &Reader{
		catalog:   catalog,
		namespace: namespace,
		storage:   s,
	}
}

// Partitions discovers all data files associated with the latest table snapshot.
// It maps each Parquet file path to a Partition object, enabling concurrent
// extraction workers to process one file at a time.
func (r *Reader) Partitions(ctx context.Context, table string, n int) ([]adapter.Partition, error) {
	resp, err := r.catalog.GetTable(ctx, r.namespace, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata: %w", err)
	}

	// Data discovery: In a full Iceberg implementation, we would traverse the
	// Manifest List -> Manifests -> Data Files tree. For this version, we
	// discover data files by listing the data prefix in the storage backend.
	prefix := strings.TrimPrefix(resp.Metadata.Location, "s3://bucket/")
	files, err := r.storage.List(ctx, prefix+"/data")
	if err != nil {
		return nil, fmt.Errorf("failed to list data files in %s: %w", resp.Metadata.Location, err)
	}

	var dataFiles []string
	for _, f := range files {
		if strings.HasSuffix(f, ".parquet") {
			dataFiles = append(dataFiles, f)
		}
	}

	partitions := make([]adapter.Partition, len(dataFiles))
	for i, file := range dataFiles {
		partitions[i] = adapter.Partition{
			ID:    fmt.Sprintf("%s-%d", table, i),
			Table: table,
			Start: file, // Start stores the relative file path for extraction
		}
	}

	return partitions, nil
}

// ReadPartition opens a specific Parquet file from the storage backend and streams
// its records into the provided channel using the Apache Arrow Parquet reader.
func (r *Reader) ReadPartition(ctx context.Context, p adapter.Partition, ch chan<- *record.Record) error {
	filePath, ok := p.Start.(string)
	if !ok {
		return fmt.Errorf("invalid partition start: expected file path string")
	}

	reader, err := r.storage.Get(ctx, filePath)
	if err != nil {
		return fmt.Errorf("failed to open data file %s: %w", filePath, err)
	}
	defer reader.Close()

	// Parquet file format requires random access (seeking) to read metadata
	// from the footer. Ensure the storage stream supports ReadAt and Seek.
	rs, ok := reader.(interface {
		io.ReadSeeker
		io.ReaderAt
	})
	if !ok {
		return fmt.Errorf("storage backend does not support ReaderAtSeeker, required for Parquet")
	}

	// Initialize the Parquet file reader.
	pr, err := file.NewParquetReader(rs)
	if err != nil {
		return fmt.Errorf("failed to create parquet reader for %s: %w", filePath, err)
	}

	// Initialize the high-level Arrow reader on top of the Parquet reader.
	fr, err := pqarrow.NewFileReader(pr, pqarrow.ArrowReadProperties{}, nil)
	if err != nil {
		return fmt.Errorf("failed to create pqarrow reader for %s: %w", filePath, err)
	}

	// Get a record reader to iterate through Arrow RecordBatches.
	rr, err := fr.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to get record reader for %s: %w", filePath, err)
	}
	defer rr.Release()

	// Iterate through Arrow RecordBatches and convert columnar data to Go records.
	for rr.Next() {
		recBatch := rr.RecordBatch()
		for i := 0; i < int(recBatch.NumRows()); i++ {
			rec := &record.Record{
				Data: make(map[string]any),
				Metadata: record.RecordMetadata{
					SourceTable: p.Table,
				},
			}

			// Map each column value to the record map.
			for colIdx := 0; colIdx < int(recBatch.NumCols()); colIdx++ {
				colName := recBatch.ColumnName(colIdx)
				col := recBatch.Column(colIdx)
				rec.Data[colName] = getVal(col, i)
			}
			ch <- rec
		}
	}

	return rr.Err()
}

// getVal performs type-safe extraction of a value from an Arrow array at a specific row index.
func getVal(col arrow.Array, row int) any {
	if col.IsNull(row) {
		return nil
	}

	switch a := col.(type) {
	case *array.Int64:
		return a.Value(row)
	case *array.Float64:
		return a.Value(row)
	case *array.Boolean:
		return a.Value(row)
	case *array.String:
		return a.Value(row)
	case *array.Timestamp:
		// Convert Arrow timestamp to Go time.Time with microsecond precision.
		return a.Value(row).ToTime(arrow.Microsecond)
	default:
		// Fallback to string representation for complex or unsupported types.
		return fmt.Sprintf("%v", col)
	}
}
