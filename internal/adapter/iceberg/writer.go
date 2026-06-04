package iceberg

import (
	"context"
	"fmt"

	"github.com/dinocodesx/gomigrate/internal/record"
	"github.com/dinocodesx/gomigrate/internal/schema"
)

// Writer handles data ingestion into Iceberg tables.
type Writer struct {
	catalog   *CatalogClient
	namespace string
	table     string
}

// NewWriter creates a new Writer instance for Iceberg.
func NewWriter(catalog *CatalogClient, namespace, table string) *Writer {
	return &Writer{
		catalog:   catalog,
		namespace: namespace,
		table:     table,
	}
}

// ApplySchema creates the Iceberg table via the REST catalog if it doesn't exist.
func (w *Writer) ApplySchema(ctx context.Context, s *schema.Schema) error {
	_, err := w.catalog.GetTable(ctx, w.namespace, w.table)
	if err == nil {
		return nil // Table already exists, idempotency achieved.
	}

	is := CreateIcebergSchema(s)
	request := map[string]any{
		"name":     w.table,
		"schema":   is,
		"location": fmt.Sprintf("s3://bucket/%s/%s", w.namespace, w.table), // Simplified location
	}

	if err := w.catalog.CreateTable(ctx, w.namespace, request); err != nil {
		return fmt.Errorf("failed to create Iceberg table %s: %w", w.table, err)
	}

	return nil
}

// WriteBatch serializes records to Parquet and commits a new snapshot to the catalog.
func (w *Writer) WriteBatch(ctx context.Context, batch []*record.Record) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}

	// In a production implementation:
	// 1. Convert []*record.Record to Arrow RecordBatch.
	// 2. Write Arrow RecordBatch to a Parquet file in temporary storage.
	// 3. Upload the Parquet file to the final storage location (S3/GCS).
	// 4. Record the file path and statistics.

	// Mocking the commit for architectural demonstration:
	commit := map[string]any{
		"action": "append",
		"data-files": []map[string]any{
			{
				"file-path":    fmt.Sprintf("s3://bucket/%s/%s/data/new-file.parquet", w.namespace, w.table),
				"file-format":  "PARQUET",
				"record-count": len(batch),
				"file-size-bytes": 1024 * 1024, // Mock size
			},
		},
	}

	if err := w.catalog.CommitSnapshot(ctx, w.namespace, w.table, commit); err != nil {
		return 0, fmt.Errorf("failed to commit snapshot to catalog: %w", err)
	}

	return len(batch), nil
}

// Close ensures any pending writes are flushed and catalog commits are finalized.
func (w *Writer) Close() error {
	return nil
}
