package iceberg

import (
	"context"

	"github.com/dinocodesx/gomigrate/internal/adapter"
	"github.com/dinocodesx/gomigrate/internal/config"
	"github.com/dinocodesx/gomigrate/internal/record"
	"github.com/dinocodesx/gomigrate/internal/schema"
)

// IcebergAdapter implements both adapter.SourceAdapter and adapter.TargetAdapter.
// It interacts with an Iceberg REST Catalog to manage table metadata and data files.
type IcebergAdapter struct {
	catalog   *CatalogClient
	namespace string
	reader    *Reader
	writer    *Writer
}

// NewIcebergAdapter returns an uninitialized IcebergAdapter instance.
func NewIcebergAdapter() *IcebergAdapter {
	return &IcebergAdapter{}
}

// Type returns the adapter identifier "iceberg".
func (a *IcebergAdapter) Type() string { return "iceberg" }

// Connect initializes the Iceberg catalog client and sets the default namespace.
func (a *IcebergAdapter) Connect(ctx context.Context, cfg config.DBConfig) error {
	a.catalog = NewCatalogClient(cfg.Host)
	a.namespace = cfg.Database
	if a.namespace == "" {
		a.namespace = "default"
	}
	return nil
}

// Partitions retrieves the latest snapshot metadata and maps Parquet data files to partitions.
func (a *IcebergAdapter) Partitions(ctx context.Context, table string, n int) ([]adapter.Partition, error) {
	if a.reader == nil {
		a.reader = NewReader(a.catalog, a.namespace)
	}
	return a.reader.Partitions(ctx, table, n)
}

// ReadPartition streams records from an Iceberg Parquet data file into the provided channel.
func (a *IcebergAdapter) ReadPartition(ctx context.Context, p adapter.Partition, ch chan<- *record.Record) error {
	if a.reader == nil {
		a.reader = NewReader(a.catalog, a.namespace)
	}
	return a.reader.ReadPartition(ctx, p, ch)
}

// Schema retrieves the current schema from the Iceberg catalog.
func (a *IcebergAdapter) Schema(ctx context.Context, table string) (*schema.Schema, error) {
	resp, err := a.catalog.GetTable(ctx, a.namespace, table)
	if err != nil {
		return nil, err
	}
	return ConvertToCanonicalSchema(resp.Metadata.Schema, table), nil
}

// WriteBatch serializes a batch of records into a Parquet file and prepares for catalog commit.
func (a *IcebergAdapter) WriteBatch(ctx context.Context, batch []*record.Record) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}
	if a.writer == nil {
		a.writer = NewWriter(a.catalog, a.namespace, batch[0].Metadata.SourceTable)
	}
	return a.writer.WriteBatch(ctx, batch)
}

// ApplySchema creates a new Iceberg table via the REST catalog if it doesn't already exist.
func (a *IcebergAdapter) ApplySchema(ctx context.Context, s *schema.Schema) error {
	a.writer = NewWriter(a.catalog, a.namespace, s.Name)
	return a.writer.ApplySchema(ctx, s)
}

// Close releases any resources used by the adapter.
func (a *IcebergAdapter) Close() error {
	if a.writer != nil {
		return a.writer.Close()
	}
	return nil
}

// Ensure interface compliance at compile time.
var _ adapter.SourceAdapter = (*IcebergAdapter)(nil)
var _ adapter.TargetAdapter = (*IcebergAdapter)(nil)
