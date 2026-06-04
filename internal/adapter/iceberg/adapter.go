// Package iceberg provides an implementation of the gomigrate adapter interfaces
// for Apache Iceberg. It utilizes a REST Catalog for metadata management and
// Apache Arrow/Parquet for high-performance data storage and retrieval.
package iceberg

import (
	"context"

	"github.com/dinocodesx/gomigrate/internal/adapter"
	"github.com/dinocodesx/gomigrate/internal/config"
	"github.com/dinocodesx/gomigrate/internal/record"
	"github.com/dinocodesx/gomigrate/internal/schema"
	"github.com/dinocodesx/gomigrate/internal/storage"
)

// IcebergAdapter implements both adapter.SourceAdapter and adapter.TargetAdapter.
// It acts as a high-level coordinator that manages the lifecycle of Iceberg
// operations, delegating specific tasks to the CatalogClient, Reader, and Writer.
type IcebergAdapter struct {
	// catalog is the client used to interact with the Iceberg REST Catalog.
	catalog   *CatalogClient
	// namespace is the Iceberg namespace (similar to a database name).
	namespace string
	// reader handles partition discovery and Parquet data extraction.
	reader    *Reader
	// writer handles schema application and Parquet data ingestion.
	writer    *Writer
	// storage is the backend (e.g., S3, Local) where data files are stored.
	storage   storage.Storage
}

// NewIcebergAdapter returns an uninitialized IcebergAdapter instance.
func NewIcebergAdapter() *IcebergAdapter {
	return &IcebergAdapter{}
}

// Type returns the adapter identifier "iceberg".
func (a *IcebergAdapter) Type() string { return "iceberg" }

// Connect initializes the Iceberg catalog client and sets up the storage backend.
// It parses the database name as the Iceberg namespace and determines the storage
// strategy from the configuration parameters.
func (a *IcebergAdapter) Connect(ctx context.Context, cfg config.DBConfig) error {
	a.catalog = NewCatalogClient(cfg.Host)
	a.namespace = cfg.Database
	if a.namespace == "" {
		a.namespace = "default"
	}

	// Initialize storage backend based on configuration or params.
	// In a production environment, this would resolve to a cloud-native
	// storage implementation (e.g., S3Storage or GCSStorage) based on the
	// Iceberg table's configured location.
	storageType := cfg.Params["storage_type"]
	if storageType == "" {
		storageType = "local"
	}

	var s storage.Storage
	var err error
	switch storageType {
	case "s3":
		// For the v1 implementation, we utilize local storage as a proxy/placeholder.
		// A full implementation would use the storage.S3 implementation.
		s, err = storage.NewLocalStorage("./iceberg_data")
	default:
		s, err = storage.NewLocalStorage("./iceberg_data")
	}
	if err != nil {
		return err
	}
	a.storage = s
	return nil
}

// Partitions analyzes the Iceberg table to determine the set of data files
// in the latest snapshot. Each data file is treated as an independent partition
// for parallel extraction.
func (a *IcebergAdapter) Partitions(ctx context.Context, table string, n int) ([]adapter.Partition, error) {
	if a.reader == nil {
		a.reader = NewReader(a.catalog, a.namespace, a.storage)
	}
	return a.reader.Partitions(ctx, table, n)
}

// ReadPartition extracts all records from a specific Parquet data file
// and streams them into the provided record channel.
func (a *IcebergAdapter) ReadPartition(ctx context.Context, p adapter.Partition, ch chan<- *record.Record) error {
	if a.reader == nil {
		a.reader = NewReader(a.catalog, a.namespace, a.storage)
	}
	return a.reader.ReadPartition(ctx, p, ch)
}

// Schema retrieves the current table schema from the Iceberg REST Catalog
// and translates it into the gomigrate canonical schema format.
func (a *IcebergAdapter) Schema(ctx context.Context, table string) (*schema.Schema, error) {
	resp, err := a.catalog.GetTable(ctx, a.namespace, table)
	if err != nil {
		return nil, err
	}
	return ConvertToCanonicalSchema(resp.Metadata.Schema, table), nil
}

// WriteBatch serializes a batch of records into a new Parquet data file,
// uploads it to storage, and commits the change as a new snapshot in the catalog.
func (a *IcebergAdapter) WriteBatch(ctx context.Context, batch []*record.Record) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}
	if a.writer == nil {
		a.writer = NewWriter(a.catalog, a.namespace, batch[0].Metadata.SourceTable, a.storage)
	}
	return a.writer.WriteBatch(ctx, batch)
}

// ApplySchema ensures the target Iceberg table exists in the catalog.
// If the table does not exist, it creates a new one with the provided schema.
func (a *IcebergAdapter) ApplySchema(ctx context.Context, s *schema.Schema) error {
	a.writer = NewWriter(a.catalog, a.namespace, s.Name, a.storage)
	return a.writer.ApplySchema(ctx, s)
}

// Close releases any resources used by the adapter, ensuring pending
// writes or commits are finalized.
func (a *IcebergAdapter) Close() error {
	if a.writer != nil {
		return a.writer.Close()
	}
	return nil
}

// Ensure interface compliance at compile time.
var _ adapter.SourceAdapter = (*IcebergAdapter)(nil)
var _ adapter.TargetAdapter = (*IcebergAdapter)(nil)
