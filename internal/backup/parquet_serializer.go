package backup

import (
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/dinocodesx/gomigrate/internal/record"
	"github.com/dinocodesx/gomigrate/internal/schema"
)

// ParquetSerializer implements Serializer for the Apache Parquet format using Apache Arrow as an intermediary.
// Records are buffered in-memory and converted into Arrow RecordBatches.
// These batches are then written as Parquet Row Groups when batchSize is reached.
// Using Arrow ensures high-performance serialization and strong typing.
type ParquetSerializer struct {
	schema      *schema.Schema // Internal gomigrate schema.
	arrowSchema *arrow.Schema  // Mapped Arrow schema.
	pool        memory.Allocator
	batchSize   int // Number of records to buffer before flushing a row group.

	// writer is the Arrow-to-Parquet file writer. It is bound by Open().
	writer *pqarrow.FileWriter
	rows   []*record.Record // In-memory buffer for the current batch.
}

// NewParquetSerializer creates a ParquetSerializer for the given table schema.
// It pre-converts the internal schema to an Arrow schema.
func NewParquetSerializer(s *schema.Schema, batchSize int) (*ParquetSerializer, error) {
	arrowSchema, err := convertToArrowSchema(s)
	if err != nil {
		return nil, fmt.Errorf("failed to convert schema to Arrow: %w", err)
	}
	if batchSize <= 0 {
		batchSize = 1000
	}
	return &ParquetSerializer{
		schema:      s,
		arrowSchema: arrowSchema,
		pool:        memory.NewGoAllocator(), // Standard Go allocator for Arrow memory.
		batchSize:   batchSize,
	}, nil
}

// Open binds the serializer to an output stream and initializes the Parquet writer.
// It uses Zstd compression by default for Parquet pages.
func (s *ParquetSerializer) Open(w io.Writer) error {
	if s.writer != nil {
		return fmt.Errorf("ParquetSerializer: Open called more than once")
	}

	// Configure Parquet writer properties.
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd))
	arrowProps := pqarrow.DefaultWriterProps()

	fw, err := pqarrow.NewFileWriter(s.arrowSchema, w, writerProps, arrowProps)
	if err != nil {
		return fmt.Errorf("failed to create Parquet file writer: %w", err)
	}
	s.writer = fw
	return nil
}

// Serialize adds a record to the in-memory buffer.
// If the buffer reaches batchSize, it triggers an automatic flush to the writer.
func (s *ParquetSerializer) Serialize(rec *record.Record) error {
	if s.writer == nil {
		return fmt.Errorf("ParquetSerializer: Open must be called before Serialize")
	}
	s.rows = append(s.rows, rec)
	if len(s.rows) >= s.batchSize {
		return s.flush()
	}
	return nil
}

// Close flushes any remaining buffered rows and writes the Parquet footer (metadata).
// The footer is essential for Parquet files to be valid.
func (s *ParquetSerializer) Close() error {
	if s.writer == nil {
		return nil
	}
	if err := s.flush(); err != nil {
		return err
	}
	if err := s.writer.Close(); err != nil {
		return fmt.Errorf("failed to close Parquet writer: %w", err)
	}
	s.writer = nil
	return nil
}

// flush converts the buffered rows into an Arrow RecordBatch and writes it as a Parquet Row Group.
// This is the core memory-to-disk transition point.
func (s *ParquetSerializer) flush() error {
	if len(s.rows) == 0 {
		return nil
	}

	// Create an Arrow RecordBuilder to assemble columns.
	rb := array.NewRecordBuilder(s.pool, s.arrowSchema)
	defer rb.Release()

	// Populate column-wise data for the Arrow batch.
	for i, col := range s.schema.Columns {
		fieldBuilder := rb.Field(i)
		for _, rec := range s.rows {
			val := rec.Data[col.Name]
			if val == nil {
				fieldBuilder.AppendNull()
				continue
			}
			// Map and append the value to the appropriate Arrow builder.
			if err := appendValue(fieldBuilder, val, col.Type); err != nil {
				return fmt.Errorf("column %q: %w", col.Name, err)
			}
		}
	}

	// Generate the RecordBatch and write it to the Parquet file.
	arrowRec := rb.NewRecordBatch()
	defer arrowRec.Release()

	if err := s.writer.Write(arrowRec); err != nil {
		return fmt.Errorf("failed to write Arrow record: %w", err)
	}

	// Clear the row buffer while retaining capacity.
	s.rows = s.rows[:0]
	return nil
}
