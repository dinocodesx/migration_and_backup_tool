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

// ParquetSerializer implements the Serializer interface for Apache Parquet.
// It uses Apache Arrow as an intermediate in-memory representation to provide
// high-performance, strongly-typed serialization and efficient compression.
type ParquetSerializer struct {
	// schema is the internal gomigrate schema definition.
	schema *schema.Schema
	// arrowSchema is the mapped Apache Arrow schema.
	arrowSchema *arrow.Schema
	// pool is the allocator for Arrow memory buffers.
	pool memory.Allocator
	// batchSize is the number of records to buffer in memory before flushing a row group.
	batchSize int
	// writer is the active Parquet file writer.
	writer *pqarrow.FileWriter
	// rows is an in-memory buffer of records for the current batch.
	rows []*record.Record
}

// NewParquetSerializer creates a new ParquetSerializer for the specified schema.
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
		pool:        memory.NewGoAllocator(),
		batchSize:   batchSize,
	}, nil
}

// Open initializes the Parquet writer on top of the provided output stream.
func (s *ParquetSerializer) Open(w io.Writer) error {
	if s.writer != nil {
		return fmt.Errorf("ParquetSerializer: Open called more than once")
	}

	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd))
	arrowProps := pqarrow.DefaultWriterProps()

	fw, err := pqarrow.NewFileWriter(s.arrowSchema, w, writerProps, arrowProps)
	if err != nil {
		return fmt.Errorf("failed to create Parquet file writer: %w", err)
	}
	s.writer = fw
	return nil
}

// Serialize buffers the record in memory and flushes a row group if the batch size is reached.
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

// Close flushes any remaining buffered records and finalizes the Parquet file metadata.
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

// flush converts the buffered Go records into an Arrow RecordBatch and writes it
// as a new row group in the Parquet file.
func (s *ParquetSerializer) flush() error {
	if len(s.rows) == 0 {
		return nil
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
				return fmt.Errorf("column %q: %w", col.Name, err)
			}
		}
	}

	arrowRec := rb.NewRecordBatch()
	defer arrowRec.Release()

	if err := s.writer.Write(arrowRec); err != nil {
		return fmt.Errorf("failed to write Arrow record: %w", err)
	}

	s.rows = s.rows[:0]
	return nil
}
