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

// ParquetSerializer implements Serializer for Apache Parquet format.
//
// Records are buffered in-memory and flushed as row groups when batchSize is
// reached. Call Close() to flush the final (possibly partial) row group and
// write the Parquet footer.
type ParquetSerializer struct {
	schema      *schema.Schema
	arrowSchema *arrow.Schema
	pool        memory.Allocator
	batchSize   int

	// writer is bound by Open(); it is nil until Open is called.
	writer *pqarrow.FileWriter
	rows   []*record.Record
}

// NewParquetSerializer creates a ParquetSerializer for the given schema.
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

// Open binds the serializer to w and creates the Parquet file writer.
// Open may not be called more than once per serializer instance.
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

// Serialize buffers rec and flushes a row group when batchSize is reached.
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

// Close flushes any remaining buffered rows and writes the Parquet footer.
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

// flush writes the currently buffered rows as a single Arrow record batch.
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

	arrowRec := rb.NewRecord()
	defer arrowRec.Release()

	if err := s.writer.Write(arrowRec); err != nil {
		return fmt.Errorf("failed to write Arrow record: %w", err)
	}

	s.rows = s.rows[:0]
	return nil
}
