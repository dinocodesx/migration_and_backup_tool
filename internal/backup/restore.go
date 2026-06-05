package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"
)

// RestoreEngine manages the reconstruction of a database from stored backup artifacts.
// It orchestrates reading from storage, verifying integrity, and ingesting data
// into a target database adapter.
type RestoreEngine struct {
	// storage is the source backend containing the backup artifacts.
	storage storage.Storage
	// target is the destination database adapter for the restored data.
	target adapter.TargetAdapter
	// logger provides structured observability for the restoration process.
	logger *zap.Logger
}

// NewRestoreEngine initializes a new RestoreEngine instance.
func NewRestoreEngine(s storage.Storage, target adapter.TargetAdapter, logger *zap.Logger) *RestoreEngine {
	return &RestoreEngine{
		storage: s,
		target:  target,
		logger:  logger,
	}
}

// Restore performs a full restoration of a table using the roadmap provided
// by a backup manifest file. It applies the schema snapshot, verifies chunk
// integrity, and streams data into the target database.
func (e *RestoreEngine) Restore(ctx context.Context, manifestPath string) error {
	mReader, err := e.storage.Get(ctx, manifestPath)
	if err != nil {
		return fmt.Errorf("failed to get manifest %q: %w", manifestPath, err)
	}
	defer mReader.Close()

	var manifest Manifest
	if err := json.NewDecoder(mReader).Decode(&manifest); err != nil {
		return fmt.Errorf("failed to decode manifest: %w", err)
	}

	e.logger.Info("starting restore",
		zap.String("operation_id", manifest.OperationID),
		zap.Int64("expected_rows", manifest.RowCount),
		zap.Int("chunks", len(manifest.Chunks)),
	)

	if err := e.target.ApplySchema(ctx, &manifest.SchemaSnapshot); err != nil {
		return fmt.Errorf("failed to apply schema: %w", err)
	}

	var totalRows int64
	for _, chunk := range manifest.Chunks {
		rows, err := e.restoreChunk(ctx, chunk)
		if err != nil {
			return fmt.Errorf("failed to restore chunk %d (%s): %w", chunk.Index, chunk.File, err)
		}
		totalRows += rows
		e.logger.Info("chunk restored",
			zap.Int("chunk_index", chunk.Index),
			zap.Int64("rows", rows),
		)
	}

	e.logger.Info("restore complete",
		zap.String("operation_id", manifest.OperationID),
		zap.Int64("total_rows", totalRows),
	)
	return nil
}

// restoreChunk handles the low-level logic of streaming a single compressed
// chunk from storage to the target database. It performs streaming SHA256
// verification to ensure end-to-end data integrity.
func (e *RestoreEngine) restoreChunk(ctx context.Context, chunk Chunk) (int64, error) {
	reader, err := e.storage.Get(ctx, chunk.File)
	if err != nil {
		return 0, fmt.Errorf("failed to get chunk file: %w", err)
	}
	defer reader.Close()

	h := sha256.New()
	teeReader := io.TeeReader(reader, h)

	zr, err := zstd.NewReader(teeReader)
	if err != nil {
		return 0, fmt.Errorf("failed to create zstd reader: %w", err)
	}
	defer zr.Close()

	// Since some formats like Parquet require random access (seeking) to read
	// metadata from the footer, and S3/Zstd streams are linear, we must buffer
	// the decompressed data into memory.
	data, err := io.ReadAll(zr)
	if err != nil {
		return 0, fmt.Errorf("failed to read chunk data: %w", err)
	}

	// Verify checksum after reading all data.
	actualHash := hex.EncodeToString(h.Sum(nil))
	if actualHash != chunk.SHA256 {
		return 0, fmt.Errorf(
			"checksum mismatch for chunk %d: expected %s, got %s",
			chunk.Index, chunk.SHA256, actualHash,
		)
	}

	// Determine the format based on file extension or magic bytes.
	isParquet := strings.Contains(chunk.File, ".parquet") || bytes.HasPrefix(data, []byte("PAR1"))

	if isParquet {
		return e.restoreParquetChunk(ctx, data)
	}
	return e.restoreJSONChunk(ctx, data)
}

func (e *RestoreEngine) restoreJSONChunk(ctx context.Context, data []byte) (int64, error) {
	const batchCapacity = 1000
	dec := json.NewDecoder(bytes.NewReader(data))
	batch := make([]*record.Record, 0, batchCapacity)
	var totalRows int64

	for {
		var row map[string]any
		if err := dec.Decode(&row); err != nil {
			if err == io.EOF {
				break
			}
			return totalRows, fmt.Errorf("failed to decode record: %w", err)
		}

		batch = append(batch, &record.Record{Data: row})
		if len(batch) >= batchCapacity {
			n, err := e.target.WriteBatch(ctx, batch)
			if err != nil {
				return totalRows, fmt.Errorf("failed to write batch: %w", err)
			}
			totalRows += int64(n)
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		n, err := e.target.WriteBatch(ctx, batch)
		if err != nil {
			return totalRows, fmt.Errorf("failed to write final batch: %w", err)
		}
		totalRows += int64(n)
	}

	return totalRows, nil
}

func (e *RestoreEngine) restoreParquetChunk(ctx context.Context, data []byte) (int64, error) {
	rs := bytes.NewReader(data)
	pr, err := file.NewParquetReader(rs)
	if err != nil {
		return 0, fmt.Errorf("failed to create parquet reader: %w", err)
	}

	fr, err := pqarrow.NewFileReader(pr, pqarrow.ArrowReadProperties{}, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create pqarrow reader: %w", err)
	}

	rr, err := fr.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to get record reader: %w", err)
	}
	defer rr.Release()

	const batchCapacity = 1000
	batch := make([]*record.Record, 0, batchCapacity)
	var totalRows int64

	for rr.Next() {
		recBatch := rr.RecordBatch()
		for i := 0; i < int(recBatch.NumRows()); i++ {
			rec := &record.Record{
				Data: make(map[string]any),
			}

			for colIdx := 0; colIdx < int(recBatch.NumCols()); colIdx++ {
				colName := recBatch.ColumnName(colIdx)
				col := recBatch.Column(colIdx)
				rec.Data[colName] = getVal(col, i)
			}

			batch = append(batch, rec)
			if len(batch) >= batchCapacity {
				n, err := e.target.WriteBatch(ctx, batch)
				if err != nil {
					return totalRows, fmt.Errorf("failed to write batch: %w", err)
				}
				totalRows += int64(n)
				batch = batch[:0]
			}
		}
	}

	if len(batch) > 0 {
		n, err := e.target.WriteBatch(ctx, batch)
		if err != nil {
			return totalRows, fmt.Errorf("failed to write final batch: %w", err)
		}
		totalRows += int64(n)
	}

	return totalRows, rr.Err()
}

// getVal performs type-safe extraction of a value from an Arrow array at a specific row index.
func getVal(col arrow.Array, row int) any {
	if col.IsNull(row) {
		return nil
	}

	switch a := col.(type) {
	case *array.Int64:
		return a.Value(row)
	case *array.Int32:
		return a.Value(row)
	case *array.Float64:
		return a.Value(row)
	case *array.Float32:
		return a.Value(row)
	case *array.Boolean:
		return a.Value(row)
	case *array.String:
		return a.Value(row)
	case *array.Binary:
		return a.Value(row)
	case *array.Timestamp:
		return a.Value(row).ToTime(arrow.Microsecond)
	default:
		return fmt.Sprintf("%v", col)
	}
}
