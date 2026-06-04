package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	"github.com/dinocodesx/gomigrate/internal/adapter"
	"github.com/dinocodesx/gomigrate/internal/record"
	"github.com/dinocodesx/gomigrate/internal/storage"
	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"
)

// RestoreEngine handles the reconstruction of a database from a backup.
// It reads from a storage backend and writes to a target database adapter.
type RestoreEngine struct {
	storage storage.Storage       // Source storage (S3, GCS, Local).
	target  adapter.TargetAdapter // Destination database (Postgres, Mongo, etc.).
	logger  *zap.Logger
}

// NewRestoreEngine creates a new RestoreEngine instance.
func NewRestoreEngine(s storage.Storage, target adapter.TargetAdapter, logger *zap.Logger) *RestoreEngine {
	return &RestoreEngine{
		storage: s,
		target:  target,
		logger:  logger,
	}
}

/* Restore orchestrates the full restoration process for a table.
 *
 * Workflow:
 * 1. Load and parse the manifest.json to get the "roadmap" of the backup.
 * 2. Apply the schema snapshot to the target database (idempotent table creation).
 * 3. Iterate through each chunk, verifying integrity and streaming data to the target.
 */
func (e *RestoreEngine) Restore(ctx context.Context, manifestPath string) error {
	// 1. Load Manifest
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

	// 2. Apply Schema
	// This ensures the table structure exists before we start inserting data.
	if err := e.target.ApplySchema(ctx, &manifest.SchemaSnapshot); err != nil {
		return fmt.Errorf("failed to apply schema: %w", err)
	}

	// 3. Restore Chunks
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

// restoreChunk handles the low-level data movement for a single chunk file.
// It uses a streaming approach to minimize memory usage:
//
//	Storage -> Checksum Hash (TeeReader) -> Decompressor -> Deserializer -> Batch Writer -> Target DB
func (e *RestoreEngine) restoreChunk(ctx context.Context, chunk Chunk) (int64, error) {
	reader, err := e.storage.Get(ctx, chunk.File)
	if err != nil {
		return 0, fmt.Errorf("failed to get chunk file: %w", err)
	}
	defer reader.Close()

	// Streaming Checksum Verification
	// io.TeeReader passes every byte read from the storage stream through the SHA256 hasher.
	// This allows us to verify the file integrity WITHOUT reading it into memory first.
	h := sha256.New()
	teeReader := io.TeeReader(reader, h)

	// Decompression
	zr, err := zstd.NewReader(teeReader)
	if err != nil {
		return 0, fmt.Errorf("failed to create zstd reader: %w", err)
	}
	defer zr.Close()

	// Deserialization & Batch Writing
	// Currently implements NDJSON restoration logic.
	// (TODO: Add Parquet restoration support by checking manifest.Source metadata).
	const batchCapacity = 1000
	dec := json.NewDecoder(zr)
	batch := make([]*record.Record, 0, batchCapacity)
	var totalRows int64

	for {
		var data map[string]any
		if err := dec.Decode(&data); err != nil {
			if err == io.EOF {
				break
			}
			return totalRows, fmt.Errorf("failed to decode record: %w", err)
		}

		batch = append(batch, &record.Record{Data: data})
		if len(batch) >= batchCapacity {
			// Write a full batch to the database.
			n, err := e.target.WriteBatch(ctx, batch)
			if err != nil {
				return totalRows, fmt.Errorf("failed to write batch: %w", err)
			}
			totalRows += int64(n)
			batch = batch[:0]
		}
	}

	// Flush any remaining records in the partial final batch.
	if len(batch) > 0 {
		n, err := e.target.WriteBatch(ctx, batch)
		if err != nil {
			return totalRows, fmt.Errorf("failed to write final batch: %w", err)
		}
		totalRows += int64(n)
	}

	// Post-Stream Checksum Verification
	// We verify the hash AFTER all records are processed. This is safe because
	// the database transaction has not been committed or we can rollback if needed.
	actualHash := hex.EncodeToString(h.Sum(nil))
	if actualHash != chunk.SHA256 {
		return totalRows, fmt.Errorf(
			"checksum mismatch for chunk %d: expected %s, got %s",
			chunk.Index, chunk.SHA256, actualHash,
		)
	}

	return totalRows, nil
}
