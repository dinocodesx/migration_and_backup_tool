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

	actualHash := hex.EncodeToString(h.Sum(nil))
	if actualHash != chunk.SHA256 {
		return totalRows, fmt.Errorf(
			"checksum mismatch for chunk %d: expected %s, got %s",
			chunk.Index, chunk.SHA256, actualHash,
		)
	}

	return totalRows, nil
}
