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

// RestoreEngine handles the restoration workflow.
type RestoreEngine struct {
	storage storage.Storage
	target  adapter.TargetAdapter
	logger  *zap.Logger
}

// NewRestoreEngine creates a new RestoreEngine.
func NewRestoreEngine(s storage.Storage, target adapter.TargetAdapter, logger *zap.Logger) *RestoreEngine {
	return &RestoreEngine{
		storage: s,
		target:  target,
		logger:  logger,
	}
}

// Restore restores a backup from storage to the target database.
//
//  1. Reads and parses manifest.json from manifestPath.
//  2. Applies the stored schema snapshot to the target.
//  3. Verifies the SHA-256 of each chunk while streaming (no full in-memory load).
//  4. Decompresses and deserialises each chunk, writing in batches to the target.
func (e *RestoreEngine) Restore(ctx context.Context, manifestPath string) error {
	// ── 1. Load Manifest ──────────────────────────────────────────────────────
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

	// ── 2. Apply Schema ───────────────────────────────────────────────────────
	if err := e.target.ApplySchema(ctx, &manifest.SchemaSnapshot); err != nil {
		return fmt.Errorf("failed to apply schema: %w", err)
	}

	// ── 3. Restore Chunks ─────────────────────────────────────────────────────
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

// restoreChunk downloads one chunk, verifies its SHA-256 while streaming,
// decompresses it, and writes records in batches to the target.
// It never loads the entire chunk into memory.
func (e *RestoreEngine) restoreChunk(ctx context.Context, chunk Chunk) (int64, error) {
	reader, err := e.storage.Get(ctx, chunk.File)
	if err != nil {
		return 0, fmt.Errorf("failed to get chunk file: %w", err)
	}
	defer reader.Close()

	// ── Streaming checksum verification ───────────────────────────────────────
	// TeeReader hashes bytes as they are read — no extra memory allocation.
	h := sha256.New()
	teeReader := io.TeeReader(reader, h)

	// ── Decompress ────────────────────────────────────────────────────────────
	zr, err := zstd.NewReader(teeReader)
	if err != nil {
		return 0, fmt.Errorf("failed to create zstd reader: %w", err)
	}
	defer zr.Close()

	// ── Deserialize and write in batches ──────────────────────────────────────
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

	// Flush remaining records.
	if len(batch) > 0 {
		n, err := e.target.WriteBatch(ctx, batch)
		if err != nil {
			return totalRows, fmt.Errorf("failed to write final batch: %w", err)
		}
		totalRows += int64(n)
	}

	// ── Verify checksum AFTER streaming (prevents TOCTOU on the data) ─────────
	// At this point all bytes have been read through teeReader, so h is complete.
	actualHash := hex.EncodeToString(h.Sum(nil))
	if actualHash != chunk.SHA256 {
		return totalRows, fmt.Errorf(
			"checksum mismatch for chunk %d: expected %s, got %s",
			chunk.Index, chunk.SHA256, actualHash,
		)
	}

	return totalRows, nil
}
