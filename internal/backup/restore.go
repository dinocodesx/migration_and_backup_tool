package backup

import (
	"bytes"
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
)

// RestoreEngine handles the restoration workflow.
type RestoreEngine struct {
	storage storage.Storage
	target  adapter.TargetAdapter
}

// NewRestoreEngine creates a new restore engine.
func NewRestoreEngine(s storage.Storage, target adapter.TargetAdapter) *RestoreEngine {
	return &RestoreEngine{
		storage: s,
		target:  target,
	}
}

// Restore restores a backup from storage to the target database.
func (e *RestoreEngine) Restore(ctx context.Context, manifestPath string) error {
	// 1. Load Manifest
	reader, err := e.storage.Get(ctx, manifestPath)
	if err != nil {
		return fmt.Errorf("failed to get manifest: %w", err)
	}
	defer reader.Close()

	var manifest Manifest
	if err := json.NewDecoder(reader).Decode(&manifest); err != nil {
		return fmt.Errorf("failed to decode manifest: %w", err)
	}

	// 2. Apply Schema
	if err := e.target.ApplySchema(ctx, &manifest.SchemaSnapshot); err != nil {
		return fmt.Errorf("failed to apply schema: %w", err)
	}

	// 3. Process Chunks
	for _, chunk := range manifest.Chunks {
		if err := e.restoreChunk(ctx, chunk); err != nil {
			return fmt.Errorf("failed to restore chunk %d: %w", chunk.Index, err)
		}
	}

	return nil
}

func (e *RestoreEngine) restoreChunk(ctx context.Context, chunk Chunk) error {
	// 1. Download and Verify Checksum FIRST
	reader, err := e.storage.Get(ctx, chunk.File)
	if err != nil {
		return fmt.Errorf("failed to get chunk file: %w", err)
	}
	defer reader.Close()

	chunkData, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read chunk: %w", err)
	}

	h := sha256.New()
	h.Write(chunkData)
	actualHash := hex.EncodeToString(h.Sum(nil))

	if actualHash != chunk.SHA256 {
		return fmt.Errorf("checksum mismatch for chunk %d: expected %s, got %s", chunk.Index, chunk.SHA256, actualHash)
	}

	// 2. Decompress
	zr, err := zstd.NewReader(bytes.NewReader(chunkData))
	if err != nil {
		return fmt.Errorf("failed to create zstd reader: %w", err)
	}
	defer zr.Close()

	// 3. Deserialize and Write
	dec := json.NewDecoder(zr)
	batch := make([]*record.Record, 0, 1000)

	for {
		var data map[string]any
		if err := dec.Decode(&data); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to decode record: %w", err)
		}

		batch = append(batch, &record.Record{Data: data})
		if len(batch) >= 1000 {
			if _, err := e.target.WriteBatch(ctx, batch); err != nil {
				return fmt.Errorf("failed to write batch: %w", err)
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if _, err := e.target.WriteBatch(ctx, batch); err != nil {
			return fmt.Errorf("failed to write final batch: %w", err)
		}
	}

	return nil
}
