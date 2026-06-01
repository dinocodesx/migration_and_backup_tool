package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/dinocodesx/gomigrate/internal/adapter"
	"github.com/dinocodesx/gomigrate/internal/record"
	"github.com/dinocodesx/gomigrate/internal/storage"
)

// Engine handles the backup workflow.
type Engine struct {
	storage    storage.Storage
	serializer Serializer
}

// NewEngine creates a new backup engine.
func NewEngine(s storage.Storage, ser Serializer) *Engine {
	return &Engine{
		storage:    s,
		serializer: ser,
	}
}

// Backup runs the backup process for a given source and table.
func (e *Engine) Backup(ctx context.Context, opID string, src adapter.SourceAdapter, table string, chunkSize int64) (*Manifest, error) {
	startTime := time.Now().UTC()
	
	s, err := src.Schema(ctx, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	partitions, err := src.Partitions(ctx, table, 16) // Default to 16 partitions
	if err != nil {
		return nil, fmt.Errorf("failed to get partitions: %w", err)
	}

	manifest := &Manifest{
		Version:        1,
		OperationID:    opID,
		Source:         SourceMetadata{Type: src.Type(), Table: table},
		CreatedAt:      startTime,
		ChunkSizeBytes: chunkSize,
		SchemaSnapshot: *s,
	}

	// For simplicity, we process partitions sequentially in this initial implementation
	// Real implementation would use the pipeline orchestrator
	var totalRows int64
	for i, p := range partitions {
		recordCh := make(chan *record.Record, 1000)
		errCh := make(chan error, 1)

		go src.ReadPartition(ctx, p, recordCh, errCh)

		chunkIndex := i
		chunkFile := fmt.Sprintf("chunk-%04d.parquet.zst", chunkIndex)
		
		rowCount, checksum, err := e.writeChunk(ctx, chunkFile, recordCh, errCh)
		if err != nil {
			return nil, fmt.Errorf("failed to write chunk %d: %w", chunkIndex, err)
		}

		manifest.Chunks = append(manifest.Chunks, Chunk{
			Index:    chunkIndex,
			File:     chunkFile,
			RowCount: rowCount,
			SHA256:   checksum,
		})
		totalRows += rowCount
	}

	manifest.RowCount = totalRows
	
	// Write manifest
	manifestData, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal manifest: %w", err)
	}

	if err := e.storage.Put(ctx, "manifest.json", bytes.NewReader(manifestData)); err != nil {
		return nil, fmt.Errorf("failed to write manifest: %w", err)
	}

	return manifest, nil
}

func (e *Engine) writeChunk(ctx context.Context, path string, recordCh <-chan *record.Record, errCh <-chan error) (int64, string, error) {
	pr, pw := io.Pipe()
	
	hash := sha256.New()
	mw := io.MultiWriter(pw, hash)

	uploadErrCh := make(chan error, 1)
	go func() {
		defer pr.Close()
		uploadErrCh <- e.storage.Put(ctx, path, pr)
	}()

	comp, err := NewCompressor(mw)
	if err != nil {
		pw.Close()
		return 0, "", err
	}

	var rowCount int64
	var fatalErr error

loop:
	for {
		select {
		case err, ok := <-errCh:
			if ok && err != nil {
				fatalErr = err
				break loop
			}
		case rec, ok := <-recordCh:
			if !ok {
				// Final check on errCh in case error was sent just before close
				select {
				case err := <-errCh:
					if err != nil {
						fatalErr = err
					}
				default:
				}
				break loop
			}
			if err := e.serializer.Serialize(comp, rec); err != nil {
				fatalErr = err
				break loop
			}
			rowCount++
		case <-ctx.Done():
			fatalErr = ctx.Err()
			break loop
		}
	}

	if err := e.serializer.Close(comp); err != nil && fatalErr == nil {
		fatalErr = err
	}
	if err := comp.Close(); err != nil && fatalErr == nil {
		fatalErr = err
	}
	pw.Close()

	if fatalErr != nil {
		return 0, "", fatalErr
	}

	if err := <-uploadErrCh; err != nil {
		return 0, "", err
	}

	return rowCount, hex.EncodeToString(hash.Sum(nil)), nil
}
