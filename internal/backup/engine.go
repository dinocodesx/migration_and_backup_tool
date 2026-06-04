// Package backup implements the core logic for exporting database data to storage.
// It handles partitioning, concurrent reading, serialization, and chunked uploads.
package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dinocodesx/gomigrate/internal/adapter"
	"github.com/dinocodesx/gomigrate/internal/record"
	"github.com/dinocodesx/gomigrate/internal/storage"
	"go.uber.org/zap"
)

// Engine handles the high-level backup workflow, orchestrating data movement
// from a source database adapter to a storage backend.
type Engine struct {
	storage    storage.Storage // Target storage backend (S3, GCS, Local).
	serializer Serializer      // Serializer for the backup format (Parquet, NDJSON).
	logger     *zap.Logger     // Structured logger for progress and error reporting.
	numReaders int             // Number of concurrent readers to use for partitioning.
}

// NewEngine creates a new backup Engine with the specified components.
// It defaults to 16 readers if a non-positive value is provided.
func NewEngine(s storage.Storage, ser Serializer, logger *zap.Logger, numReaders int) *Engine {
	if numReaders <= 0 {
		numReaders = 16
	}
	return &Engine{
		storage:    s,
		serializer: ser,
		logger:     logger,
		numReaders: numReaders,
	}
}

/* Backup runs a full backup of a specific table from the source to the configured storage.
 * It performs the following steps:
 * 1. Retrieves the table schema for the manifest metadata.
 * 2. Partitions the table into logical chunks for parallel reading.
 * 3. Initializes a manifest to track the backup's progress and metadata.
 * 4. Reads data from each partition and streams it through the chunk manager.
 * 5. Flushes final data and persists the manifest.json roadmap.
 */
func (e *Engine) Backup(ctx context.Context, opID string, src adapter.SourceAdapter, table string, chunkSize int64) (*Manifest, error) {
	// Set default chunk size to 512 MiB if not specified.
	if chunkSize <= 0 {
		chunkSize = 512 * 1024 * 1024
	}

	// 1. Fetch schema snapshot for restoration reproducibility.
	s, err := src.Schema(ctx, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema for table %q: %w", table, err)
	}

	// 2. Split the table into partitions to enable parallel processing.
	partitions, err := src.Partitions(ctx, table, e.numReaders)
	if err != nil {
		return nil, fmt.Errorf("failed to get partitions for table %q: %w", table, err)
	}

	// 3. Initialize the manifest object.
	manifest := &Manifest{
		Version:        1,
		OperationID:    opID,
		Source:         SourceMetadata{Type: src.Type(), Table: table},
		CreatedAt:      time.Now().UTC(),
		ChunkSizeBytes: chunkSize,
		SchemaSnapshot: *s,
	}

	// 4. Setup the chunk manager to handle streaming data to storage chunks.
	cm := newChunkManager(e, ctx, opID, chunkSize)
	if err := cm.openChunk(); err != nil {
		return nil, err
	}

	// 5. Process each partition. Currently sequential at the engine level,
	// but can be parallelized here in future phases.
	for _, p := range partitions {
		if err := e.backupPartition(ctx, src, p, table, cm); err != nil {
			return nil, err
		}
	}

	// 6. Finalize the last chunk if it contains data.
	if err := cm.flushFinalChunk(); err != nil {
		return nil, err
	}

	// 7. Complete the manifest metadata.
	manifest.Chunks = cm.chunks
	manifest.RowCount = cm.totalRows

	// 8. Persist the manifest.json to storage. This is the "root" of the backup.
	if err := e.saveManifest(ctx, manifest); err != nil {
		return nil, err
	}

	e.logger.Info("backup complete",
		zap.String("operation_id", opID),
		zap.String("table", table),
		zap.Int64("total_rows", cm.totalRows),
		zap.Int("chunks", len(manifest.Chunks)),
	)
	return manifest, nil
}

// backupPartition reads records from a single database partition and pipes them
// into the chunk manager. It uses a channel to decouple reading from serialization.
func (e *Engine) backupPartition(ctx context.Context, src adapter.SourceAdapter, p adapter.Partition, table string, cm *chunkManager) error {
	e.logger.Info("backing up partition", zap.String("partition_id", p.ID), zap.String("table", table))

	// Buffer channel to avoid blocking the reader too often.
	partCh := make(chan *record.Record, 1000)
	readErrCh := make(chan error, 1)

	// Start concurrent read.
	go func() {
		readErrCh <- src.ReadPartition(ctx, p, partCh)
	}()

	// Consume records as they arrive and add them to the current chunk.
	for rec := range partCh {
		if err := cm.addRecord(rec); err != nil {
			return fmt.Errorf("serialize error in partition %s: %w", p.ID, err)
		}
	}

	// Wait for reader to finish and check for errors.
	if err := <-readErrCh; err != nil {
		return fmt.Errorf("read error in partition %s: %w", p.ID, err)
	}
	return nil
}

// saveManifest serializes the manifest and uploads it to the storage root.
func (e *Engine) saveManifest(ctx context.Context, manifest *Manifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}
	if err := e.storage.Put(ctx, "manifest.json", bytes.NewReader(data)); err != nil {
		return fmt.Errorf("failed to write manifest: %w", err)
	}
	return nil
}

// chunkManager handles the lifecycle of backup chunks (files).
// It tracks row counts, total size, and handles rotation when a chunk exceeds maxSize.
type chunkManager struct {
	engine     *Engine
	ctx        context.Context
	opID       string
	maxSize    int64   // Threshold for rotating to a new chunk file.
	chunkIndex int     // Sequence number of the current chunk.
	totalRows  int64   // Aggregated rows across all chunks.
	chunks     []Chunk // Metadata for all completed chunks.

	buf       *countWriter // In-memory buffer for the current chunk.
	comp      *Compressor  // Compression wrapper (e.g., Zstd).
	chunkFile string       // Filename of the current chunk.
	chunkRows int64        // Row count for the current chunk.
}

func newChunkManager(e *Engine, ctx context.Context, opID string, maxSize int64) *chunkManager {
	return &chunkManager{engine: e, ctx: ctx, opID: opID, maxSize: maxSize}
}

// openChunk initializes the resources for a new chunk file.
// It sets up the buffer, compressor, and serializer for the next set of records.
func (c *chunkManager) openChunk() error {
	c.chunkFile = fmt.Sprintf("chunk-%04d.parquet.zst", c.chunkIndex)
	c.buf = &countWriter{buf: &bytes.Buffer{}}

	// Initialize compression (Zstd by default).
	var err error
	c.comp, err = NewCompressor(c.buf)
	if err != nil {
		return fmt.Errorf("failed to create compressor for chunk %d: %w", c.chunkIndex, err)
	}

	// Open the serializer on top of the compressor.
	if err := c.engine.serializer.Open(c.comp); err != nil {
		return fmt.Errorf("failed to open serializer for chunk %d: %w", c.chunkIndex, err)
	}
	return nil
}

// addRecord serializes a record into the current chunk and handles rotation logic.
func (c *chunkManager) addRecord(rec *record.Record) error {
	if err := c.engine.serializer.Serialize(rec); err != nil {
		return err
	}
	c.chunkRows++
	c.totalRows++

	// Check if the current chunk's uncompressed size has exceeded the threshold.
	if c.buf.Written() >= c.maxSize {
		if err := c.flushChunk(); err != nil {
			return err
		}
		c.chunkRows = 0
		if err := c.openChunk(); err != nil {
			return err
		}
	}
	return nil
}

// flushFinalChunk closes the current chunk only if it has records.
func (c *chunkManager) flushFinalChunk() error {
	if c.chunkRows > 0 {
		return c.flushChunk()
	}
	// No records in final chunk — close resources cleanly without an upload.
	_ = c.engine.serializer.Close()
	_ = c.comp.Close()
	return nil
}

// flushChunk finalizes the current chunk, computes its SHA256 checksum,
// and uploads it to the storage backend.
func (c *chunkManager) flushChunk() error {
	// 1. Close the serializer and compressor to flush internal buffers.
	if err := c.engine.serializer.Close(); err != nil {
		return fmt.Errorf("failed to close serializer for chunk %d: %w", c.chunkIndex, err)
	}
	if err := c.comp.Close(); err != nil {
		return fmt.Errorf("failed to close compressor for chunk %d: %w", c.chunkIndex, err)
	}

	// 2. Compute SHA256 for integrity verification.
	data := c.buf.buf.Bytes()
	h := sha256.New()
	h.Write(data)
	checksum := hex.EncodeToString(h.Sum(nil))

	// 3. Upload to storage.
	if err := c.engine.storage.Put(c.ctx, c.chunkFile, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("failed to upload chunk %d: %w", c.chunkIndex, err)
	}

	// 4. Record chunk metadata.
	c.chunks = append(c.chunks, Chunk{
		Index:    c.chunkIndex,
		File:     c.chunkFile,
		RowCount: c.chunkRows,
		SHA256:   checksum,
	})

	c.engine.logger.Info("chunk written",
		zap.String("operation_id", c.opID),
		zap.Int("chunk_index", c.chunkIndex),
		zap.Int64("rows", c.chunkRows),
		zap.Int("bytes", len(data)),
		zap.String("sha256", checksum),
	)
	c.chunkIndex++
	return nil
}

// countWriter is a wrapper around bytes.Buffer that tracks the number of bytes written.
// This is used to determine when to rotate chunks.
type countWriter struct {
	buf     *bytes.Buffer
	written int64
}

func (cw *countWriter) Write(p []byte) (int, error) {
	n, err := cw.buf.Write(p)
	cw.written += int64(n)
	return n, err
}

func (cw *countWriter) Written() int64 {
	return cw.written
}
