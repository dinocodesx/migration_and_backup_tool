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

// Engine handles the backup workflow.
type Engine struct {
	storage    storage.Storage
	serializer Serializer
	logger     *zap.Logger
	numReaders int
}

// NewEngine creates a new backup Engine.
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

// Backup runs a full backup of table from src to storage.
func (e *Engine) Backup(ctx context.Context, opID string, src adapter.SourceAdapter, table string, chunkSize int64) (*Manifest, error) {
	if chunkSize <= 0 {
		chunkSize = 512 * 1024 * 1024 // 512 MiB default
	}

	s, err := src.Schema(ctx, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema for table %q: %w", table, err)
	}

	partitions, err := src.Partitions(ctx, table, e.numReaders)
	if err != nil {
		return nil, fmt.Errorf("failed to get partitions for table %q: %w", table, err)
	}

	manifest := &Manifest{
		Version:        1,
		OperationID:    opID,
		Source:         SourceMetadata{Type: src.Type(), Table: table},
		CreatedAt:      time.Now().UTC(),
		ChunkSizeBytes: chunkSize,
		SchemaSnapshot: *s,
	}

	cm := newChunkManager(e, ctx, opID, chunkSize)
	if err := cm.openChunk(); err != nil {
		return nil, err
	}

	for _, p := range partitions {
		if err := e.backupPartition(ctx, src, p, table, cm); err != nil {
			return nil, err
		}
	}

	if err := cm.flushFinalChunk(); err != nil {
		return nil, err
	}

	manifest.Chunks = cm.chunks
	manifest.RowCount = cm.totalRows

	if err := e.saveManifest(ctx, manifest); err != nil {
		return nil, err
	}

	e.logger.Info("backup complete", zap.String("operation_id", opID), zap.String("table", table), zap.Int64("total_rows", cm.totalRows), zap.Int("chunks", len(manifest.Chunks)))
	return manifest, nil
}

func (e *Engine) backupPartition(ctx context.Context, src adapter.SourceAdapter, p adapter.Partition, table string, cm *chunkManager) error {
	e.logger.Info("backing up partition", zap.String("partition_id", p.ID), zap.String("table", table))

	partCh := make(chan *record.Record, 1000)
	readErrCh := make(chan error, 1)
	go func() {
		readErrCh <- src.ReadPartition(ctx, p, partCh)
	}()

	for rec := range partCh {
		if err := cm.addRecord(rec); err != nil {
			return fmt.Errorf("serialize error in partition %s: %w", p.ID, err)
		}
	}

	if err := <-readErrCh; err != nil {
		return fmt.Errorf("read error in partition %s: %w", p.ID, err)
	}
	return nil
}

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

// ── chunkManager ──────────────────────────────────────────────────────────────

type chunkManager struct {
	engine     *Engine
	ctx        context.Context
	opID       string
	maxSize    int64
	chunkIndex int
	totalRows  int64
	chunks     []Chunk

	buf       *countWriter
	comp      *Compressor
	chunkFile string
	chunkRows int64
}

func newChunkManager(e *Engine, ctx context.Context, opID string, maxSize int64) *chunkManager {
	return &chunkManager{engine: e, ctx: ctx, opID: opID, maxSize: maxSize}
}

func (c *chunkManager) openChunk() error {
	c.chunkFile = fmt.Sprintf("chunk-%04d.parquet.zst", c.chunkIndex)
	c.buf = &countWriter{buf: &bytes.Buffer{}}

	var err error
	c.comp, err = NewCompressor(c.buf)
	if err != nil {
		return fmt.Errorf("failed to create compressor for chunk %d: %w", c.chunkIndex, err)
	}
	if err := c.engine.serializer.Open(c.comp); err != nil {
		return fmt.Errorf("failed to open serializer for chunk %d: %w", c.chunkIndex, err)
	}
	return nil
}

func (c *chunkManager) addRecord(rec *record.Record) error {
	if err := c.engine.serializer.Serialize(rec); err != nil {
		return err
	}
	c.chunkRows++
	c.totalRows++

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

func (c *chunkManager) flushFinalChunk() error {
	if c.chunkRows > 0 {
		return c.flushChunk()
	}
	// No records in final chunk — close cleanly without uploading an empty file.
	_ = c.engine.serializer.Close()
	_ = c.comp.Close()
	return nil
}

func (c *chunkManager) flushChunk() error {
	if err := c.engine.serializer.Close(); err != nil {
		return fmt.Errorf("failed to close serializer for chunk %d: %w", c.chunkIndex, err)
	}
	if err := c.comp.Close(); err != nil {
		return fmt.Errorf("failed to close compressor for chunk %d: %w", c.chunkIndex, err)
	}

	data := c.buf.buf.Bytes()
	h := sha256.New()
	h.Write(data)
	checksum := hex.EncodeToString(h.Sum(nil))

	if err := c.engine.storage.Put(c.ctx, c.chunkFile, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("failed to upload chunk %d: %w", c.chunkIndex, err)
	}

	c.chunks = append(c.chunks, Chunk{
		Index:    c.chunkIndex,
		File:     c.chunkFile,
		RowCount: c.chunkRows,
		SHA256:   checksum,
	})

	c.engine.logger.Info("chunk written", zap.String("operation_id", c.opID), zap.Int("chunk_index", c.chunkIndex), zap.Int64("rows", c.chunkRows), zap.Int("bytes", len(data)), zap.String("sha256", checksum))
	c.chunkIndex++
	return nil
}

// ── countWriter ───────────────────────────────────────────────────────────────

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
