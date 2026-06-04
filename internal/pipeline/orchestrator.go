// Package pipeline implements the core execution model for data movement.
// It uses a highly concurrent architecture based on the producer-consumer
// pattern, leveraging Go's channels and goroutines for maximum throughput.
package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dinocodesx/gomigrate/internal/adapter"
	"github.com/dinocodesx/gomigrate/internal/checkpoint"
	"github.com/dinocodesx/gomigrate/internal/config"
	"github.com/dinocodesx/gomigrate/internal/errs"
	"github.com/dinocodesx/gomigrate/internal/metrics"
	"github.com/dinocodesx/gomigrate/internal/migration"
	"github.com/dinocodesx/gomigrate/internal/record"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const tracerName = "github.com/dinocodesx/gomigrate/pipeline"

// Orchestrator manages the end-to-end execution of a migration job. It
// coordinates the lifecycle of readers, transformers, and writers, and
// ensures that progress is consistently persisted to the checkpoint store.
type Orchestrator struct {
	// config defines the concurrency and batching parameters for the pipeline.
	config config.ConcurrencyConfig
	// checkpoint is the persistent store for tracking migration progress.
	checkpoint *checkpoint.Store
	// mapper handles record transformation between source and target engines.
	mapper *migration.SchemaMapper
	// dlq handles failed records.
	dlq *errs.DLQ
	// logger provides structured observability for the orchestration process.
	logger *zap.Logger
}

// NewOrchestrator initializes a new Orchestrator with the specified dependencies.
func NewOrchestrator(cfg config.ConcurrencyConfig, cp *checkpoint.Store, mapper *migration.SchemaMapper, dlq *errs.DLQ, logger *zap.Logger) *Orchestrator {
	return &Orchestrator{config: cfg, checkpoint: cp, mapper: mapper, dlq: dlq, logger: logger}
}

// Migrate executes the full data pipeline for a specific table. It handles
// partition discovery, initializes the multi-stage concurrent pipeline,
// and blocks until all data is processed or a fatal error occurs.
func (o *Orchestrator) Migrate(ctx context.Context, opID string, src adapter.SourceAdapter, dst adapter.TargetAdapter, table string) error {
	// Start a root OTel span covering the whole migration for this table.
	tracer := otel.Tracer(tracerName)
	ctx, rootSpan := tracer.Start(ctx, "migrate",
		trace.WithAttributes(
			attribute.String("operation_id", opID),
			attribute.String("table", table),
			attribute.String("source", src.Type()),
			attribute.String("target", dst.Type()),
		),
	)
	defer func() { rootSpan.End() }()

	allPartitions, err := src.Partitions(ctx, table, o.config.NumReaders)
	if err != nil {
		rootSpan.RecordError(err)
		rootSpan.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("failed to get partitions for table %q: %w", table, err)
	}

	// Filter out completed partitions if resuming
	var partitions []adapter.Partition
	checkpoints, _ := o.checkpoint.ListPartitions(opID)
	donePartitions := make(map[string]bool)
	for _, cp := range checkpoints {
		if cp.Status == checkpoint.StatusDone {
			donePartitions[cp.PartitionID] = true
		}
	}

	for _, p := range allPartitions {
		if !donePartitions[p.ID] {
			// If we have a checkpoint, adjust the partition start
			for _, cp := range checkpoints {
				if cp.PartitionID == p.ID && cp.LastCommitted != nil {
					p.Start = cp.LastCommitted
					o.logger.Info("resuming partition", zap.String("partition_id", p.ID), zap.Any("from", p.Start))
					break
				}
			}
			partitions = append(partitions, p)
		}
	}

	if len(partitions) == 0 {
		o.logger.Info("table is empty or already fully migrated", zap.String("table", table))
		return nil
	}

	metrics.PartitionsTotal.WithLabelValues(table).Set(float64(len(partitions)))
	o.logger.Info("starting migration", zap.String("operation_id", opID), zap.String("table", table), zap.Int("partitions", len(partitions)))

	g, gctx := errgroup.WithContext(ctx)

	recordCh := make(chan *record.Record, o.config.NumReaders*o.config.BatchSize)
	batchCh := make(chan []*record.Record, o.config.NumWriters*2)

	o.startReaders(gctx, g, src, partitions, table, recordCh)
	o.startTransformers(gctx, g, table, recordCh, batchCh)
	o.startWriters(gctx, g, opID, table, dst, batchCh)

	if err := g.Wait(); err != nil {
		rootSpan.RecordError(err)
		rootSpan.SetStatus(codes.Error, err.Error())
		return err
	}
	rootSpan.SetStatus(codes.Ok, "migration complete")
	return nil
}

// startReaders initializes the extraction stage of the pipeline.
func (o *Orchestrator) startReaders(ctx context.Context, g *errgroup.Group, src adapter.SourceAdapter, partitions []adapter.Partition, table string, recordCh chan<- *record.Record) {
	var wg sync.WaitGroup
	for _, p := range partitions {
		p := p
		wg.Add(1)
		g.Go(func() error {
			defer wg.Done()
			return o.runReader(ctx, src, p, recordCh)
		})
	}
	go func() {
		wg.Wait()
		close(recordCh)
		metrics.PartitionsDone.WithLabelValues(table).Set(float64(len(partitions)))
	}()
}

// runReader extracts records from a specific partition and streams them to the central channel.
// It creates a child OTel span for the partition to enable per-partition tracing.
func (o *Orchestrator) runReader(ctx context.Context, src adapter.SourceAdapter, p adapter.Partition, recordCh chan<- *record.Record) error {
	// Create a child span for this specific partition.
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(ctx, "partition.read",
		trace.WithAttributes(
			attribute.String("partition_id", p.ID),
			attribute.String("table", p.Table),
		),
	)
	defer span.End()

	partCh := make(chan *record.Record, o.config.BatchSize)
	readErrCh := make(chan error, 1)

	go func() {
		readErrCh <- src.ReadPartition(ctx, p, partCh)
	}()

	for rec := range partCh {
		select {
		case recordCh <- rec:
		case <-ctx.Done():
			for range partCh {
			}
			span.RecordError(ctx.Err())
			span.SetStatus(codes.Error, ctx.Err().Error())
			return ctx.Err()
		}
	}
	if err := <-readErrCh; err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.SetStatus(codes.Ok, "partition read complete")
	return nil
}

// startTransformers initializes the transformation stage of the pipeline.
func (o *Orchestrator) startTransformers(ctx context.Context, g *errgroup.Group, table string, recordCh <-chan *record.Record, batchCh chan<- []*record.Record) {
	var wg sync.WaitGroup
	for i := 0; i < o.config.NumTransformers; i++ {
		wg.Add(1)
		g.Go(func() error {
			defer wg.Done()
			return o.runTransformer(ctx, table, recordCh, batchCh)
		})
	}
	go func() {
		wg.Wait()
		close(batchCh)
	}()
}

// runTransformer consumes individual records, applies transformations, and groups them into batches.
func (o *Orchestrator) runTransformer(ctx context.Context, table string, recordCh <-chan *record.Record, batchCh chan<- []*record.Record) error {
	var batch []*record.Record
	timeout := o.config.BatchTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ticker := time.NewTicker(timeout)
	defer ticker.Stop()

	flush := func() {
		if len(batch) > 0 {
			batchCh <- batch
			batch = nil
		}
	}

	for {
		select {
		case rec, ok := <-recordCh:
			if !ok {
				flush()
				return nil
			}
			metrics.RecordsRead.WithLabelValues(table).Inc()

			mappedRec := rec
			if o.mapper != nil {
				mappedRec = o.mapper.MapRecord(rec)
			}

			batch = append(batch, mappedRec)
			if len(batch) >= o.config.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// startWriters initializes the ingestion stage of the pipeline.
func (o *Orchestrator) startWriters(ctx context.Context, g *errgroup.Group, opID, table string, dst adapter.TargetAdapter, batchCh <-chan []*record.Record) {
	for i := 0; i < o.config.NumWriters; i++ {
		g.Go(func() error {
			return o.runWriter(ctx, opID, table, dst, batchCh)
		})
	}
}

// runWriter consumes batches and performs bulk ingestion into the target database.
func (o *Orchestrator) runWriter(ctx context.Context, opID, table string, dst adapter.TargetAdapter, batchCh <-chan []*record.Record) error {
	batchCount := 0
	flushEvery := o.config.FlushEveryNBatches
	if flushEvery <= 0 {
		flushEvery = 10
	}

	for {
		select {
		case batch, ok := <-batchCh:
			if !ok {
				return nil
			}

			start := time.Now()
			n, err := dst.WriteBatch(ctx, batch)
			if err != nil {
				o.logger.Warn("batch write failed, attempting individual records", zap.String("table", table), zap.Error(err))
				o.handleWriteFailure(ctx, table, dst, batch)
				// Even if some failed, we count others as done for checkpointing if possible
				// But simpler is to checkpoint only on full batch success
				continue
			}

			metrics.RecordsWritten.WithLabelValues(table).Add(float64(n))
			metrics.BatchWriteDuration.WithLabelValues(table).Observe(time.Since(start).Seconds())

			batchCount++
			if batchCount%flushEvery == 0 {
				o.saveCheckpoints(ctx, opID, batch)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// handleWriteFailure isolates bad records and sends them to the DLQ.
func (o *Orchestrator) handleWriteFailure(ctx context.Context, table string, dst adapter.TargetAdapter, batch []*record.Record) {
	for _, rec := range batch {
		// Attempt individual write (with small retry)
		_, err := dst.WriteBatch(ctx, []*record.Record{rec})
		if err != nil {
			metrics.RecordsFailed.WithLabelValues(table).Inc()
			if o.dlq != nil {
				if dlqErr := o.dlq.Write(rec, err, 1); dlqErr != nil {
					o.logger.Error("failed to write to DLQ", zap.Error(dlqErr))
				}
			}
		} else {
			metrics.RecordsWritten.WithLabelValues(table).Inc()
		}
	}
}

// saveCheckpoints persists the progress of all partitions represented in a batch.
func (o *Orchestrator) saveCheckpoints(ctx context.Context, opID string, batch []*record.Record) {
	// Group records by partition to find the high-water mark for each
	lastCommitted := make(map[string]any)
	counts := make(map[string]int64)

	for _, rec := range batch {
		pid := rec.Metadata.PartitionID
		lastCommitted[pid] = rec.Metadata.Offset
		counts[pid]++
	}

	for pid, offset := range lastCommitted {
		cp, err := o.checkpoint.GetPartition(opID, pid)
		if err != nil {
			cp = &checkpoint.PartitionCheckpoint{
				PartitionID: pid,
				Status:      checkpoint.StatusInProgress,
				UpdatedAt:   time.Now(),
			}
		}
		cp.LastCommitted = offset
		cp.RowsWritten += counts[pid]
		cp.UpdatedAt = time.Now()

		if err := o.checkpoint.SavePartition(opID, *cp); err != nil {
			o.logger.Warn("failed to save checkpoint", zap.String("operation_id", opID), zap.String("partition_id", pid), zap.Error(err))
		}
	}
}
