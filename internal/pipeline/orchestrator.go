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
	"github.com/dinocodesx/gomigrate/internal/metrics"
	"github.com/dinocodesx/gomigrate/internal/migration"
	"github.com/dinocodesx/gomigrate/internal/record"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

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
	// logger provides structured observability for the orchestration process.
	logger *zap.Logger
}

// NewOrchestrator initializes a new Orchestrator with the specified dependencies.
func NewOrchestrator(cfg config.ConcurrencyConfig, cp *checkpoint.Store, mapper *migration.SchemaMapper, logger *zap.Logger) *Orchestrator {
	return &Orchestrator{config: cfg, checkpoint: cp, mapper: mapper, logger: logger}
}

// Migrate executes the full data pipeline for a specific table. It handles
// partition discovery, initializes the multi-stage concurrent pipeline,
// and blocks until all data is processed or a fatal error occurs.
func (o *Orchestrator) Migrate(ctx context.Context, opID string, src adapter.SourceAdapter, dst adapter.TargetAdapter, table string) error {
	partitions, err := src.Partitions(ctx, table, o.config.NumReaders)
	if err != nil {
		return fmt.Errorf("failed to get partitions for table %q: %w", table, err)
	}
	if len(partitions) == 0 {
		o.logger.Info("table is empty — nothing to migrate", zap.String("table", table))
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

	return g.Wait()
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
func (o *Orchestrator) runReader(ctx context.Context, src adapter.SourceAdapter, p adapter.Partition, recordCh chan<- *record.Record) error {
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
			return ctx.Err()
		}
	}
	return <-readErrCh
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
	ticker := time.NewTicker(o.config.BatchTimeout)
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
				metrics.RecordsFailed.WithLabelValues(table).Add(float64(len(batch)))
				return fmt.Errorf("write batch failed: %w", err)
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

// saveCheckpoints persists the progress of all partitions represented in a batch.
func (o *Orchestrator) saveCheckpoints(ctx context.Context, opID string, batch []*record.Record) {
	progress := make(map[string]*checkpoint.PartitionCheckpoint)

	for _, rec := range batch {
		pid := rec.Metadata.PartitionID
		cp, exists := progress[pid]
		if !exists {
			existing, err := o.checkpoint.GetPartition(opID, pid)
			if err != nil {
				cp = &checkpoint.PartitionCheckpoint{PartitionID: pid, Status: checkpoint.StatusInProgress, UpdatedAt: time.Now()}
			} else {
				cp = existing
				cp.Status = checkpoint.StatusInProgress
				cp.UpdatedAt = time.Now()
			}
			progress[pid] = cp
		}
		cp.LastCommitted = rec.Metadata.Offset
		cp.RowsWritten++
	}

	for _, cp := range progress {
		if err := o.checkpoint.SavePartition(opID, *cp); err != nil {
			o.logger.Warn("failed to save checkpoint", zap.String("operation_id", opID), zap.String("partition_id", cp.PartitionID), zap.Error(err))
		}
	}
}
