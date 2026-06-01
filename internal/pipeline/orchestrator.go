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
	"golang.org/x/sync/errgroup"
)

// Orchestrator coordinates the data migration pipeline.
type Orchestrator struct {
	config     config.ConcurrencyConfig
	checkpoint *checkpoint.Store
	mapper     *migration.SchemaMapper
}

// NewOrchestrator creates a new orchestrator.
func NewOrchestrator(cfg config.ConcurrencyConfig, cp *checkpoint.Store, mapper *migration.SchemaMapper) *Orchestrator {
	return &Orchestrator{
		config:     cfg,
		checkpoint: cp,
		mapper:     mapper,
	}
}

// Migrate runs a migration from source to target.
func (o *Orchestrator) Migrate(ctx context.Context, opID string, src adapter.SourceAdapter, dst adapter.TargetAdapter, table string) error {
	partitions, err := src.Partitions(ctx, table, o.config.NumReaders)
	if err != nil {
		return fmt.Errorf("failed to get partitions: %w", err)
	}

	metrics.PartitionsTotal.WithLabelValues(table).Set(float64(len(partitions)))
	g, ctx := errgroup.WithContext(ctx)

	// recordCh capacity: N * batchSize
	recordCh := make(chan *record.Record, o.config.NumReaders*o.config.BatchSize)
	
	// batchCh capacity: W * 2
	batchCh := make(chan []*record.Record, o.config.NumWriters*2)

	// Start Readers
	var wgReaders sync.WaitGroup
	for _, p := range partitions {
		p := p
		wgReaders.Add(1)
		g.Go(func() error {
			defer wgReaders.Done()
			errCh := make(chan error, 1)
			go src.ReadPartition(ctx, p, recordCh, errCh)
			
			select {
			case err := <-errCh:
				return err
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}

	// Close recordCh when all readers are done
	go func() {
		wgReaders.Wait()
		metrics.PartitionsDone.WithLabelValues(table).Set(float64(len(partitions)))
		close(recordCh)
	}()

	// Start Transformers (simple batching for now)
	var wgTransformers sync.WaitGroup
	for i := 0; i < o.config.NumTransformers; i++ {
		wgTransformers.Add(1)
		g.Go(func() error {
			defer wgTransformers.Done()
			var batch []*record.Record
			ticker := time.NewTicker(o.config.BatchTimeout)
			defer ticker.Stop()

			for {
				select {
				case rec, ok := <-recordCh:
					if !ok {
						if len(batch) > 0 {
							batchCh <- batch
						}
						return nil
					}
					metrics.RecordsRead.WithLabelValues(table).Inc()
					
					// Apply transformation/mapping
					mappedRec := rec
					if o.mapper != nil {
						mappedRec = o.mapper.MapRecord(rec)
					}
					
					batch = append(batch, mappedRec)
					if len(batch) >= o.config.BatchSize {
						batchCh <- batch
						batch = nil
					}
				case <-ticker.C:
					if len(batch) > 0 {
						batchCh <- batch
						batch = nil
					}
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		})
	}

	// Start Writers
	var wgWriters sync.WaitGroup
	for i := 0; i < o.config.NumWriters; i++ {
		wgWriters.Add(1)
		g.Go(func() error {
			defer wgWriters.Done()
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
						return fmt.Errorf("failed to write batch: %w", err)
					}
					metrics.RecordsWritten.WithLabelValues(table).Add(float64(n))
					metrics.BatchWriteDuration.WithLabelValues(table).Observe(time.Since(start).Seconds())

					// Update checkpoints for all partitions in this batch
					progress := make(map[string]*checkpoint.PartitionCheckpoint)
					for _, rec := range batch {
						pid := rec.Metadata.PartitionID
						cp, ok := progress[pid]
						if !ok {
							// Try to load existing checkpoint from store
							existing, err := o.checkpoint.GetPartition(opID, pid)
							if err != nil {
								// If not found, create new one
								cp = &checkpoint.PartitionCheckpoint{
									PartitionID: pid,
									Status:      checkpoint.StatusInProgress,
									UpdatedAt:   time.Now(),
								}
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
							// Log error but don't fail migration for checkpoint failure
							fmt.Printf("Warning: failed to save checkpoint for partition %s: %v\n", cp.PartitionID, err)
						}
					}

				case <-ctx.Done():
					return ctx.Err()
				}
			}
		})
	}

	// Wait for transformers to finish and close batchCh
	go func() {
		wgTransformers.Wait()
		close(batchCh)
	}()

	return g.Wait()
}
