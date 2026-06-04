package adapter

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/dinocodesx/gomigrate/internal/record"
)

// FaultConfig defines when and how a fault should be injected.
type FaultConfig struct {
	// FailAfterRecords triggers a failure after processing this many records.
	FailAfterRecords int64
	// FailAfterBatches triggers a failure after processing this many batches (Target only).
	FailAfterBatches int64
	// Error is the error to return when the fault is triggered.
	Error error
}

// FaultSourceAdapter wraps a SourceAdapter to inject failures for testing.
type FaultSourceAdapter struct {
	SourceAdapter
	config       FaultConfig
	recordsCount atomic.Int64
}

func NewFaultSourceAdapter(inner SourceAdapter, cfg FaultConfig) *FaultSourceAdapter {
	return &FaultSourceAdapter{SourceAdapter: inner, config: cfg}
}

func (a *FaultSourceAdapter) ReadPartition(ctx context.Context, p Partition, ch chan<- *record.Record) error {
	wrappedCh := make(chan *record.Record)
	var err error
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		err = a.SourceAdapter.ReadPartition(ctx, p, wrappedCh)
	}()

	for rec := range wrappedCh {
		if a.config.FailAfterRecords > 0 && a.recordsCount.Add(1) >= a.config.FailAfterRecords {
			// Drain wrappedCh in background to avoid leaking SourceAdapter goroutine if it doesn't respect ctx
			go func() {
				for range wrappedCh {
				}
			}()
			return a.config.Error
		}
		select {
		case ch <- rec:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	wg.Wait()
	return err
}

// FaultTargetAdapter wraps a TargetAdapter to inject failures for testing.
type FaultTargetAdapter struct {
	TargetAdapter
	config       FaultConfig
	recordsCount atomic.Int64
	batchesCount atomic.Int64
}

func NewFaultTargetAdapter(inner TargetAdapter, cfg FaultConfig) *FaultTargetAdapter {
	return &FaultTargetAdapter{TargetAdapter: inner, config: cfg}
}

func (a *FaultTargetAdapter) WriteBatch(ctx context.Context, batch []*record.Record) (int, error) {
	if a.config.FailAfterBatches > 0 && a.batchesCount.Add(1) >= a.config.FailAfterBatches {
		return 0, a.config.Error
	}

	if a.config.FailAfterRecords > 0 {
		current := a.recordsCount.Load()
		if current >= a.config.FailAfterRecords {
			return 0, a.config.Error
		}
		if current+int64(len(batch)) >= a.config.FailAfterRecords {
			// Partial success simulation could be complex, for now we just fail the batch
			return 0, a.config.Error
		}
		a.recordsCount.Add(int64(len(batch)))
	}

	return a.TargetAdapter.WriteBatch(ctx, batch)
}
