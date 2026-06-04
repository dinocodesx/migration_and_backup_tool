package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dinocodesx/gomigrate/internal/adapter"
	"github.com/dinocodesx/gomigrate/internal/checkpoint"
	"github.com/dinocodesx/gomigrate/internal/config"
	"github.com/dinocodesx/gomigrate/internal/pipeline"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestResumability(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()
	cpPath := "test_resume.bolt"
	defer os.Remove(cpPath)

	store, err := checkpoint.NewStore(cpPath)
	assert.NoError(t, err)
	defer store.Close()

	totalRecords := 1000
	failAt := 500
	mockSrc := NewMockAdapter(totalRecords)
	mockDst := NewMockAdapter(0)

	// Wrap source with fault injector
	faultSrc := adapter.NewFaultSourceAdapter(mockSrc, adapter.FaultConfig{
		FailAfterRecords: int64(failAt),
		Error:            fmt.Errorf("simulated failure"),
	})

	cfg := config.ConcurrencyConfig{
		NumReaders:      1,
		NumTransformers: 1,
		NumWriters:      1,
		BatchSize:       10,
		BatchTimeout:    1 * time.Second,
		FlushEveryNBatches: 1,
	}

	orch := pipeline.NewOrchestrator(cfg, store, nil, nil, logger)
	opID := "test-op"

	// First run - should fail
	err = orch.Migrate(ctx, opID, faultSrc, mockDst, "test")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "simulated failure")

	// Verify partial progress
	writtenFirstRun := mockDst.written.Load()
	assert.True(t, writtenFirstRun >= 400 && writtenFirstRun <= 600, "Should have written some records")

	// Second run - resume
	// Use the original mock source (without fault)
	err = orch.Migrate(ctx, opID, mockSrc, mockDst, "test")
	assert.NoError(t, err)

	// Verify total records written
	assert.Equal(t, int64(totalRecords), mockDst.written.Load())
}
