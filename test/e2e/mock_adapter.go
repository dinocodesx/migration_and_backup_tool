package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"

	"github.com/dinocodesx/gomigrate/internal/adapter"
	"github.com/dinocodesx/gomigrate/internal/config"
	"github.com/dinocodesx/gomigrate/internal/record"
	"github.com/dinocodesx/gomigrate/internal/schema"
)

type MockAdapter struct {
	records []*record.Record
	written atomic.Int64
}

func NewMockAdapter(count int) *MockAdapter {
	recs := make([]*record.Record, count)
	for i := 0; i < count; i++ {
		recs[i] = &record.Record{
			ID:   fmt.Sprintf("%d", i),
			Data: map[string]any{"val": i},
			Metadata: record.RecordMetadata{
				Offset:         int64(i),
				PartitionID:    "p0",
				SourceTable:    "test",
			},
		}
	}
	return &MockAdapter{records: recs}
}

func (a *MockAdapter) Type() string { return "mock" }
func (a *MockAdapter) Connect(ctx context.Context, cfg config.DBConfig) error { return nil }
func (a *MockAdapter) Close() error { return nil }

func (a *MockAdapter) Partitions(ctx context.Context, table string, n int) ([]adapter.Partition, error) {
	return []adapter.Partition{{ID: "p0", Table: "test", Start: int64(0), End: int64(len(a.records))}}, nil
}

func (a *MockAdapter) ReadPartition(ctx context.Context, p adapter.Partition, ch chan<- *record.Record) error {
	start := int64(0)
	if p.Start != nil {
		switch v := p.Start.(type) {
		case int64:
			start = v + 1
		case int:
			start = int64(v) + 1
		case json.Number:
			n, _ := v.Int64()
			start = n + 1
		case float64:
			start = int64(v) + 1
		}
	}
	for i := start; i < int64(len(a.records)); i++ {
		select {
		case ch <- a.records[i]:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	close(ch)
	return nil
}

func (a *MockAdapter) Schema(ctx context.Context, table string) (*schema.Schema, error) {
	return &schema.Schema{Name: "test"}, nil
}

func (a *MockAdapter) WriteBatch(ctx context.Context, batch []*record.Record) (int, error) {
	a.written.Add(int64(len(batch)))
	return len(batch), nil
}

func (a *MockAdapter) ApplySchema(ctx context.Context, s *schema.Schema) error { return nil }
