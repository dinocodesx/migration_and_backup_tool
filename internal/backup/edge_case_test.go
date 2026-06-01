package backup

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/dinocodesx/gomigrate/internal/adapter"
	"github.com/dinocodesx/gomigrate/internal/config"
	"github.com/dinocodesx/gomigrate/internal/record"
	"github.com/dinocodesx/gomigrate/internal/schema"
	"go.uber.org/zap"
)

// ── MockSourceAdapter ─────────────────────────────────────────────────────────

// MockSourceAdapter is a test double for adapter.SourceAdapter.
type MockSourceAdapter struct {
	records []*record.Record
	err     error // injected error, sent after all records
}

func (m *MockSourceAdapter) Type() string                                           { return "mock" }
func (m *MockSourceAdapter) Connect(_ context.Context, _ config.DBConfig) error    { return nil }
func (m *MockSourceAdapter) Close() error                                           { return nil }
func (m *MockSourceAdapter) Schema(_ context.Context, table string) (*schema.Schema, error) {
	return &schema.Schema{Name: table}, nil
}
func (m *MockSourceAdapter) Partitions(_ context.Context, table string, _ int) ([]adapter.Partition, error) {
	return []adapter.Partition{{ID: "p1", Table: table}}, nil
}

// ReadPartition sends all records then (optionally) an error, then closes ch.
func (m *MockSourceAdapter) ReadPartition(_ context.Context, _ adapter.Partition, ch chan<- *record.Record) error {
	defer close(ch)
	for _, r := range m.records {
		ch <- r
	}
	return m.err
}

// ── FaultyStorage ─────────────────────────────────────────────────────────────

// FaultyStorage is a storage backend that fails Put() with a configurable error.
type FaultyStorage struct {
	putErr error
}

func (f *FaultyStorage) Put(_ context.Context, _ string, reader io.Reader) error {
	if f.putErr != nil {
		return f.putErr
	}
	_, err := io.Copy(io.Discard, reader)
	return err
}
func (f *FaultyStorage) Get(_ context.Context, _ string) (io.ReadCloser, error) { return nil, nil }
func (f *FaultyStorage) List(_ context.Context, _ string) ([]string, error)     { return nil, nil }
func (f *FaultyStorage) Delete(_ context.Context, _ string) error               { return nil }
func (f *FaultyStorage) Exists(_ context.Context, _ string) (bool, error)       { return false, nil }

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestBackup_SourceError(t *testing.T) {
	src := &MockSourceAdapter{
		err: errors.New("database connection lost"),
	}
	store := &FaultyStorage{}
	engine := NewEngine(store, NewNDJSONSerializer(), zap.NewNop(), 1)

	_, err := engine.Backup(context.Background(), "test-op", src, "users", 0)
	if err == nil {
		t.Fatal("expected error from backup engine when source fails, got nil")
	}
	t.Logf("got expected error: %v", err)
}

func TestBackup_StorageError(t *testing.T) {
	src := &MockSourceAdapter{
		records: []*record.Record{{Data: map[string]any{"id": 1}}},
	}
	store := &FaultyStorage{
		putErr: errors.New("disk full"),
	}
	engine := NewEngine(store, NewNDJSONSerializer(), zap.NewNop(), 1)

	_, err := engine.Backup(context.Background(), "test-op", src, "users", 0)
	if err == nil {
		t.Fatal("expected error from backup engine when storage fails, got nil")
	}
	t.Logf("got expected error: %v", err)
}

func TestBackup_EmptySource(t *testing.T) {
	src := &MockSourceAdapter{} // no records, no error
	store := &FaultyStorage{}
	engine := NewEngine(store, NewNDJSONSerializer(), zap.NewNop(), 1)

	manifest, err := engine.Backup(context.Background(), "test-op-empty", src, "users", 0)
	if err != nil {
		t.Fatalf("unexpected error for empty source: %v", err)
	}
	if manifest.RowCount != 0 {
		t.Errorf("expected 0 rows, got %d", manifest.RowCount)
	}
}
