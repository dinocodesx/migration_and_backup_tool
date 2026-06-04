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

// MockSourceAdapter is a test double for simulating various database behaviors.
type MockSourceAdapter struct {
	records []*record.Record
	err     error // Optional error to return after processing records.
}

func (m *MockSourceAdapter) Type() string                                       { return "mock" }
func (m *MockSourceAdapter) Connect(_ context.Context, _ config.DBConfig) error { return nil }
func (m *MockSourceAdapter) Close() error                                       { return nil }
func (m *MockSourceAdapter) Schema(_ context.Context, table string) (*schema.Schema, error) {
	return &schema.Schema{Name: table}, nil
}
func (m *MockSourceAdapter) Partitions(_ context.Context, table string, _ int) ([]adapter.Partition, error) {
	return []adapter.Partition{{ID: "p1", Table: table}}, nil
}

// ReadPartition simulates a database read by pushing records into the channel.
func (m *MockSourceAdapter) ReadPartition(_ context.Context, _ adapter.Partition, ch chan<- *record.Record) error {
	defer close(ch)
	for _, r := range m.records {
		ch <- r
	}
	return m.err
}

// FaultyStorage simulates storage failures (e.g., S3 outage or Disk Full).
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

// TestBackup_SourceError verifies that the engine correctly propagates errors from the database reader.
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

// TestBackup_StorageError verifies that the engine handles write failures (e.g., S3 500s or Disk Full).
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

// TestBackup_EmptySource ensures the engine handles empty tables gracefully.
func TestBackup_EmptySource(t *testing.T) {
	src := &MockSourceAdapter{} // No records.
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
