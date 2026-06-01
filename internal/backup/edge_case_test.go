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
)

// MockSourceAdapter for edge case testing
type MockSourceAdapter struct {
	records []*record.Record
	err     error
}

func (m *MockSourceAdapter) Type() string                                           { return "mock" }
func (m *MockSourceAdapter) Connect(ctx context.Context, cfg config.DBConfig) error { return nil }
func (m *MockSourceAdapter) Close() error                                           { return nil }
func (m *MockSourceAdapter) Schema(ctx context.Context, table string) (*schema.Schema, error) {
	return &schema.Schema{Name: table}, nil
}
func (m *MockSourceAdapter) Partitions(ctx context.Context, table string, n int) ([]adapter.Partition, error) {
	return []adapter.Partition{{ID: "p1"}}, nil
}
func (m *MockSourceAdapter) ReadPartition(ctx context.Context, p adapter.Partition, ch chan<- *record.Record, errCh chan<- error) {
	defer close(ch)
	for _, r := range m.records {
		ch <- r
	}
	if m.err != nil {
		errCh <- m.err
	}
}

// MockStorage with fault injection
type FaultyStorage struct {
	putErr error
}

func (f *FaultyStorage) Put(ctx context.Context, path string, reader io.Reader) error {
	if f.putErr != nil {
		return f.putErr
	}
	_, err := io.Copy(io.Discard, reader)
	return err
}
func (f *FaultyStorage) Get(ctx context.Context, path string) (io.ReadCloser, error) { return nil, nil }
func (f *FaultyStorage) List(ctx context.Context, prefix string) ([]string, error)   { return nil, nil }
func (f *FaultyStorage) Delete(ctx context.Context, path string) error               { return nil }
func (f *FaultyStorage) Exists(ctx context.Context, path string) (bool, error)       { return false, nil }

func TestBackup_SourceError(t *testing.T) {
	src := &MockSourceAdapter{
		err: errors.New("database connection lost"),
	}
	store := &FaultyStorage{}
	engine := NewEngine(store, NewNDJSONSerializer())

	_, err := engine.Backup(context.Background(), "test-op", src, "users", 1024)
	if err == nil {
		t.Fatal("expected error from backup engine when source fails, got nil")
	}
}

func TestBackup_StorageError(t *testing.T) {
	src := &MockSourceAdapter{
		records: []*record.Record{{Data: map[string]any{"id": 1}}},
	}
	store := &FaultyStorage{
		putErr: errors.New("disk full"),
	}
	engine := NewEngine(store, NewNDJSONSerializer())

	_, err := engine.Backup(context.Background(), "test-op", src, "users", 1024)
	if err == nil {
		t.Fatal("expected error from backup engine when storage fails, got nil")
	}
}
