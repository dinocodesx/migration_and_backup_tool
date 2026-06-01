package checkpoint

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStore(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "checkpoint_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.bolt")
	store, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	// Test SaveMeta
	meta := OperationMeta{
		OperationID: "op-123",
		StartTime:   time.Now().UTC(),
		ConfigHash:  "hash-abc",
	}
	if err := store.SaveMeta(meta); err != nil {
		t.Errorf("failed to save meta: %v", err)
	}

	// Test SavePartition
	cp := PartitionCheckpoint{
		PartitionID:   "p-1",
		Status:        StatusInProgress,
		LastCommitted: int64(100),
		RowsWritten:   100,
		UpdatedAt:     time.Now().UTC(),
	}
	if err := store.SavePartition(meta.OperationID, cp); err != nil {
		t.Errorf("failed to save partition: %v", err)
	}

	// Test GetPartition
	got, err := store.GetPartition(meta.OperationID, cp.PartitionID)
	if err != nil {
		t.Fatalf("failed to get partition: %v", err)
	}

	if got.PartitionID != cp.PartitionID {
		t.Errorf("expected PartitionID %s, got %s", cp.PartitionID, got.PartitionID)
	}
	// Note: bbolt might truncate time precision, so we might need to compare Unix timestamps
	if got.LastCommitted.(float64) != float64(cp.LastCommitted.(int64)) {
		t.Errorf("expected LastCommitted %v, got %v", cp.LastCommitted, got.LastCommitted)
	}
}
