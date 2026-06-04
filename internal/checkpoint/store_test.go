package checkpoint

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStore_SaveAndGetPartition(t *testing.T) {
	store, cleanup := newTestStore(t)
	defer cleanup()

	meta := OperationMeta{
		OperationID: "op-123",
		StartTime:   time.Now().UTC(),
		ConfigHash:  "hash-abc",
	}
	if err := store.SaveMeta(meta); err != nil {
		t.Fatalf("SaveMeta: %v", err)
	}

	cp := PartitionCheckpoint{
		PartitionID:   "p-1",
		Status:        StatusInProgress,
		LastCommitted: int64(100),
		RowsWritten:   100,
		UpdatedAt:     time.Now().UTC(),
	}
	if err := store.SavePartition(meta.OperationID, cp); err != nil {
		t.Fatalf("SavePartition: %v", err)
	}

	got, err := store.GetPartition(meta.OperationID, cp.PartitionID)
	if err != nil {
		t.Fatalf("GetPartition: %v", err)
	}
	if got.PartitionID != cp.PartitionID {
		t.Errorf("PartitionID: want %s, got %s", cp.PartitionID, got.PartitionID)
	}
}

func TestStore_ListPartitions(t *testing.T) {
	store, cleanup := newTestStore(t)
	defer cleanup()

	opID := "op-list"
	for i := range 5 {
		cp := PartitionCheckpoint{
			PartitionID: string(rune('a' + i)),
			Status:      StatusPending,
			UpdatedAt:   time.Now().UTC(),
		}
		if err := store.SavePartition(opID, cp); err != nil {
			t.Fatalf("SavePartition[%d]: %v", i, err)
		}
	}

	// Save one partition for a different operation — must not appear in list.
	other := PartitionCheckpoint{PartitionID: "x", Status: StatusPending, UpdatedAt: time.Now().UTC()}
	if err := store.SavePartition("other-op", other); err != nil {
		t.Fatalf("SavePartition other: %v", err)
	}

	partitions, err := store.ListPartitions(opID)
	if err != nil {
		t.Fatalf("ListPartitions: %v", err)
	}
	if len(partitions) != 5 {
		t.Errorf("ListPartitions: want 5, got %d", len(partitions))
	}
}

func TestStore_SaveStatus(t *testing.T) {
	store, cleanup := newTestStore(t)
	defer cleanup()

	opID := "op-status"
	cp := PartitionCheckpoint{PartitionID: "p1", Status: StatusPending, UpdatedAt: time.Now().UTC()}
	if err := store.SavePartition(opID, cp); err != nil {
		t.Fatalf("SavePartition: %v", err)
	}

	if err := store.SaveStatus(opID, "p1", StatusDone); err != nil {
		t.Fatalf("SaveStatus: %v", err)
	}

	got, err := store.GetPartition(opID, "p1")
	if err != nil {
		t.Fatalf("GetPartition: %v", err)
	}
	if got.Status != StatusDone {
		t.Errorf("Status: want Done, got %s", got.Status)
	}
}

func TestStore_GetPartitionNotFound(t *testing.T) {
	store, cleanup := newTestStore(t)
	defer cleanup()

	_, err := store.GetPartition("no-op", "no-partition")
	if !errors.Is(err, ErrCheckpointNotFound) {
		t.Errorf("want ErrCheckpointNotFound, got %v", err)
	}
}

func TestStore_DeleteOperation(t *testing.T) {
	store, cleanup := newTestStore(t)
	defer cleanup()

	opID := "op-del"
	meta := OperationMeta{OperationID: opID, StartTime: time.Now().UTC()}
	_ = store.SaveMeta(meta)
	_ = store.SavePartition(opID, PartitionCheckpoint{PartitionID: "p1", Status: StatusDone, UpdatedAt: time.Now().UTC()})

	if err := store.DeleteOperation(opID); err != nil {
		t.Fatalf("DeleteOperation: %v", err)
	}

	partitions, err := store.ListPartitions(opID)
	if err != nil {
		t.Fatalf("ListPartitions after delete: %v", err)
	}
	if len(partitions) != 0 {
		t.Errorf("expected 0 partitions after delete, got %d", len(partitions))
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func newTestStore(t *testing.T) (*Store, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "checkpoint_test_*")
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(filepath.Join(dir, "test.bolt"))
	if err != nil {
		_ = os.RemoveAll(dir)
		t.Fatalf("NewStore: %v", err)
	}
	return store, func() {
		_ = store.Close()
		_ = os.RemoveAll(dir)
	}
}
