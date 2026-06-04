package storage

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
)

// TestLocalStorage verifies the end-to-end functionality of the local filesystem
// storage backend, including file creation, existence checks, retrieval,
// directory listing, and deletion.
func TestLocalStorage(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	s, err := NewLocalStorage(tempDir)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	ctx := context.Background()
	path := "test/file.txt"
	content := []byte("hello world")

	// Verify file creation.
	if err := s.Put(ctx, path, bytes.NewReader(content)); err != nil {
		t.Fatalf("failed to put file: %v", err)
	}

	// Verify existence check.
	exists, err := s.Exists(ctx, path)
	if err != nil {
		t.Fatalf("failed to check existence: %v", err)
	}
	if !exists {
		t.Errorf("expected file to exist")
	}

	// Verify data retrieval.
	reader, err := s.Get(ctx, path)
	if err != nil {
		t.Fatalf("failed to get file: %v", err)
	}
	defer reader.Close()

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("expected content %s, got %s", content, got)
	}

	// Verify directory listing.
	paths, err := s.List(ctx, "test")
	if err != nil {
		t.Fatalf("failed to list: %v", err)
	}
	if len(paths) != 1 || paths[0] != path {
		t.Errorf("expected paths [%s], got %v", path, paths)
	}

	// Verify file deletion.
	if err := s.Delete(ctx, path); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}
	exists, _ = s.Exists(ctx, path)
	if exists {
		t.Errorf("expected file to be deleted")
	}
}
