package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// LocalStorage is a storage backend that uses the local filesystem.
type LocalStorage struct {
	baseDir string
}

// NewLocalStorage creates a new LocalStorage rooted at baseDir.
func NewLocalStorage(baseDir string) (*LocalStorage, error) {
	absPath, err := filepath.Abs(baseDir)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path: %w", err)
	}

	if err := os.MkdirAll(absPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	return &LocalStorage{baseDir: absPath}, nil
}

func (s *LocalStorage) fullPath(path string) string {
	return filepath.Join(s.baseDir, path)
}

func (s *LocalStorage) Put(ctx context.Context, path string, reader io.Reader) error {
	fullPath := s.fullPath(path)
	
	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return fmt.Errorf("failed to create parent directory: %w", err)
	}

	f, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer f.Close()

	_, err = io.Copy(f, reader)
	if err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

func (s *LocalStorage) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	fullPath := s.fullPath(path)
	f, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("file not found: %s", path)
		}
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	return f, nil
}

func (s *LocalStorage) List(ctx context.Context, prefix string) ([]string, error) {
	var paths []string
	searchDir := s.fullPath(prefix)
	
	// If searchDir is a file, just return it if it matches
	info, err := os.Stat(searchDir)
	if err == nil && !info.IsDir() {
		return []string{prefix}, nil
	}

	err = filepath.Walk(searchDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			relPath, err := filepath.Rel(s.baseDir, path)
			if err != nil {
				return err
			}
			paths = append(paths, relPath)
		}
		return nil
	})

	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}

	return paths, nil
}

func (s *LocalStorage) Delete(ctx context.Context, path string) error {
	fullPath := s.fullPath(path)
	if err := os.Remove(fullPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to delete file: %w", err)
	}
	return nil
}

func (s *LocalStorage) Exists(ctx context.Context, path string) (bool, error) {
	fullPath := s.fullPath(path)
	_, err := os.Stat(fullPath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("failed to check if file exists: %w", err)
}
