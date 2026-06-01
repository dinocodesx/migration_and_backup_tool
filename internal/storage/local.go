package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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

func (s *LocalStorage) fullPath(path string) (string, error) {
	fp := filepath.Join(s.baseDir, filepath.FromSlash(path))
	rel, err := filepath.Rel(s.baseDir, fp)
	if err != nil || strings.HasPrefix(rel, "..") || rel == ".." {
		return "", fmt.Errorf("invalid path: %s", path)
	}
	return fp, nil
}

func (s *LocalStorage) Put(ctx context.Context, path string, reader io.Reader) (err error) {
	fullPath, err := s.fullPath(path)
	if err != nil {
		return err
	}
	
	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return fmt.Errorf("failed to create parent directory: %w", err)
	}

	f, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	
	defer func() {
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
	}()

	_, err = io.Copy(f, reader)
	if err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	if err = f.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	return nil
}

func (s *LocalStorage) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	fullPath, err := s.fullPath(path)
	if err != nil {
		return nil, err
	}
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
	fullPrefix, err := s.fullPath(prefix)
	if err != nil {
		return nil, err
	}
	
	var paths []string
	
	// If fullPrefix is a file, just return it if it matches
	info, err := os.Stat(fullPrefix)
	if err == nil && !info.IsDir() {
		return []string{prefix}, nil
	}

	err = filepath.Walk(fullPrefix, func(path string, info os.FileInfo, err error) error {
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
	fullPath, err := s.fullPath(path)
	if err != nil {
		return err
	}
	if err := os.Remove(fullPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to delete file: %w", err)
	}
	return nil
}

func (s *LocalStorage) Exists(ctx context.Context, path string) (bool, error) {
	fullPath, err := s.fullPath(path)
	if err != nil {
		return false, err
	}
	_, err = os.Stat(fullPath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("failed to check if file exists: %w", err)
}
