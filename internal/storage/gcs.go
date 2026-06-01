package storage

import (
	"context"
	"fmt"
	"io"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// GCSStorage is a storage backend that uses Google Cloud Storage.
type GCSStorage struct {
	client *storage.Client
	bucket string
	prefix string
}

// NewGCSStorage creates a new GCSStorage backend.
func NewGCSStorage(ctx context.Context, bucket, prefix string) (*GCSStorage, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	return &GCSStorage{
		client: client,
		bucket: bucket,
		prefix: prefix,
	}, nil
}

func (s *GCSStorage) fullPath(path string) string {
	return s.prefix + path
}

func (s *GCSStorage) Put(ctx context.Context, path string, reader io.Reader) error {
	w := s.client.Bucket(s.bucket).Object(s.fullPath(path)).NewWriter(ctx)
	if _, err := io.Copy(w, reader); err != nil {
		w.Close()
		return fmt.Errorf("failed to write to GCS: %w", err)
	}
	return w.Close()
}

func (s *GCSStorage) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	r, err := s.client.Bucket(s.bucket).Object(s.fullPath(path)).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get from GCS: %w", err)
	}
	return r, nil
}

func (s *GCSStorage) List(ctx context.Context, prefix string) ([]string, error) {
	var paths []string
	fullPrefix := s.fullPath(prefix)

	it := s.client.Bucket(s.bucket).Objects(ctx, &storage.Query{Prefix: fullPrefix})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list GCS objects: %w", err)
		}
		relPath := strings.TrimPrefix(attrs.Name, s.prefix)
		paths = append(paths, relPath)
	}

	return paths, nil
}

func (s *GCSStorage) Delete(ctx context.Context, path string) error {
	if err := s.client.Bucket(s.bucket).Object(s.fullPath(path)).Delete(ctx); err != nil {
		return fmt.Errorf("failed to delete from GCS: %w", err)
	}
	return nil
}

func (s *GCSStorage) Exists(ctx context.Context, path string) (bool, error) {
	_, err := s.client.Bucket(s.bucket).Object(s.fullPath(path)).Attrs(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return false, nil
		}
		return false, fmt.Errorf("failed to check if GCS object exists: %w", err)
	}
	return true, nil
}
