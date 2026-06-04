// Package storage provides common interfaces and implementations for interacting
// with various blob storage backends. It abstracts the complexities of provider-specific
// APIs (like AWS S3 or GCS) into a uniform interface.
package storage

import (
	"context"
	"io"
)

// Storage defines the mandatory interface for all storage backend implementations.
// It supports basic CRUD-like operations for managing backup artifacts.
type Storage interface {
	// Put streams data from 'reader' into a specific 'path' in the storage backend.
	Put(ctx context.Context, path string, reader io.Reader) error

	// Get returns a readable stream for the object at the specified 'path'.
	// It is the caller's responsibility to close the returned ReadCloser.
	Get(ctx context.Context, path string) (io.ReadCloser, error)

	// List returns a slice of all object paths located under the specified 'prefix'.
	List(ctx context.Context, prefix string) ([]string, error)

	// Delete removes the object at the specified 'path' from the storage backend.
	Delete(ctx context.Context, path string) error

	// Exists determines if an object currently exists at the specified 'path'.
	Exists(ctx context.Context, path string) (bool, error)
}
