package storage

import (
	"context"
	"io"
)

// Storage defines the interface for persisting and retrieving backup chunks.
type Storage interface {
	// Put writes the data from reader to the specified path.
	Put(ctx context.Context, path string, reader io.Reader) error

	// Get returns a reader for the specified path.
	// The caller is responsible for closing the reader.
	Get(ctx context.Context, path string) (io.ReadCloser, error)

	// List returns a list of paths under the specified prefix.
	List(ctx context.Context, prefix string) ([]string, error)

	// Delete removes the specified path.
	Delete(ctx context.Context, path string) error

	// Exists checks if the specified path exists.
	Exists(ctx context.Context, path string) (bool, error)
}
