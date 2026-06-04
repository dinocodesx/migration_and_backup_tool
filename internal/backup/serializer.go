package backup

import (
	"io"

	"github.com/dinocodesx/gomigrate/internal/record"
)

/*
Serializer defines the interface for converting record batches into a persistent binary stream.

It is the abstraction layer between the database records and the storage format (e.g., Parquet, JSON).

Standard Lifecycle:
 1. Open(w): Bind the serializer to a target io.Writer (usually a Compressor or Storage stream).
 2. Serialize(rec): Convert and write individual records. Buffering may occur.
 3. Close(): Finalize the stream. Writes headers/footers and flushes all internal buffers.

Note: Serializer instances are NOT thread-safe. They are managed by the Engine
which ensures sequential access per chunk.
*/
type Serializer interface {
	// Open binds the serializer to w. It must be called once before any Serialize calls.
	Open(w io.Writer) error

	// Serialize encodes a single record into the bound writer.
	Serialize(rec *record.Record) error

	// Close finalizes the serialization process. This is CRITICAL for formats like Parquet
	// which require a metadata footer to be written at the end of the file.
	Close() error
}
