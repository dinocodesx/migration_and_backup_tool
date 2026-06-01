package backup

import (
	"io"

	"github.com/dinocodesx/gomigrate/internal/record"
)

// Serializer converts records into a streamable binary format.
//
// Lifecycle:
//
//  1. Call Open(w) to bind the serializer to an output writer.
//  2. Call Serialize(rec) for each record.
//  3. Call Close() to flush all buffered data and finalise the format (e.g.
//     write Parquet footer). After Close, the serializer MUST NOT be reused.
//
// Thread safety: implementations are NOT required to be safe for concurrent
// use; the backup engine calls these methods from a single goroutine.
type Serializer interface {
	// Open binds the serializer to w for the lifetime of one chunk.
	Open(w io.Writer) error

	// Serialize encodes a single record into the underlying writer.
	Serialize(rec *record.Record) error

	// Close flushes any buffered data and finalises the format (e.g. Parquet
	// footer). The caller is responsible for closing w itself.
	Close() error
}
