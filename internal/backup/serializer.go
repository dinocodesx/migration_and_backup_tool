package backup

import (
	"io"

	"github.com/dinocodesx/gomigrate/internal/record"
)

// Serializer defines the interface for converting record streams into persistent
// file formats. It provides the abstraction needed to support multiple output
// formats like Parquet, NDJSON, or CSV within the backup engine.
//
// Lifecycle:
//  1. Open(w): Bind to an output stream (usually a compressor or storage writer).
//  2. Serialize(rec): Encode individual records.
//  3. Close(): Finalize the stream and write any required footers/metadata.
type Serializer interface {
	// Open prepares the serializer to write data to 'w'.
	Open(w io.Writer) error

	// Serialize encodes a single record into the bound writer.
	Serialize(rec *record.Record) error

	// Close finalizes the serialization process. This is critical for formats
	// like Parquet that require trailing metadata.
	Close() error
}
