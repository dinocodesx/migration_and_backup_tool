package backup

import (
	"io"
	"github.com/dinocodesx/gomigrate/internal/record"
)

// Serializer defines the interface for converting records into a streamable format.
type Serializer interface {
	// Serialize writes a single record to the writer.
	Serialize(w io.Writer, rec *record.Record) error

	// Flush ensures any buffered data is written to the writer.
	Flush(w io.Writer) error

	// Close finalizes the serialization process.
	Close(w io.Writer) error
}
