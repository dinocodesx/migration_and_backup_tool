package backup

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/dinocodesx/gomigrate/internal/record"
)

// NDJSONSerializer implements Serializer for Newline-Delimited JSON (NDJSON).
// Each record is written as a single JSON object followed by a newline (\n).
// This format is highly interoperable and easy to process with standard Unix tools.
type NDJSONSerializer struct {
	enc *json.Encoder
}

// NewNDJSONSerializer creates a new NDJSONSerializer instance.
func NewNDJSONSerializer() *NDJSONSerializer {
	return &NDJSONSerializer{}
}

// Open binds the serializer to an output writer.
// It initializes the internal JSON encoder.
func (s *NDJSONSerializer) Open(w io.Writer) error {
	s.enc = json.NewEncoder(w)
	return nil
}

// Serialize encodes a single record.Record into a JSON line.
// It handles complex nested structures automatically via the standard library encoder.
func (s *NDJSONSerializer) Serialize(rec *record.Record) error {
	if s.enc == nil {
		return fmt.Errorf("NDJSONSerializer: Open must be called before Serialize")
	}
	// We only serialize the Data map, not the entire record object metadata.
	return s.enc.Encode(rec.Data)
}

// Close finalizes the serializer.
// For NDJSON, this is a no-op as no footer or closing tag is required.
func (s *NDJSONSerializer) Close() error {
	s.enc = nil
	return nil
}
