package backup

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/dinocodesx/gomigrate/internal/record"
)

// NDJSONSerializer implements the Serializer interface for Newline-Delimited JSON.
// This format is highly interoperable and suitable for simple backup scenarios.
// Each record is serialized as a single-line JSON object.
type NDJSONSerializer struct {
	// enc is the internal JSON encoder bound to a stream.
	enc *json.Encoder
}

// NewNDJSONSerializer returns a new NDJSONSerializer instance.
func NewNDJSONSerializer() *NDJSONSerializer {
	return &NDJSONSerializer{}
}

// Open binds the serializer to the target writer.
func (s *NDJSONSerializer) Open(w io.Writer) error {
	s.enc = json.NewEncoder(w)
	return nil
}

// Serialize encodes the record's data map into a single JSON line.
func (s *NDJSONSerializer) Serialize(rec *record.Record) error {
	if s.enc == nil {
		return fmt.Errorf("NDJSONSerializer: Open must be called before Serialize")
	}
	return s.enc.Encode(rec.Data)
}

// Close finalizes the serialization process. For NDJSON, this is a no-op.
func (s *NDJSONSerializer) Close() error {
	s.enc = nil
	return nil
}
