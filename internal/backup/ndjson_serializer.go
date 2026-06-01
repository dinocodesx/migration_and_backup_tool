package backup

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/dinocodesx/gomigrate/internal/record"
)

// NDJSONSerializer implements Serializer for Newline-Delimited JSON (NDJSON).
// Each record is written as a single JSON object followed by a newline.
type NDJSONSerializer struct {
	enc *json.Encoder
}

// NewNDJSONSerializer creates a new NDJSONSerializer.
func NewNDJSONSerializer() *NDJSONSerializer {
	return &NDJSONSerializer{}
}

// Open binds the serializer to w. It must be called before Serialize.
func (s *NDJSONSerializer) Open(w io.Writer) error {
	s.enc = json.NewEncoder(w)
	return nil
}

// Serialize encodes rec.Data as a JSON line.
func (s *NDJSONSerializer) Serialize(rec *record.Record) error {
	if s.enc == nil {
		return fmt.Errorf("NDJSONSerializer: Open must be called before Serialize")
	}
	return s.enc.Encode(rec.Data)
}

// Close is a no-op for NDJSON (no footer required).
func (s *NDJSONSerializer) Close() error {
	s.enc = nil
	return nil
}
