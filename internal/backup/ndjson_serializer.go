package backup

import (
	"encoding/json"
	"io"
	"github.com/dinocodesx/migration_and_backup_tool/internal/record"
)

// NDJSONSerializer implements the Serializer interface for Newline Delimited JSON.
type NDJSONSerializer struct{}

// NewNDJSONSerializer creates a new NDJSONSerializer.
func NewNDJSONSerializer() *NDJSONSerializer {
	return &NDJSONSerializer{}
}

func (s *NDJSONSerializer) Serialize(w io.Writer, rec *record.Record) error {
	data, err := json.Marshal(rec.Data)
	if err != nil {
		return err
	}
	_, err = w.Write(append(data, '\n'))
	return err
}

func (s *NDJSONSerializer) Flush(w io.Writer) error {
	return nil
}

func (s *NDJSONSerializer) Close(w io.Writer) error {
	return nil
}
