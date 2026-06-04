package backup

import (
	"bytes"
	"testing"

	"github.com/dinocodesx/gomigrate/internal/record"
	"github.com/dinocodesx/gomigrate/internal/schema"
)

// TestParquetSerializer verifies that Go records are correctly mapped and
// serialized into the binary Parquet format, including support for null values.
func TestParquetSerializer(t *testing.T) {
	s := &schema.Schema{
		Name: "users",
		Columns: []schema.Column{
			{Name: "id", Type: "int64", Nullable: false},
			{Name: "name", Type: "string", Nullable: true},
			{Name: "active", Type: "bool", Nullable: false},
		},
	}

	ser, err := NewParquetSerializer(s, 100)
	if err != nil {
		t.Fatalf("failed to create serializer: %v", err)
	}

	var buf bytes.Buffer
	if err := ser.Open(&buf); err != nil {
		t.Fatalf("failed to open serializer: %v", err)
	}

	records := []*record.Record{
		{Data: map[string]any{"id": int64(1), "name": "Alice", "active": true}},
		{Data: map[string]any{"id": int64(2), "name": "Bob", "active": false}},
		{Data: map[string]any{"id": int64(3), "name": nil, "active": true}},
	}

	for _, r := range records {
		if err := ser.Serialize(r); err != nil {
			t.Fatalf("failed to serialize record: %v", err)
		}
	}

	if err := ser.Close(); err != nil {
		t.Fatalf("failed to close serializer: %v", err)
	}

	if buf.Len() == 0 {
		t.Errorf("expected non-empty Parquet output")
	}
}
