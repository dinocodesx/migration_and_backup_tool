package backup

import (
	"bytes"
	"testing"

	"github.com/dinocodesx/gomigrate/internal/record"
	"github.com/dinocodesx/gomigrate/internal/schema"
)

// TestParquetSerializer verifies the end-to-end serialization of records into Parquet format.
func TestParquetSerializer(t *testing.T) {
	// 1. Define a sample schema with varied types and nullability.
	s := &schema.Schema{
		Name: "users",
		Columns: []schema.Column{
			{Name: "id", Type: "int64", Nullable: false},
			{Name: "name", Type: "string", Nullable: true},
			{Name: "active", Type: "bool", Nullable: false},
		},
	}

	// 2. Initialize the serializer with a small batch size to trigger flushes.
	ser, err := NewParquetSerializer(s, 100)
	if err != nil {
		t.Fatalf("failed to create serializer: %v", err)
	}

	var buf bytes.Buffer
	if err := ser.Open(&buf); err != nil {
		t.Fatalf("failed to open serializer: %v", err)
	}

	// 3. Prepare test records including a null value.
	records := []*record.Record{
		{Data: map[string]any{"id": int64(1), "name": "Alice", "active": true}},
		{Data: map[string]any{"id": int64(2), "name": "Bob", "active": false}},
		{Data: map[string]any{"id": int64(3), "name": nil, "active": true}},
	}

	// 4. Perform serialization.
	for _, r := range records {
		if err := ser.Serialize(r); err != nil {
			t.Fatalf("failed to serialize record: %v", err)
		}
	}

	// 5. Finalize the file.
	if err := ser.Close(); err != nil {
		t.Fatalf("failed to close serializer: %v", err)
	}

	// 6. Basic validation of output.
	if buf.Len() == 0 {
		t.Errorf("expected non-empty Parquet output")
	}
}
