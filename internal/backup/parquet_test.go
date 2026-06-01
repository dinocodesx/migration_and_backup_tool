package backup

import (
	"bytes"
	"testing"

	"github.com/dinocodesx/migration_and_backup_tool/internal/record"
	"github.com/dinocodesx/migration_and_backup_tool/internal/schema"
)

func TestParquetSerializer(t *testing.T) {
	s := &schema.Schema{
		Name: "users",
		Columns: []schema.Column{
			{Name: "id", Type: "int64", Nullable: false},
			{Name: "name", Type: "string", Nullable: true},
			{Name: "active", Type: "bool", Nullable: false},
		},
	}

	ser, err := NewParquetSerializer(s)
	if err != nil {
		t.Fatalf("failed to create serializer: %v", err)
	}

	records := []*record.Record{
		{Data: map[string]any{"id": int64(1), "name": "Alice", "active": true}},
		{Data: map[string]any{"id": int64(2), "name": "Bob", "active": false}},
		{Data: map[string]any{"id": int64(3), "name": nil, "active": true}},
	}

	var buf bytes.Buffer
	for _, r := range records {
		if err := ser.Serialize(&buf, r); err != nil {
			t.Fatalf("failed to serialize: %v", err)
		}
	}

	if err := ser.Close(&buf); err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	if buf.Len() == 0 {
		t.Errorf("expected non-empty output")
	}
}
