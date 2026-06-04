package migration_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dinocodesx/gomigrate/internal/migration"
)

func TestApplyTransform_None(t *testing.T) {
	out, err := migration.ApplyTransform("hello", "none")
	if err != nil {
		t.Fatal(err)
	}
	if out != "hello" {
		t.Errorf("expected 'hello', got %v", out)
	}
}

func TestApplyTransform_ToJSONString(t *testing.T) {
	out, err := migration.ApplyTransform(map[string]any{"key": "val"}, "to_json_string")
	if err != nil {
		t.Fatal(err)
	}
	s, ok := out.(string)
	if !ok {
		t.Fatalf("expected string, got %T", out)
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(s), &parsed); err != nil {
		t.Fatalf("result is not valid JSON: %v", err)
	}
	if parsed["key"] != "val" {
		t.Errorf("expected key='val', got %v", parsed["key"])
	}
}

func TestApplyTransform_FromJSONString(t *testing.T) {
	out, err := migration.ApplyTransform(`{"foo":"bar"}`, "from_json_string")
	if err != nil {
		t.Fatal(err)
	}
	m, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", out)
	}
	if m["foo"] != "bar" {
		t.Errorf("expected foo='bar', got %v", m["foo"])
	}
}

func TestApplyTransform_ToUnixMs(t *testing.T) {
	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	out, err := migration.ApplyTransform(ts, "to_unix_ms")
	if err != nil {
		t.Fatal(err)
	}
	ms, ok := out.(int64)
	if !ok {
		t.Fatalf("expected int64, got %T", out)
	}
	if ms != ts.UnixMilli() {
		t.Errorf("expected %d, got %d", ts.UnixMilli(), ms)
	}
}

func TestApplyTransform_FromUnixMs(t *testing.T) {
	expected := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	out, err := migration.ApplyTransform(expected.UnixMilli(), "from_unix_ms")
	if err != nil {
		t.Fatal(err)
	}
	got, ok := out.(time.Time)
	if !ok {
		t.Fatalf("expected time.Time, got %T", out)
	}
	if !got.Equal(expected) {
		t.Errorf("expected %v, got %v", expected, got)
	}
}

func TestApplyTransform_ToUpper(t *testing.T) {
	out, err := migration.ApplyTransform("hello world", "to_upper")
	if err != nil {
		t.Fatal(err)
	}
	if out != "HELLO WORLD" {
		t.Errorf("expected 'HELLO WORLD', got %v", out)
	}
}

func TestApplyTransform_ToLower(t *testing.T) {
	out, err := migration.ApplyTransform("HELLO WORLD", "to_lower")
	if err != nil {
		t.Fatal(err)
	}
	if out != "hello world" {
		t.Errorf("expected 'hello world', got %v", out)
	}
}

func TestApplyTransform_UUIDToString(t *testing.T) {
	uuid := "550e8400-e29b-41d4-a716-446655440000"
	out, err := migration.ApplyTransform(uuid, "uuid_to_string")
	if err != nil {
		t.Fatal(err)
	}
	if out != uuid {
		t.Errorf("expected %q, got %v", uuid, out)
	}
}

func TestApplyTransform_StringToUUID_Valid(t *testing.T) {
	uuid := "550e8400-e29b-41d4-a716-446655440000"
	out, err := migration.ApplyTransform(uuid, "string_to_uuid")
	if err != nil {
		t.Fatal(err)
	}
	if out != uuid {
		t.Errorf("expected %q, got %v", uuid, out)
	}
}

func TestApplyTransform_StringToUUID_Invalid(t *testing.T) {
	_, err := migration.ApplyTransform("not-a-uuid", "string_to_uuid")
	if err == nil {
		t.Error("expected error for invalid UUID, got nil")
	}
}

func TestApplyTransform_FlattenJSON(t *testing.T) {
	nested := map[string]any{
		"a": map[string]any{
			"b": map[string]any{
				"c": "deep",
			},
		},
		"x": 42,
	}
	out, err := migration.ApplyTransform(nested, "flatten_json")
	if err != nil {
		t.Fatal(err)
	}
	flat, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", out)
	}
	if flat["a.b.c"] != "deep" {
		t.Errorf("expected a.b.c='deep', got %v", flat["a.b.c"])
	}
	if flat["x"] != 42 {
		t.Errorf("expected x=42, got %v", flat["x"])
	}
}

func TestApplyTransform_UnknownTransform(t *testing.T) {
	_, err := migration.ApplyTransform("val", "does_not_exist")
	if err == nil {
		t.Error("expected error for unknown transform, got nil")
	}
	if !strings.Contains(err.Error(), "unknown transform") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestApplyTransform_ToUnixMs_StringInput(t *testing.T) {
	ts := "2025-01-01T00:00:00Z"
	out, err := migration.ApplyTransform(ts, "to_unix_ms")
	if err != nil {
		t.Fatal(err)
	}
	ms, ok := out.(int64)
	if !ok {
		t.Fatalf("expected int64, got %T", out)
	}
	expected := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	if ms != expected {
		t.Errorf("expected %d, got %d", expected, ms)
	}
}
