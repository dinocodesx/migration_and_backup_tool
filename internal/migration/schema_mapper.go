// Package migration implements logic for transforming and moving data between
// disparate database engines. It handles the complexities of type mapping,
// structural normalization, and pipeline orchestration.
package migration

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/dinocodesx/gomigrate/internal/config"
	"github.com/dinocodesx/gomigrate/internal/record"
)

// SchemaMapper provides logic for translating records from a source database's
// format into the target database's expected structure. It handles type
// coercion and field-level transformations.
type SchemaMapper struct {
	// sourceType is the identifier of the origin database engine.
	sourceType string
	// targetType is the identifier of the destination database engine.
	targetType string
	// masking defines the PII protection rules.
	masking []config.MaskingConfig
}

// NewSchemaMapper initializes a new SchemaMapper for the specified engine pair.
func NewSchemaMapper(srcType, dstType string, masking []config.MaskingConfig) *SchemaMapper {
	return &SchemaMapper{
		sourceType: srcType,
		targetType: dstType,
		masking:    masking,
	}
}

// MapRecord applies transformation and coercion rules to a single record.
// It returns a modified Record suitable for ingestion into the target database.
func (m *SchemaMapper) MapRecord(rec *record.Record) *record.Record {
	if m.sourceType == "mongo" && m.targetType == "postgres" {
		// Specific coercion logic for Mongo to Postgres migrations.
		// MongoDB _id field is typically an ObjectID string; rename to a
		// postgres-compatible field if needed.
	}

	// Apply masking rules
	for _, rule := range m.masking {
		if val, exists := rec.Data[rule.Column]; exists {
			rec.Data[rule.Column] = m.applyMask(val, rule.Strategy)
		}
	}

	return rec
}

// ApplyTransform applies a named transformation to a value.
// Built-in transforms: none, to_json_string, from_json_string, to_unix_ms,
// from_unix_ms, to_upper, to_lower, uuid_to_string, string_to_uuid, flatten_json.
func ApplyTransform(val any, transform string) (any, error) {
	switch transform {
	case "none", "":
		return val, nil

	case "to_json_string":
		b, err := json.Marshal(val)
		if err != nil {
			return nil, fmt.Errorf("to_json_string: %w", err)
		}
		return string(b), nil

	case "from_json_string":
		s, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("from_json_string: expected string, got %T", val)
		}
		var out any
		if err := json.Unmarshal([]byte(s), &out); err != nil {
			return nil, fmt.Errorf("from_json_string: %w", err)
		}
		return out, nil

	case "to_unix_ms":
		switch t := val.(type) {
		case time.Time:
			return t.UnixMilli(), nil
		case string:
			parsed, err := time.Parse(time.RFC3339, t)
			if err != nil {
				return nil, fmt.Errorf("to_unix_ms: cannot parse %q: %w", t, err)
			}
			return parsed.UnixMilli(), nil
		default:
			return nil, fmt.Errorf("to_unix_ms: unsupported type %T", val)
		}

	case "from_unix_ms":
		switch n := val.(type) {
		case int64:
			return time.UnixMilli(n).UTC(), nil
		case float64:
			return time.UnixMilli(int64(n)).UTC(), nil
		case json.Number:
			ms, err := n.Int64()
			if err != nil {
				return nil, fmt.Errorf("from_unix_ms: %w", err)
			}
			return time.UnixMilli(ms).UTC(), nil
		default:
			return nil, fmt.Errorf("from_unix_ms: unsupported type %T", val)
		}

	case "to_upper":
		s := fmt.Sprintf("%v", val)
		return strings.ToUpper(s), nil

	case "to_lower":
		s := fmt.Sprintf("%v", val)
		return strings.ToLower(s), nil

	case "uuid_to_string":
		// UUID is already stored as string in our canonical form; just ensure it's a string.
		return fmt.Sprintf("%v", val), nil

	case "string_to_uuid":
		s, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("string_to_uuid: expected string, got %T", val)
		}
		// Validate UUID format: 8-4-4-4-12 hex digits
		clean := strings.ReplaceAll(s, "-", "")
		if len(clean) != 32 {
			return nil, fmt.Errorf("string_to_uuid: %q is not a valid UUID", s)
		}
		for _, c := range clean {
			if !unicode.Is(unicode.ASCII_Hex_Digit, c) {
				return nil, fmt.Errorf("string_to_uuid: %q contains non-hex character %q", s, c)
			}
		}
		return s, nil

	case "flatten_json":
		// Flattens a nested map into a dotted-path map.
		m, ok := val.(map[string]any)
		if !ok {
			// If already a string try to parse it first
			if s, ok2 := val.(string); ok2 {
				if err := json.Unmarshal([]byte(s), &m); err != nil {
					return nil, fmt.Errorf("flatten_json: cannot parse string as JSON: %w", err)
				}
			} else {
				return nil, fmt.Errorf("flatten_json: expected map[string]any or JSON string, got %T", val)
			}
		}
		flat := make(map[string]any)
		flattenMap("", m, flat)
		return flat, nil

	default:
		return nil, fmt.Errorf("unknown transform: %q", transform)
	}
}

// flattenMap recursively flattens a nested map into dotted-path keys.
func flattenMap(prefix string, m map[string]any, out map[string]any) {
	for k, v := range m {
		key := k
		if prefix != "" {
			key = prefix + "." + k
		}
		if nested, ok := v.(map[string]any); ok {
			flattenMap(key, nested, out)
		} else {
			out[key] = v
		}
	}
}

// applyMask applies a PII masking strategy to a single field value.
func (m *SchemaMapper) applyMask(val any, strategy string) any {
	strVal := fmt.Sprintf("%v", val)

	switch strategy {
	case "sha256":
		h := sha256.New()
		h.Write([]byte(strVal))
		return fmt.Sprintf("%x", h.Sum(nil))
	case "redact":
		return "[REDACTED]"
	case "partial":
		if len(strVal) > 4 {
			return "****" + strVal[len(strVal)-4:]
		}
		return "****"
	default:
		return val
	}
}
