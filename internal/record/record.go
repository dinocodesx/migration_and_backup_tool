package record

import "time"

// Record is the universal in-memory representation of a single database row or document.
type Record struct {
	ID       string         // Source-assigned logical ID
	Data     map[string]any // Column/field values (normalized)
	Metadata RecordMetadata
}

// RecordMetadata contains tracking information about the record's origin and integrity.
type RecordMetadata struct {
	SourceTable   string
	SourceDB      string
	PartitionID   string
	Offset        any      // Logical offset (e.g., PK value, ctid, or cursor position)
	Checksum      [32]byte // SHA-256 of Data bytes
	IngestionTime time.Time
}
