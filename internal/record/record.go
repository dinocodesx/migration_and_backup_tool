// Package record defines the universal data representation used within the
// gomigrate pipeline. It abstracts away database-specific formats into a
// common structure that can be easily transformed and moved between different
// storage engines.
package record

import "time"

// Record is the canonical in-memory representation of a single database row or
// document. It decouples the data content from its source-specific metadata,
// enabling generic transformation and serialization logic.
type Record struct {
	// ID is a unique identifier for the record, usually derived from the
	// source's primary key or unique index.
	ID string
	// Data stores the actual column/field values as a map. Keys are field names,
	// and values are normalized Go types.
	Data map[string]any
	// Metadata contains provenance and integrity information for the record.
	Metadata RecordMetadata
}

// RecordMetadata provides traceability and auditing information for a specific
// record as it flows through the migration pipeline.
type RecordMetadata struct {
	// SourceTable is the name of the table or collection the record was read from.
	SourceTable string
	// SourceDB is the name of the database the record originated from.
	SourceDB string
	// PartitionID identifies the unit of parallel work this record belonged to.
	PartitionID string
	// Offset represents the logical position of the record within its partition
	// (e.g., a PK value or a cursor position). Used for checkpointing.
	Offset any
	// Checksum is a SHA-256 hash of the 'Data' content, used for integrity verification.
	Checksum [32]byte
	// IngestionTime is the timestamp when the record was first read into the system.
	IngestionTime time.Time
}
