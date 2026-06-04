package backup

import (
	"time"

	"github.com/dinocodesx/gomigrate/internal/schema"
)

// Manifest represents the metadata and file structure of a completed backup.
// It acts as the "root of trust" for restoration and integrity verification,
// documenting exactly what data was backed up and how it is organized.
type Manifest struct {
	// Version is the schema version of the manifest format itself.
	Version int `json:"version"`
	// OperationID is the unique identifier of the backup job.
	OperationID string `json:"operation_id"`
	// Source contains provenance information about the origin database.
	Source SourceMetadata `json:"source"`
	// CreatedAt is the UTC timestamp when the backup was finalized.
	CreatedAt time.Time `json:"created_at"`
	// RowCount is the total number of records across all backup chunks.
	RowCount int64 `json:"row_count"`
	// ChunkSizeBytes is the configured size threshold used for chunking.
	ChunkSizeBytes int64 `json:"chunk_size_bytes"`
	// Chunks is an ordered list of data files and their integrity hashes.
	Chunks []Chunk `json:"chunks"`
	// SchemaSnapshot is the exact structural definition of the table at backup time.
	SchemaSnapshot schema.Schema `json:"schema_snapshot"`
}

// SourceMetadata documents the origin of the backed-up data.
type SourceMetadata struct {
	// Type is the source database engine (e.g., "postgres").
	Type string `json:"type"`
	// DB is the name of the source database.
	DB string `json:"db"`
	// Table is the name of the backed-up table.
	Table string `json:"table"`
}

// Chunk describes a single data artifact within a multi-file backup.
type Chunk struct {
	// Index is the sequence number of the chunk (0-indexed).
	Index int `json:"index"`
	// File is the relative path/name of the chunk artifact in storage.
	File string `json:"file"`
	// RowCount is the number of records contained in this specific chunk.
	RowCount int64 `json:"row_count"`
	// SHA256 is the hexadecimal integrity hash of the chunk file.
	SHA256 string `json:"sha256"`
}
