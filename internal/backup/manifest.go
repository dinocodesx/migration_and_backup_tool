package backup

import (
	"time"

	"github.com/dinocodesx/gomigrate/internal/schema"
)

// Manifest represents the index of a completed backup.
// It acts as the "root of trust" and the roadmap for any subsequent restoration or verification.
// If the manifest is lost, the backup chunks become difficult to assemble correctly.
type Manifest struct {
	Version        int            `json:"version"`          // Schema version of the manifest itself.
	OperationID    string         `json:"operation_id"`     // Unique ID of the backup operation.
	Source         SourceMetadata `json:"source"`           // Details about where the data came from.
	CreatedAt      time.Time      `json:"created_at"`       // Timestamp when the backup was finalized.
	RowCount       int64          `json:"row_count"`        // Total records across all chunks.
	ChunkSizeBytes int64          `json:"chunk_size_bytes"` // Configured size threshold for chunks.
	Chunks         []Chunk        `json:"chunks"`           // Ordered list of chunk files and their metadata.
	SchemaSnapshot schema.Schema  `json:"schema_snapshot"`  // Exact schema of the table at backup time.
}

// SourceMetadata contains provenance information about the database source.
type SourceMetadata struct {
	Type  string `json:"type"`  // Source adapter type (e.g., "postgres", "mongo").
	DB    string `json:"db"`    // Name of the database.
	Table string `json:"table"` // Name of the table.
}

// Chunk represents a single data file in the backup.
// It includes a SHA256 checksum for end-to-end integrity verification.
type Chunk struct {
	Index    int    `json:"index"`     // Sequence number (0, 1, 2...).
	File     string `json:"file"`      // Filename relative to the manifest location.
	RowCount int64  `json:"row_count"` // Number of records in this specific chunk.
	SHA256   string `json:"sha256"`    // Checksum for integrity validation.
}
