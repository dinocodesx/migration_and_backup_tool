package backup

import (
	"time"
	"github.com/dinocodesx/gomigrate/internal/schema"
)

// Manifest represents the index of a completed backup.
type Manifest struct {
	Version        int             `json:"version"`
	OperationID    string          `json:"operation_id"`
	Source         SourceMetadata  `json:"source"`
	CreatedAt      time.Time       `json:"created_at"`
	RowCount       int64           `json:"row_count"`
	ChunkSizeBytes int64           `json:"chunk_size_bytes"`
	Chunks         []Chunk         `json:"chunks"`
	SchemaSnapshot schema.Schema   `json:"schema_snapshot"`
}

// SourceMetadata contains information about the database source.
type SourceMetadata struct {
	Type  string `json:"type"`
	DB    string `json:"db"`
	Table string `json:"table"`
}

// Chunk represents a single data file in the backup.
type Chunk struct {
	Index    int    `json:"index"`
	File     string `json:"file"`
	RowCount int64  `json:"row_count"`
	SHA256   string `json:"sha256"`
}
