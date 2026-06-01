package checkpoint

import "time"

type PartitionStatus string

const (
	StatusPending    PartitionStatus = "Pending"
	StatusInProgress PartitionStatus = "InProgress"
	StatusDone       PartitionStatus = "Done"
	StatusFailed     PartitionStatus = "Failed"
)

// PartitionCheckpoint tracks the progress of a single partition.
type PartitionCheckpoint struct {
	PartitionID   string          `json:"partition_id"`
	Status        PartitionStatus `json:"status"`
	LastCommitted any             `json:"last_committed"` // last primary key / offset
	RowsWritten   int64           `json:"rows_written"`
	ErrorCount    int64           `json:"error_count"`
	UpdatedAt     time.Time       `json:"updated_at"`
}

// OperationMeta tracks top-level metadata for a migration or backup.
type OperationMeta struct {
	OperationID string    `json:"operation_id"`
	StartTime   time.Time `json:"start_time"`
	ConfigHash  string    `json:"config_hash"`
}
