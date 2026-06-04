package checkpoint

import "time"

// PartitionStatus represents the operational state of a single parallel work unit.
type PartitionStatus string

const (
	// StatusPending indicates the partition has been discovered but not started.
	StatusPending PartitionStatus = "Pending"
	// StatusInProgress indicates a worker is currently processing the partition.
	StatusInProgress PartitionStatus = "InProgress"
	// StatusDone indicates the partition has been fully processed and committed.
	StatusDone PartitionStatus = "Done"
	// StatusFailed indicates the partition processing stopped due to a fatal error.
	StatusFailed PartitionStatus = "Failed"
)

// PartitionCheckpoint stores the persistent progress of a specific partition.
// It allows the migration engine to resume from the exact last-committed record
// in the event of a failure or intentional restart.
type PartitionCheckpoint struct {
	// PartitionID is the unique identifier for the partition.
	PartitionID string `json:"partition_id"`
	// Status is the current lifecycle state of the partition.
	Status PartitionStatus `json:"status"`
	// LastCommitted is the logical offset of the last successfully written record.
	LastCommitted any `json:"last_committed"`
	// RowsWritten is the total number of records successfully migrated in this partition.
	RowsWritten int64 `json:"rows_written"`
	// ErrorCount is the number of individual record failures encountered.
	ErrorCount int64 `json:"error_count"`
	// UpdatedAt is the timestamp of the last checkpoint update.
	UpdatedAt time.Time `json:"updated_at"`
}

// OperationMeta stores global metadata for a high-level job (migration or backup).
type OperationMeta struct {
	// OperationID is the unique identifier for the entire job.
	OperationID string `json:"operation_id"`
	// StartTime is the UTC timestamp when the job was initiated.
	StartTime time.Time `json:"start_time"`
	// ConfigHash is a checksum of the configuration used for this job.
	ConfigHash string `json:"config_hash"`
}
