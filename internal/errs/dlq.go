package errs

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/dinocodesx/gomigrate/internal/record"
)

// DLQRecord wraps a failed record with error information for the dead-letter queue.
type DLQRecord struct {
	RecordID    string         `json:"record_id"`
	Table       string         `json:"table"`
	Error       string         `json:"error"`
	Attempts    int            `json:"attempts"`
	Timestamp   time.Time      `json:"timestamp"`
	Payload     map[string]any `json:"payload"`
}

// DLQ handles the persistence of records that failed all retry attempts.
type DLQ struct {
	mu   sync.Mutex
	file *os.File
}

// NewDLQ opens or creates a DLQ file at the specified path.
func NewDLQ(path string) (*DLQ, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open DLQ file: %w", err)
	}
	return &DLQ{file: f}, nil
}

// Close flushes and closes the DLQ file.
func (d *DLQ) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.file.Close()
}

// Write captures a failed record and its error context to the DLQ.
func (d *DLQ) Write(rec *record.Record, err error, attempts int) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	dlqRec := DLQRecord{
		RecordID:  rec.ID,
		Table:     rec.Metadata.SourceTable,
		Error:     err.Error(),
		Attempts:  attempts,
		Timestamp: time.Now(),
		Payload:   rec.Data,
	}

	data, err := json.Marshal(dlqRec)
	if err != nil {
		return fmt.Errorf("failed to marshal DLQ record: %w", err)
	}

	if _, err := d.file.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("failed to write to DLQ file: %w", err)
	}

	return nil
}
