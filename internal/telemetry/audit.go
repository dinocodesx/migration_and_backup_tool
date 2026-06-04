package telemetry

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// AuditEntry records a single operation event in the append-only audit log.
type AuditEntry struct {
	// Timestamp is the UTC time the entry was written.
	Timestamp time.Time `json:"ts"`
	// User is the OS username that started the operation.
	User string `json:"user"`
	// Hostname is the machine that ran the operation.
	Hostname string `json:"hostname"`
	// Operation is the type of job ("migrate", "backup", "restore").
	Operation string `json:"operation"`
	// Source is the source database type and host.
	Source string `json:"source"`
	// Target is the target database type and host (empty for backup).
	Target string `json:"target,omitempty"`
	// Tables lists the tables included in the operation.
	Tables []string `json:"tables"`
	// StartTime is when the operation started.
	StartTime time.Time `json:"start_time"`
	// EndTime is when the operation finished (zero if still running).
	EndTime time.Time `json:"end_time,omitempty"`
	// Outcome is "success", "failure", or "partial".
	Outcome string `json:"outcome,omitempty"`
	// RowCount is the total number of rows processed.
	RowCount int64 `json:"row_count,omitempty"`
	// ConfigHash is a SHA-256 hex string of the redacted config for reproducibility.
	ConfigHash string `json:"config_hash,omitempty"`
	// Error contains the error message if the outcome is "failure".
	Error string `json:"error,omitempty"`
}

// AuditLog provides an append-only, JSON-lines audit log for all operations.
// It is safe for concurrent use.
type AuditLog struct {
	mu   sync.Mutex
	file *os.File
}

// NewAuditLog opens (or creates) an audit log file at the given path.
func NewAuditLog(path string) (*AuditLog, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open audit log %q: %w", path, err)
	}
	return &AuditLog{file: f}, nil
}

// Write appends a single audit entry to the log.
func (a *AuditLog) Write(entry AuditEntry) error {
	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now().UTC()
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal audit entry: %w", err)
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if _, err := a.file.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("failed to write audit entry: %w", err)
	}
	return nil
}

// Close flushes and closes the audit log file.
func (a *AuditLog) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.file.Close()
}

// CurrentUser returns the current OS username, or "unknown" on error.
func CurrentUser() string {
	if u := os.Getenv("USER"); u != "" {
		return u
	}
	if u := os.Getenv("USERNAME"); u != "" {
		return u
	}
	return "unknown"
}

// CurrentHostname returns the machine's hostname, or "unknown" on error.
func CurrentHostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}
