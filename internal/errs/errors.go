// Package errs provides error handling and resilience patterns for gomigrate.
// It includes logic for identifying fatal vs. transient errors, implementing
// retry policies, and managing circuit breakers to prevent cascading failures.
package errs

import (
	"errors"
	"fmt"
)

// FatalError wraps an error that is considered unrecoverable. Encountering
// a FatalError should cause the current operation to stop immediately without retrying.
// Examples include authentication failures, missing tables, or full disks.
type FatalError struct {
	// Cause is the underlying error that triggered the failure.
	Cause error
}

func (e *FatalError) Error() string { return e.Cause.Error() }
func (e *FatalError) Unwrap() error { return e.Cause }

// Fatal wraps an existing error into a FatalError, signalling that no further
// retries should be attempted for the current operation.
func Fatal(err error) error {
	if err == nil {
		return nil
	}
	return &FatalError{Cause: err}
}

// IsFatal determines if 'err' or any error in its chain is of type FatalError.
func IsFatal(err error) bool {
	var fe *FatalError
	return errors.As(err, &fe)
}

// TransientError wraps an error that is likely to resolve on its own after
// a period of time. Encountering a TransientError triggers the retry logic.
// Examples include temporary network glitches or database lock contention.
type TransientError struct {
	// Cause is the underlying error that triggered the failure.
	Cause error
}

func (e *TransientError) Error() string { return fmt.Sprintf("transient: %s", e.Cause.Error()) }
func (e *TransientError) Unwrap() error { return e.Cause }

// Transient wraps an existing error into a TransientError, explicitly
// marking it as safe to retry.
func Transient(err error) error {
	if err == nil {
		return nil
	}
	return &TransientError{Cause: err}
}

// IsTransient determines if 'err' has been explicitly tagged as transient.
func IsTransient(err error) bool {
	var te *TransientError
	return errors.As(err, &te)
}

// IsRetryable determines if an operation should be retried based on the
// error encountered. By default, all errors except those explicitly wrapped
// as FatalError are considered retryable.
func IsRetryable(err error) bool {
	return !IsFatal(err)
}
