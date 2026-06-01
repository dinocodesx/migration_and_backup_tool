package errs

import (
	"errors"
	"fmt"
)

// FatalError wraps an error that should not be retried.
// Examples: auth failures, table not found, disk full, constraint violations.
type FatalError struct {
	Cause error
}

func (e *FatalError) Error() string { return e.Cause.Error() }
func (e *FatalError) Unwrap() error { return e.Cause }

// Fatal wraps err in a FatalError so the retry loop will not retry it.
func Fatal(err error) error {
	if err == nil {
		return nil
	}
	return &FatalError{Cause: err}
}

// IsFatal reports whether err (or any error in its chain) is a FatalError.
func IsFatal(err error) bool {
	var fe *FatalError
	return errors.As(err, &fe)
}

// TransientError wraps an error that is safe to retry.
type TransientError struct {
	Cause error
}

func (e *TransientError) Error() string { return fmt.Sprintf("transient: %s", e.Cause.Error()) }
func (e *TransientError) Unwrap() error { return e.Cause }

// Transient wraps err in a TransientError, signalling it is safe to retry.
func Transient(err error) error {
	if err == nil {
		return nil
	}
	return &TransientError{Cause: err}
}

// IsTransient reports whether err is explicitly tagged as transient.
func IsTransient(err error) bool {
	var te *TransientError
	return errors.As(err, &te)
}

// IsRetryable reports whether an error should be retried.
// An error is retryable if it is NOT a FatalError.
// Unknown errors are retried by default (safe side for network flaps).
func IsRetryable(err error) bool {
	return !IsFatal(err)
}
