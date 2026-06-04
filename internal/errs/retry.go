package errs

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"
)

// RetryPolicy defines the parameters for an exponential backoff strategy.
// It includes logic for jitter to prevent "thundering herd" problems where
// multiple clients retry simultaneously and overload a recovering system.
type RetryPolicy struct {
	// MaxAttempts is the maximum number of times an operation will be retried.
	MaxAttempts int
	// InitialInterval is the wait time before the first retry.
	InitialInterval time.Duration
	// MaxInterval is the maximum possible wait time between retries.
	MaxInterval time.Duration
	// Multiplier is the factor by which the interval increases each attempt.
	Multiplier float64
	// Jitter is the fraction of randomness added to the interval (0 to 1).
	Jitter float64
}

// DefaultRetry provides a balanced retry strategy for typical production workloads.
var DefaultRetry = RetryPolicy{
	MaxAttempts:     5,
	InitialInterval: 100 * time.Millisecond,
	MaxInterval:     30 * time.Second,
	Multiplier:      2.0,
	Jitter:          0.2,
}

// Do executes the function 'f' repeatedly according to the policy until it
// succeeds, returns a FatalError, or the context is cancelled.
func (p RetryPolicy) Do(ctx context.Context, f func() error) error {
	var lastErr error
	for attempt := 0; attempt <= p.MaxAttempts; attempt++ {
		if attempt > 0 {
			wait := p.NextInterval(attempt)
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			}
		}

		err := f()
		if err == nil {
			return nil
		}

		if IsFatal(err) {
			return err
		}

		lastErr = err
	}
	return fmt.Errorf("operation failed after %d attempts: %w", p.MaxAttempts+1, lastErr)
}

// NextInterval calculates the delay duration for a specific retry attempt,
// applying exponential backoff and randomized jitter.
func (p RetryPolicy) NextInterval(attempt int) time.Duration {
	if attempt <= 0 {
		return 0
	}

	interval := float64(p.InitialInterval)
	for i := 1; i < attempt; i++ {
		interval *= p.Multiplier
		if interval > float64(p.MaxInterval) {
			interval = float64(p.MaxInterval)
			break
		}
	}

	if p.Jitter > 0 {
		jitterRange := interval * p.Jitter
		minJitter := interval - (jitterRange / 2)
		interval = minJitter + (rand.Float64() * jitterRange)
	}

	return time.Duration(interval)
}
