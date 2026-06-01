package errs

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"
)

// RetryPolicy defines how a failed operation should be retried.
type RetryPolicy struct {
	MaxAttempts     int
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
	Jitter          float64 // fraction of interval added randomly
}

// DefaultRetry provides a balanced retry strategy for production workloads.
var DefaultRetry = RetryPolicy{
	MaxAttempts:     5,
	InitialInterval: 100 * time.Millisecond,
	MaxInterval:     30 * time.Second,
	Multiplier:      2.0,
	Jitter:          0.2,
}

// Do executes a function with retries according to the policy.
// If the function returns a FatalError the loop stops immediately.
// If the context is cancelled the loop stops and returns the context error.
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

		// Do not retry fatal errors — propagate immediately.
		if IsFatal(err) {
			return err
		}

		lastErr = err
	}
	return fmt.Errorf("operation failed after %d attempts: %w", p.MaxAttempts+1, lastErr)
}

// NextInterval calculates the wait time for the given attempt number (1-based).
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
		// rand.Float64() from math/rand/v2 uses a per-package automatically-seeded source.
		interval = minJitter + (rand.Float64() * jitterRange) //nolint:gosec // jitter, not security
	}

	return time.Duration(interval)
}
