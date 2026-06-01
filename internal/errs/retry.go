package errs

import (
	"context"
	"fmt"
	"math/rand"
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

		lastErr = err
		// Check if error is fatal (optional: add error classification)
	}
	return fmt.Errorf("operation failed after %d attempts: %w", p.MaxAttempts+1, lastErr)
}

// NextInterval calculates the wait time for the next attempt.
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
