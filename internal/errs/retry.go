package errs

import (
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

// NextInterval calculates the wait time for the next attempt.
func (p RetryPolicy) NextInterval(attempt int) time.Duration {
	if attempt <= 0 {
		return 0
	}

	if p.MaxAttempts > 0 && attempt > p.MaxAttempts {
		attempt = p.MaxAttempts
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
		jitter := p.Jitter
		if jitter > 1.0 {
			jitter = 1.0
		}

		jitterRange := interval * jitter
		minJitter := interval - (jitterRange / 2)
		interval = minJitter + (rand.Float64() * jitterRange)
	}

	if interval <= 0 && p.InitialInterval > 0 {
		return 1 * time.Millisecond
	}

	return time.Duration(interval)
}
