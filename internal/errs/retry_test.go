package errs

import (
	"testing"
	"time"
)

// TestRetryPolicy_NextInterval verifies that the exponential backoff calculation
// correctly scales the interval across multiple attempts and respects the maximum cap.
func TestRetryPolicy_NextInterval(t *testing.T) {
	policy := RetryPolicy{
		InitialInterval: 100 * time.Millisecond,
		MaxInterval:     1 * time.Second,
		Multiplier:      2.0,
		Jitter:          0,
	}

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{0, 0},
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{4, 800 * time.Millisecond},
		{5, 1000 * time.Millisecond},
	}

	for _, tt := range tests {
		got := policy.NextInterval(tt.attempt)
		if got != tt.expected {
			t.Errorf("NextInterval(%d) = %v; want %v", tt.attempt, got, tt.expected)
		}
	}
}

// TestRetryPolicy_Jitter verifies that randomized jitter is applied within the
// expected bounds to ensure that intervals are non-deterministic.
func TestRetryPolicy_Jitter(t *testing.T) {
	policy := RetryPolicy{
		MaxAttempts:     5,
		InitialInterval: 100 * time.Millisecond,
		MaxInterval:     1 * time.Second,
		Multiplier:      2.0,
		Jitter:          0.2,
	}

	for i := 1; i <= 10; i++ {
		got := policy.NextInterval(i)

		effAttempt := i
		if effAttempt > policy.MaxAttempts {
			effAttempt = policy.MaxAttempts
		}

		base := float64(policy.InitialInterval)
		for j := 1; j < effAttempt; j++ {
			base *= policy.Multiplier
		}
		if base > float64(policy.MaxInterval) {
			base = float64(policy.MaxInterval)
		}

		min := time.Duration(base * (1 - policy.Jitter/2))
		max := time.Duration(base * (1 + policy.Jitter/2))

		if got < min || got > max {
			t.Errorf("NextInterval(%d) = %v; want range [%v, %v]", i, got, min, max)
		}
	}
}
