package errs

import (
	"testing"
	"time"
)

func TestRetryPolicy_NextInterval(t *testing.T) {
	policy := RetryPolicy{
		InitialInterval: 100 * time.Millisecond,
		MaxInterval:     1 * time.Second,
		Multiplier:      2.0,
		Jitter:          0, // Disable jitter for predictable testing
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
		{5, 1000 * time.Millisecond}, // MaxInterval capped
	}

	for _, tt := range tests {
		got := policy.NextInterval(tt.attempt)
		if got != tt.expected {
			t.Errorf("NextInterval(%d) = %v; want %v", tt.attempt, got, tt.expected)
		}
	}
}

func TestRetryPolicy_Jitter(t *testing.T) {
	policy := RetryPolicy{
		InitialInterval: 100 * time.Millisecond,
		MaxInterval:     1 * time.Second,
		Multiplier:      2.0,
		Jitter:          0.5,
	}

	// Just ensure it doesn't crash and stays within bounds
	for i := 1; i < 10; i++ {
		got := policy.NextInterval(i)
		if got <= 0 {
			t.Errorf("NextInterval(%d) with jitter should be > 0, got %v", i, got)
		}
	}
}
