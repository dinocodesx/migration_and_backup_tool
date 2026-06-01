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
		MaxAttempts:     5,
		InitialInterval: 100 * time.Millisecond,
		MaxInterval:     1 * time.Second,
		Multiplier:      2.0,
		Jitter:          0.2,
	}

	for i := 1; i <= 10; i++ {
		got := policy.NextInterval(i)

		// Determine effective attempt for calculation
		effAttempt := i
		if effAttempt > policy.MaxAttempts {
			effAttempt = policy.MaxAttempts
		}

		// Compute base interval (without jitter)
		base := float64(policy.InitialInterval)
		for j := 1; j < effAttempt; j++ {
			base *= policy.Multiplier
		}
		if base > float64(policy.MaxInterval) {
			base = float64(policy.MaxInterval)
		}

		// Assert bounds
		min := time.Duration(base * (1 - policy.Jitter/2))
		max := time.Duration(base * (1 + policy.Jitter/2))

		if got < min || got > max {
			t.Errorf("NextInterval(%d) = %v; want range [%v, %v]", i, got, min, max)
		}
	}

	// Verify MaxAttempts behavior specifically
	if policy.NextInterval(5) == 0 {
		t.Fatal("NextInterval(5) should not be 0")
	}
	// We can't compare exactly due to jitter, but let's check without jitter
	policyNoJitter := policy
	policyNoJitter.Jitter = 0
	if policyNoJitter.NextInterval(5) != policyNoJitter.NextInterval(6) {
		t.Errorf("expected NextInterval(5) == NextInterval(6) with no jitter, got %v != %v",
			policyNoJitter.NextInterval(5), policyNoJitter.NextInterval(6))
	}
}
