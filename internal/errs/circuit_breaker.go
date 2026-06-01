package errs

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrCircuitOpen = errors.New("circuit breaker is open")
)

type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

// CircuitBreaker implements the circuit breaker pattern to prevent cascading failures.
type CircuitBreaker struct {
	mu          sync.RWMutex
	state       State
	failures    int
	threshold   int
	timeout     time.Duration
	lastFailure time.Time
}

// NewCircuitBreaker creates a new circuit breaker.
func NewCircuitBreaker(threshold int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		threshold: threshold,
		timeout:   timeout,
	}
}

// Execute wraps a function call with circuit breaker logic.
func (cb *CircuitBreaker) Execute(ctx context.Context, f func() error) error {
	if !cb.allow() {
		return ErrCircuitOpen
	}

	err := f()
	if err != nil {
		cb.recordFailure()
		return err
	}

	cb.recordSuccess()
	return nil
}

func (cb *CircuitBreaker) allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == StateClosed {
		return true
	}

	if cb.state == StateOpen {
		if time.Since(cb.lastFailure) > cb.timeout {
			cb.state = StateHalfOpen
			return true
		}
		return false
	}

	if cb.state == StateHalfOpen {
		// In half-open, we allow one request to trial the circuit.
		// Subsequent concurrent requests will be blocked until the trial finishes.
		// We use a simple strategy: if it's already half-open, we only allow it
		// if we haven't seen a failure/success since entering half-open.
		// For simplicity in this implementation, we'll just allow the first one
		// and others will wait or be rejected.
		return false
	}

	return true
}

func (cb *CircuitBreaker) recordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures++
	cb.lastFailure = time.Now()
	if cb.state == StateHalfOpen || cb.failures >= cb.threshold {
		cb.state = StateOpen
	}
}

func (cb *CircuitBreaker) recordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures = 0
	cb.state = StateClosed
}
