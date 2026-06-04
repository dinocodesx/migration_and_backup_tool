package errs

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	// ErrCircuitOpen is returned by Execute when the circuit is in the Open state
	// and is rejecting all incoming requests.
	ErrCircuitOpen = errors.New("circuit breaker is open")
)

// State defines the possible operational modes of a circuit breaker.
type State int

const (
	// StateClosed allows all requests to pass through.
	StateClosed State = iota
	// StateOpen rejects all requests immediately.
	StateOpen
	// StateHalfOpen allows a single trial request to verify if the system has recovered.
	StateHalfOpen
)

// CircuitBreaker implements the circuit breaker pattern to improve system resilience.
// It tracks consecutive failures and "trips" the circuit when a threshold is
// exceeded, preventing the application from overloading a failing dependency.
type CircuitBreaker struct {
	mu            sync.Mutex
	state         State
	failures      int
	threshold     int
	timeout       time.Duration
	lastFailure   time.Time
	trialInFlight bool
}

// NewCircuitBreaker initializes a new CircuitBreaker with the specified
// failure threshold and recovery timeout.
func NewCircuitBreaker(threshold int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		threshold: threshold,
		timeout:   timeout,
	}
}

// Execute wraps a function call with circuit breaker logic. It returns
// ErrCircuitOpen if the circuit is currently open and no trial is permitted.
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

// CurrentState returns the current lifecycle state of the circuit breaker.
func (cb *CircuitBreaker) CurrentState() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

// allow determines if a request should be permitted based on the current state.
func (cb *CircuitBreaker) allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		return true

	case StateOpen:
		if time.Since(cb.lastFailure) > cb.timeout {
			cb.state = StateHalfOpen
			cb.trialInFlight = true
			return true
		}
		return false

	case StateHalfOpen:
		if cb.trialInFlight {
			return false
		}
		cb.trialInFlight = true
		return true
	}

	return false
}

// recordFailure increments the failure count and trips the circuit if necessary.
func (cb *CircuitBreaker) recordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures++
	cb.lastFailure = time.Now()
	cb.trialInFlight = false

	if cb.state == StateHalfOpen || cb.failures >= cb.threshold {
		cb.state = StateOpen
	}
}

// recordSuccess resets the failure count and restores the circuit to the Closed state.
func (cb *CircuitBreaker) recordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures = 0
	cb.trialInFlight = false
	cb.state = StateClosed
}
