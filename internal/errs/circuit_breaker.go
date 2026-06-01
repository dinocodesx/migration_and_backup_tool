package errs

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	// ErrCircuitOpen is returned when the circuit breaker is open and rejects the call.
	ErrCircuitOpen = errors.New("circuit breaker is open")
)

// State represents the state of the circuit breaker.
type State int

const (
	StateClosed   State = iota // Normal operation — all requests allowed.
	StateOpen                  // Failure threshold exceeded — all requests rejected.
	StateHalfOpen              // Timeout elapsed — one trial request allowed.
)

// CircuitBreaker implements the circuit breaker pattern to prevent cascading failures.
//
// State transitions:
//
//	Closed  → Open      : failure count reaches threshold
//	Open    → HalfOpen  : timeout elapses since last failure
//	HalfOpen → Closed   : trial request succeeds
//	HalfOpen → Open     : trial request fails
type CircuitBreaker struct {
	mu          sync.Mutex
	state       State
	failures    int
	threshold   int
	timeout     time.Duration
	lastFailure time.Time
	// trialInFlight prevents a second goroutine from starting another trial while one is running.
	trialInFlight bool
}

// NewCircuitBreaker creates a new CircuitBreaker.
//   - threshold: number of consecutive failures before opening.
//   - timeout: how long to wait in Open state before attempting a trial.
func NewCircuitBreaker(threshold int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		threshold: threshold,
		timeout:   timeout,
	}
}

// Execute wraps a function call with circuit breaker logic.
// Returns ErrCircuitOpen if the circuit is open and no trial is permitted.
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

// State returns the current circuit state (for observability / metrics).
func (cb *CircuitBreaker) CurrentState() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

// allow decides whether to permit an execution.
// It also performs the Open→HalfOpen transition atomically.
func (cb *CircuitBreaker) allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		return true

	case StateOpen:
		if time.Since(cb.lastFailure) > cb.timeout {
			// Transition to HalfOpen and allow exactly ONE trial request.
			cb.state = StateHalfOpen
			cb.trialInFlight = true
			return true
		}
		return false

	case StateHalfOpen:
		// Block concurrent requests while the trial is in-flight.
		if cb.trialInFlight {
			return false
		}
		// trialInFlight was reset by a successful trial — allow next request.
		// (This branch is a safety net; normally Closed state is restored by recordSuccess.)
		cb.trialInFlight = true
		return true
	}

	return false
}

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

func (cb *CircuitBreaker) recordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures = 0
	cb.trialInFlight = false
	cb.state = StateClosed
}
