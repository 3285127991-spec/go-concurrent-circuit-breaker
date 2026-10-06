// Package circuitbreaker provides a concurrent, in-memory circuit breaker
// with token-based request tracking and generation (epoch) isolation
// across state cycles.
package circuitbreaker

import (
	"errors"
	"sync"
	"time"
)

// State is the circuit breaker state.
type State int

const (
	// Closed allows all requests and counts consecutive failures.
	Closed State = iota
	// Open rejects all requests until the open duration elapses.
	Open
	// HalfOpen allows exactly one probe request.
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Closed:
		return "Closed"
	case Open:
		return "Open"
	case HalfOpen:
		return "HalfOpen"
	}
	return "Unknown"
}

var (
	// ErrOpen is returned by Allow while the breaker is open and the
	// open duration has not yet elapsed.
	ErrOpen = errors.New("circuitbreaker: breaker is open")
	// ErrHalfOpenBusy is returned by Allow while a half-open probe is
	// already in flight.
	ErrHalfOpenBusy = errors.New("circuitbreaker: half-open probe already in flight")
	// ErrUnknownToken is returned by Report for a token that was never
	// issued or has already been reported.
	ErrUnknownToken = errors.New("circuitbreaker: unknown or already reported token")
	// ErrStaleToken is returned by Report for a token issued in a
	// previous generation (before the last state transition). It is
	// consumed but has no effect on the breaker state.
	ErrStaleToken = errors.New("circuitbreaker: stale token from a previous generation")
)

// Clock abstracts the current time so tests can inject a fake clock.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// Snapshot is an independent copy of the breaker's observable state.
type Snapshot struct {
	// State is the current breaker state.
	State State
	// ConsecutiveFailures is the current count of consecutive failures
	// in the Closed state.
	ConsecutiveFailures int
	// Outstanding is the number of unreported tokens still valid in the
	// current generation.
	Outstanding int
	// Probing reports whether a half-open probe is currently in flight.
	Probing bool
}

// Breaker is a concurrent in-memory circuit breaker. The zero value is
// not usable; construct with New or NewWithClock.
type Breaker struct {
	mu           sync.Mutex
	clock        Clock
	threshold    int
	openDuration time.Duration
	state        State
	consecFails  int
	generation   uint64
	nextToken    uint64
	outstanding  map[uint64]uint64 // token -> generation it was issued in
	openedAt     time.Time
}

// New creates a Breaker using the real system clock. failureThreshold
// must be a positive integer and openDuration must be positive.
func New(failureThreshold int, openDuration time.Duration) *Breaker {
	return NewWithClock(failureThreshold, openDuration, realClock{})
}

// NewWithClock creates a Breaker with an injectable clock, for tests and
// deterministic demos. It panics on invalid arguments.
func NewWithClock(failureThreshold int, openDuration time.Duration, clock Clock) *Breaker {
	if failureThreshold <= 0 {
		panic("circuitbreaker: failureThreshold must be a positive integer")
	}
	if openDuration <= 0 {
		panic("circuitbreaker: openDuration must be positive")
	}
	if clock == nil {
		panic("circuitbreaker: clock must not be nil")
	}
	return &Breaker{
		clock:        clock,
		threshold:    failureThreshold,
		openDuration: openDuration,
		state:        Closed,
		outstanding:  make(map[uint64]uint64),
	}
}

// Allow asks permission to perform a request. In Closed state it returns
// a unique non-zero token that must later be passed to Report exactly
// once. In Open state it fails with ErrOpen until the open duration
// elapses; the first Allow after that atomically switches to HalfOpen and
// returns the single probe token. Further calls while HalfOpen fail with
// ErrHalfOpenBusy.
func (b *Breaker) Allow() (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Closed:
		return b.issueLocked(), nil
	case Open:
		if b.clock.Now().Sub(b.openedAt) < b.openDuration {
			return 0, ErrOpen
		}
		b.transitionLocked(HalfOpen)
		return b.issueLocked(), nil
	default: // HalfOpen
		return 0, ErrHalfOpenBusy
	}
}

// Report completes a request previously admitted by Allow. A token can be
// reported at most once; repeated or unknown tokens return
// ErrUnknownToken. Tokens issued before the latest state transition are
// stale: they return ErrStaleToken and never affect the new state.
//
// In Closed state, a success resets the consecutive failure count and a
// failure increments it, tripping to Open at the threshold. In HalfOpen
// state, a probe success closes the breaker and resets the count, while a
// probe failure reopens it and restarts the cooldown.
func (b *Breaker) Report(token uint64, success bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	gen, ok := b.outstanding[token]
	if !ok {
		return ErrUnknownToken
	}
	delete(b.outstanding, token)
	if gen != b.generation {
		return ErrStaleToken
	}
	switch b.state {
	case Closed:
		if success {
			b.consecFails = 0
		} else {
			b.consecFails++
			if b.consecFails >= b.threshold {
				b.transitionLocked(Open)
			}
		}
	case HalfOpen:
		if success {
			b.consecFails = 0
			b.transitionLocked(Closed)
		} else {
			b.transitionLocked(Open)
		}
	}
	return nil
}

// State returns the current breaker state.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Snapshot returns an independent copy of the breaker's observable state.
func (b *Breaker) Snapshot() Snapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	snap := Snapshot{
		State:               b.state,
		ConsecutiveFailures: b.consecFails,
		Probing:             b.state == HalfOpen,
	}
	for _, gen := range b.outstanding {
		if gen == b.generation {
			snap.Outstanding++
		}
	}
	return snap
}

// issueLocked returns a fresh unique non-zero token. Caller holds b.mu.
func (b *Breaker) issueLocked() uint64 {
	b.nextToken++
	b.outstanding[b.nextToken] = b.generation
	return b.nextToken
}

// transitionLocked switches state and bumps the generation so that all
// previously issued tokens become stale. Caller holds b.mu.
func (b *Breaker) transitionLocked(s State) {
	b.state = s
	b.generation++
	if s == Open {
		b.openedAt = b.clock.Now()
	}
}
