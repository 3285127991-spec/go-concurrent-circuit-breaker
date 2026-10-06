// Package breaker provides a concurrent in-memory circuit breaker.
//
// The breaker admits requests with Allow, which returns a unique non-zero
// token, and learns their outcome through Report. Consecutive failures trip
// the breaker Open; after a cooldown a single half-open probe decides
// whether to close again. Tokens are scoped to a generation that is bumped
// on every state transition, so tokens from a previous cycle are stale and
// can never affect the new state.
package breaker

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// State is the circuit breaker state.
type State int

const (
	// Closed admits all requests.
	Closed State = iota
	// Open rejects all requests until the cooldown elapses.
	Open
	// HalfOpen admits exactly one probe request.
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

// Token identifies an admitted request. Tokens are unique and non-zero.
// The zero value is never issued and is always rejected by Report.
type Token uint64

var (
	// ErrCircuitOpen is returned by Allow while the breaker is Open and the
	// cooldown has not yet elapsed.
	ErrCircuitOpen = errors.New("breaker: circuit open, request rejected")
	// ErrProbeInFlight is returned by Allow in HalfOpen when the single
	// probe slot is already taken.
	ErrProbeInFlight = errors.New("breaker: half-open probe already in flight, request rejected")
	// ErrTokenUnknown is returned by Report for a token that was never
	// issued in the current generation (including the zero token).
	ErrTokenUnknown = errors.New("breaker: unknown token")
	// ErrTokenAlreadyReported is returned by Report for a token of the
	// current generation that was already reported.
	ErrTokenAlreadyReported = errors.New("breaker: token already reported")
	// ErrTokenStale is returned by Report for a token issued in a previous
	// state cycle; it can no longer affect the breaker.
	ErrTokenStale = errors.New("breaker: stale token from a previous state cycle")
)

// Snapshot is an independent value copy of the breaker's observable state.
type Snapshot struct {
	State               State
	ConsecutiveFailures int
	// OutstandingRequests counts admitted requests of the current
	// generation that have not been reported yet.
	OutstandingRequests int
	// HalfOpenProbing reports whether the half-open probe is in flight.
	HalfOpenProbing bool
}

// Breaker is a concurrent in-memory circuit breaker. The zero value is not
// usable; construct with New or NewWithClock. All methods are safe for
// concurrent use.
type Breaker struct {
	clock            Clock
	failureThreshold int
	openDuration     time.Duration

	mu                  sync.Mutex
	state               State
	consecutiveFailures int
	generation          uint64
	issuedSeq           uint32
	outstanding         map[Token]struct{}
	openedAt            time.Time
	probing             bool
}

// New creates a Breaker using time.Now. failureThreshold must be a positive
// integer and openDuration must be positive.
func New(failureThreshold int, openDuration time.Duration) (*Breaker, error) {
	return NewWithClock(failureThreshold, openDuration, realClock{})
}

// NewWithClock creates a Breaker with an injectable clock, for tests and
// deterministic demos.
func NewWithClock(failureThreshold int, openDuration time.Duration, clock Clock) (*Breaker, error) {
	if failureThreshold <= 0 {
		return nil, fmt.Errorf("breaker: failureThreshold must be positive, got %d", failureThreshold)
	}
	if openDuration <= 0 {
		return nil, fmt.Errorf("breaker: openDuration must be positive, got %s", openDuration)
	}
	if clock == nil {
		return nil, errors.New("breaker: clock must not be nil")
	}
	return &Breaker{
		clock:            clock,
		failureThreshold: failureThreshold,
		openDuration:     openDuration,
		state:            Closed,
		generation:       1,
		outstanding:      make(map[Token]struct{}),
	}, nil
}

// Allow admits a request and returns its unique non-zero token. The caller
// must later report the outcome with Report exactly once.
//
// In Open, once the cooldown has elapsed the first Allow atomically switches
// the breaker to HalfOpen and returns the single probe token; concurrent
// callers are rejected.
func (b *Breaker) Allow() (Token, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case Open:
		if b.clock.Now().Sub(b.openedAt) < b.openDuration {
			return 0, ErrCircuitOpen
		}
		b.enterHalfOpenLocked()
		return b.issueLocked(true), nil
	case HalfOpen:
		return 0, ErrProbeInFlight
	default: // Closed
		return b.issueLocked(false), nil
	}
}

// Report records the outcome of the request admitted with token. A token
// can be reported successfully at most once; repeated, unknown, or stale
// tokens return a distinct error.
func (b *Breaker) Report(token Token, success bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	gen, seq := splitToken(token)
	if token == 0 || seq == 0 {
		return ErrTokenUnknown
	}
	if gen != b.generation {
		return ErrTokenStale
	}
	if _, ok := b.outstanding[token]; ok {
		delete(b.outstanding, token)
		b.applyResultLocked(success)
		return nil
	}
	if seq <= b.issuedSeq {
		return ErrTokenAlreadyReported
	}
	return ErrTokenUnknown
}

// State returns the current breaker state.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Snapshot returns an independent value copy of the current state,
// consecutive failure count, number of outstanding requests of the current
// generation, and whether a half-open probe is in flight.
func (b *Breaker) Snapshot() Snapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	return Snapshot{
		State:               b.state,
		ConsecutiveFailures: b.consecutiveFailures,
		OutstandingRequests: len(b.outstanding),
		HalfOpenProbing:     b.probing,
	}
}

// issueLocked mints the next token of the current generation.
func (b *Breaker) issueLocked(probe bool) Token {
	b.issuedSeq++
	token := makeToken(b.generation, b.issuedSeq)
	b.outstanding[token] = struct{}{}
	if probe {
		b.probing = true
	}
	return token
}

// applyResultLocked folds a completed request into the state machine.
func (b *Breaker) applyResultLocked(success bool) {
	switch b.state {
	case Closed:
		if success {
			b.consecutiveFailures = 0
			return
		}
		b.consecutiveFailures++
		if b.consecutiveFailures >= b.failureThreshold {
			b.enterOpenLocked()
		}
	case HalfOpen:
		if success {
			b.enterClosedLocked()
		} else {
			b.enterOpenLocked()
		}
	}
}

// transitionLocked bumps the generation and drops outstanding tokens so
// requests admitted in the previous cycle become stale.
func (b *Breaker) transitionLocked(state State) {
	b.state = state
	b.generation++
	b.issuedSeq = 0
	b.outstanding = make(map[Token]struct{})
	b.probing = false
}

func (b *Breaker) enterOpenLocked() {
	b.transitionLocked(Open)
	b.openedAt = b.clock.Now()
}

func (b *Breaker) enterHalfOpenLocked() {
	b.transitionLocked(HalfOpen)
	b.consecutiveFailures = 0
}

func (b *Breaker) enterClosedLocked() {
	b.transitionLocked(Closed)
	b.consecutiveFailures = 0
}

func makeToken(generation uint64, seq uint32) Token {
	return Token(generation<<32 | uint64(seq))
}

func splitToken(t Token) (generation uint64, seq uint32) {
	return uint64(t) >> 32, uint32(uint64(t))
}
