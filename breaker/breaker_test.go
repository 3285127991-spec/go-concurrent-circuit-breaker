package breaker

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestBreaker(t *testing.T, threshold int, open time.Duration) (*Breaker, *ManualClock) {
	t.Helper()
	clock := NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	b, err := NewWithClock(threshold, open, clock)
	if err != nil {
		t.Fatalf("NewWithClock(%d, %s): %v", threshold, open, err)
	}
	return b, clock
}

func mustAllow(t *testing.T, b *Breaker) Token {
	t.Helper()
	tok, err := b.Allow()
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	return tok
}

func mustReport(t *testing.T, b *Breaker, tok Token, success bool) {
	t.Helper()
	if err := b.Report(tok, success); err != nil {
		t.Fatalf("Report(%d, %t): %v", tok, success, err)
	}
}

// trip drives the breaker Open with threshold failures.
func trip(t *testing.T, b *Breaker, threshold int) {
	t.Helper()
	for i := 0; i < threshold; i++ {
		tok := mustAllow(t, b)
		mustReport(t, b, tok, false)
	}
	if got := b.State(); got != Open {
		t.Fatalf("State after %d failures = %s, want Open", threshold, got)
	}
}

func TestNewValidatesParams(t *testing.T) {
	clock := NewManualClock(time.Now())
	cases := []struct {
		name      string
		threshold int
		open      time.Duration
		clock     Clock
	}{
		{"zero threshold", 0, time.Second, clock},
		{"negative threshold", -2, time.Second, clock},
		{"zero open duration", 1, 0, clock},
		{"negative open duration", 1, -time.Second, clock},
		{"nil clock", 1, time.Second, nil},
	}
	for _, tc := range cases {
		if _, err := NewWithClock(tc.threshold, tc.open, tc.clock); err == nil {
			t.Errorf("%s: expected error, got nil", tc.name)
		}
	}
	if _, err := New(2, time.Second); err != nil {
		t.Errorf("New with valid params: %v", err)
	}
}

func TestAllowReportSuccess(t *testing.T) {
	b, _ := newTestBreaker(t, 2, time.Minute)

	t1 := mustAllow(t, b)
	t2 := mustAllow(t, b)
	if t1 == 0 || t2 == 0 {
		t.Fatalf("tokens must be non-zero, got %d and %d", t1, t2)
	}
	if t1 == t2 {
		t.Fatalf("tokens must be unique, both are %d", t1)
	}
	mustReport(t, b, t1, true)
	mustReport(t, b, t2, true)

	if got := b.Snapshot(); got.State != Closed || got.ConsecutiveFailures != 0 || got.OutstandingRequests != 0 {
		t.Fatalf("Snapshot = %+v, want clean Closed", got)
	}

	if err := b.Report(0, true); !errors.Is(err, ErrTokenUnknown) {
		t.Errorf("Report(zero token) = %v, want ErrTokenUnknown", err)
	}
	neverIssued := makeToken(1, 1<<20)
	if err := b.Report(neverIssued, true); !errors.Is(err, ErrTokenUnknown) {
		t.Errorf("Report(never issued) = %v, want ErrTokenUnknown", err)
	}
}

func TestSuccessResetsConsecutiveFailures(t *testing.T) {
	b, _ := newTestBreaker(t, 3, time.Minute)

	fail := func() { tok := mustAllow(t, b); mustReport(t, b, tok, false) }
	succeed := func() { tok := mustAllow(t, b); mustReport(t, b, tok, true) }

	fail()
	fail()
	if got := b.Snapshot().ConsecutiveFailures; got != 2 {
		t.Fatalf("ConsecutiveFailures = %d, want 2", got)
	}
	succeed()
	if got := b.Snapshot().ConsecutiveFailures; got != 0 {
		t.Fatalf("ConsecutiveFailures after success = %d, want 0", got)
	}
	fail()
	fail()
	if got := b.State(); got != Closed {
		t.Fatalf("State = %s, want Closed (2 < threshold 3)", got)
	}
}

func TestFailuresTripOpen(t *testing.T) {
	b, _ := newTestBreaker(t, 3, time.Minute)

	for i := 1; i <= 2; i++ {
		tok := mustAllow(t, b)
		mustReport(t, b, tok, false)
		if got := b.State(); got != Closed {
			t.Fatalf("State after %d failures = %s, want Closed", i, got)
		}
	}
	tok := mustAllow(t, b)
	mustReport(t, b, tok, false)
	if got := b.State(); got != Open {
		t.Fatalf("State after 3 failures = %s, want Open", got)
	}
}

func TestOpenRejectsUntilCooldown(t *testing.T) {
	b, clock := newTestBreaker(t, 1, 30*time.Second)
	trip(t, b, 1)

	if _, err := b.Allow(); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("Allow in Open = %v, want ErrCircuitOpen", err)
	}
	clock.Advance(29 * time.Second)
	if _, err := b.Allow(); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("Allow before cooldown = %v, want ErrCircuitOpen", err)
	}
	clock.Advance(1 * time.Second) // exactly 30s: cooldown elapsed
	if _, err := b.Allow(); err != nil {
		t.Fatalf("Allow at cooldown boundary = %v, want probe token", err)
	}
	if got := b.State(); got != HalfOpen {
		t.Fatalf("State = %s, want HalfOpen", got)
	}
}

func TestHalfOpenSingleProbe(t *testing.T) {
	b, clock := newTestBreaker(t, 1, 30*time.Second)
	trip(t, b, 1)
	clock.Advance(30 * time.Second)

	probe := mustAllow(t, b)
	if probe == 0 {
		t.Fatal("probe token must be non-zero")
	}
	if _, err := b.Allow(); !errors.Is(err, ErrProbeInFlight) {
		t.Fatalf("second Allow in HalfOpen = %v, want ErrProbeInFlight", err)
	}
	if got := b.Snapshot(); !got.HalfOpenProbing || got.OutstandingRequests != 1 {
		t.Fatalf("Snapshot = %+v, want probing with 1 outstanding", got)
	}
}

func TestProbeSuccessCloses(t *testing.T) {
	b, clock := newTestBreaker(t, 2, 30*time.Second)
	trip(t, b, 2)
	clock.Advance(30 * time.Second)

	probe := mustAllow(t, b)
	mustReport(t, b, probe, true)

	snap := b.Snapshot()
	if snap.State != Closed || snap.ConsecutiveFailures != 0 || snap.HalfOpenProbing {
		t.Fatalf("Snapshot = %+v, want Closed with reset counters", snap)
	}
	if _, err := b.Allow(); err != nil {
		t.Fatalf("Allow after recovery = %v, want nil", err)
	}
}

func TestProbeFailureReopens(t *testing.T) {
	b, clock := newTestBreaker(t, 1, 30*time.Second)
	trip(t, b, 1)
	clock.Advance(30 * time.Second)

	probe := mustAllow(t, b)
	mustReport(t, b, probe, false)

	if got := b.State(); got != Open {
		t.Fatalf("State after failed probe = %s, want Open", got)
	}
	if _, err := b.Allow(); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("Allow after failed probe = %v, want ErrCircuitOpen", err)
	}
	clock.Advance(29 * time.Second)
	if _, err := b.Allow(); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("Allow before second cooldown = %v, want ErrCircuitOpen", err)
	}
	clock.Advance(1 * time.Second)
	if _, err := b.Allow(); err != nil {
		t.Fatalf("Allow after second cooldown = %v, want new probe", err)
	}
}

func TestTokenErrors(t *testing.T) {
	t.Run("duplicate report", func(t *testing.T) {
		b, _ := newTestBreaker(t, 2, time.Minute)
		tok := mustAllow(t, b)
		mustReport(t, b, tok, true)
		if err := b.Report(tok, true); !errors.Is(err, ErrTokenAlreadyReported) {
			t.Fatalf("second Report = %v, want ErrTokenAlreadyReported", err)
		}
	})

	t.Run("stale after trip", func(t *testing.T) {
		b, _ := newTestBreaker(t, 2, time.Minute)
		victim := mustAllow(t, b) // left outstanding while the breaker trips
		trip(t, b, 2)
		if err := b.Report(victim, true); !errors.Is(err, ErrTokenStale) {
			t.Fatalf("Report(old token) = %v, want ErrTokenStale", err)
		}
		if got := b.State(); got != Open {
			t.Fatalf("stale report changed state to %s, want Open", got)
		}
	})

	t.Run("stale after full cycle", func(t *testing.T) {
		b, clock := newTestBreaker(t, 1, 30*time.Second)
		victim := mustAllow(t, b)
		trip(t, b, 1)
		clock.Advance(30 * time.Second)
		probe := mustAllow(t, b)
		mustReport(t, b, probe, true) // back to Closed, new generation
		if err := b.Report(victim, false); !errors.Is(err, ErrTokenStale) {
			t.Fatalf("Report(ancient token) = %v, want ErrTokenStale", err)
		}
		if got := b.Snapshot(); got.State != Closed || got.ConsecutiveFailures != 0 {
			t.Fatalf("stale report polluted state: %+v", got)
		}
	})
}

func TestSnapshot(t *testing.T) {
	b, clock := newTestBreaker(t, 2, 30*time.Second)

	want := Snapshot{State: Closed, ConsecutiveFailures: 0, OutstandingRequests: 0, HalfOpenProbing: false}
	if got := b.Snapshot(); got != want {
		t.Fatalf("initial Snapshot = %+v, want %+v", got, want)
	}

	pending := mustAllow(t, b)
	if got := b.Snapshot(); got.OutstandingRequests != 1 {
		t.Fatalf("OutstandingRequests = %d, want 1", got.OutstandingRequests)
	}
	mustReport(t, b, pending, false)
	if got := b.Snapshot(); got.ConsecutiveFailures != 1 || got.OutstandingRequests != 0 {
		t.Fatalf("Snapshot after failure = %+v", got)
	}

	tok := mustAllow(t, b)
	mustReport(t, b, tok, false) // trips Open
	if got := b.Snapshot(); got.State != Open || got.ConsecutiveFailures != 2 || got.OutstandingRequests != 0 {
		t.Fatalf("Snapshot in Open = %+v, want Open/2/0", got)
	}

	clock.Advance(30 * time.Second)
	mustAllow(t, b)
	if got := b.Snapshot(); got.State != HalfOpen || !got.HalfOpenProbing || got.OutstandingRequests != 1 {
		t.Fatalf("Snapshot in HalfOpen = %+v, want probing with 1 outstanding", got)
	}
}

func TestConcurrentAccess(t *testing.T) {
	b, clock := newTestBreaker(t, 4, time.Second)

	// Phase 1: many goroutines allow+report successfully; the breaker must
	// stay Closed and every report must succeed exactly once.
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				tok, err := b.Allow()
				if err != nil {
					t.Errorf("Allow: %v", err)
					return
				}
				if err := b.Report(tok, true); err != nil {
					t.Errorf("Report: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if got := b.State(); got != Closed {
		t.Fatalf("State after concurrent success = %s, want Closed", got)
	}

	// Phase 2: trip the breaker, cool down, then race Allows; exactly one
	// goroutine may win the HalfOpen probe.
	trip(t, b, 4)
	clock.Advance(time.Second)

	const racers = 16
	start := make(chan struct{})
	var won atomic.Int32
	var rejected atomic.Int32
	for g := 0; g < racers; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := b.Allow()
			switch {
			case err == nil:
				won.Add(1)
			case errors.Is(err, ErrProbeInFlight):
				rejected.Add(1)
			default:
				t.Errorf("unexpected Allow error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := won.Load(); got != 1 {
		t.Fatalf("probe winners = %d, want exactly 1", got)
	}
	if got := rejected.Load(); got != racers-1 {
		t.Fatalf("rejected racers = %d, want %d", got, racers-1)
	}
}
