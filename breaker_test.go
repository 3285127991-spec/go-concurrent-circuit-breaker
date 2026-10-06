package circuitbreaker

import (
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newTestBreaker(threshold int, openDur time.Duration) (*Breaker, *fakeClock) {
	clock := newFakeClock()
	return NewWithClock(threshold, openDur, clock), clock
}

func mustAllow(t *testing.T, b *Breaker) uint64 {
	t.Helper()
	token, err := b.Allow()
	if err != nil {
		t.Fatalf("Allow() error = %v, want nil", err)
	}
	if token == 0 {
		t.Fatal("Allow() returned zero token")
	}
	return token
}

func mustReport(t *testing.T, b *Breaker, token uint64, success bool) {
	t.Helper()
	if err := b.Report(token, success); err != nil {
		t.Fatalf("Report(%d, %v) error = %v, want nil", token, success, err)
	}
}

func TestAllowIssuesUniqueNonZeroTokens(t *testing.T) {
	b, _ := newTestBreaker(3, time.Second)
	seen := make(map[uint64]bool)
	for i := 0; i < 100; i++ {
		token := mustAllow(t, b)
		if seen[token] {
			t.Fatalf("duplicate token %d", token)
		}
		seen[token] = true
	}
}

func TestReportSuccessResetsConsecutiveFailures(t *testing.T) {
	b, _ := newTestBreaker(3, time.Second)
	mustReport(t, b, mustAllow(t, b), false)
	mustReport(t, b, mustAllow(t, b), false)
	mustReport(t, b, mustAllow(t, b), true)
	if got := b.Snapshot().ConsecutiveFailures; got != 0 {
		t.Fatalf("ConsecutiveFailures = %d, want 0", got)
	}
	// Two more failures must not trip the breaker: the count restarted.
	mustReport(t, b, mustAllow(t, b), false)
	mustReport(t, b, mustAllow(t, b), false)
	if got := b.State(); got != Closed {
		t.Fatalf("State = %v, want Closed", got)
	}
}

func TestConsecutiveFailuresTripOpen(t *testing.T) {
	b, _ := newTestBreaker(3, time.Second)
	for i := 0; i < 2; i++ {
		mustReport(t, b, mustAllow(t, b), false)
	}
	if got := b.State(); got != Closed {
		t.Fatalf("State after 2 failures = %v, want Closed", got)
	}
	mustReport(t, b, mustAllow(t, b), false)
	if got := b.State(); got != Open {
		t.Fatalf("State after 3 failures = %v, want Open", got)
	}
}

func TestOpenRejectsAllowUntilCooldown(t *testing.T) {
	b, clock := newTestBreaker(1, 10*time.Second)
	mustReport(t, b, mustAllow(t, b), false)
	if _, err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatalf("Allow() error = %v, want ErrOpen", err)
	}
	clock.Advance(9 * time.Second)
	if _, err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatalf("Allow() before cooldown error = %v, want ErrOpen", err)
	}
}

func TestHalfOpenAllowsSingleProbe(t *testing.T) {
	b, clock := newTestBreaker(1, 10*time.Second)
	mustReport(t, b, mustAllow(t, b), false)
	clock.Advance(10 * time.Second)
	probe := mustAllow(t, b)
	if got := b.State(); got != HalfOpen {
		t.Fatalf("State = %v, want HalfOpen", got)
	}
	if _, err := b.Allow(); !errors.Is(err, ErrHalfOpenBusy) {
		t.Fatalf("second Allow() error = %v, want ErrHalfOpenBusy", err)
	}
	mustReport(t, b, probe, true)
}

func TestProbeSuccessClosesAndResets(t *testing.T) {
	b, clock := newTestBreaker(2, 10*time.Second)
	mustReport(t, b, mustAllow(t, b), false)
	mustReport(t, b, mustAllow(t, b), false)
	clock.Advance(10 * time.Second)
	mustReport(t, b, mustAllow(t, b), true)
	snap := b.Snapshot()
	if snap.State != Closed || snap.ConsecutiveFailures != 0 || snap.Probing {
		t.Fatalf("Snapshot = %+v, want Closed with 0 failures and no probe", snap)
	}
	// Breaker is usable again: one failure must not reopen it.
	mustReport(t, b, mustAllow(t, b), false)
	if got := b.State(); got != Closed {
		t.Fatalf("State = %v, want Closed", got)
	}
}

func TestProbeFailureReopensAndRestartsCooldown(t *testing.T) {
	b, clock := newTestBreaker(1, 10*time.Second)
	mustReport(t, b, mustAllow(t, b), false)
	clock.Advance(10 * time.Second)
	mustReport(t, b, mustAllow(t, b), false) // probe fails
	if got := b.State(); got != Open {
		t.Fatalf("State = %v, want Open", got)
	}
	// Cooldown restarts from the probe failure, not the first opening.
	clock.Advance(9 * time.Second)
	if _, err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatalf("Allow() error = %v, want ErrOpen", err)
	}
	clock.Advance(time.Second)
	if _, err := b.Allow(); err != nil {
		t.Fatalf("Allow() after restarted cooldown error = %v, want nil", err)
	}
	if got := b.State(); got != HalfOpen {
		t.Fatalf("State = %v, want HalfOpen", got)
	}
}

func TestDuplicateAndUnknownTokenRejected(t *testing.T) {
	b, _ := newTestBreaker(3, time.Second)
	token := mustAllow(t, b)
	mustReport(t, b, token, true)
	if err := b.Report(token, true); !errors.Is(err, ErrUnknownToken) {
		t.Fatalf("second Report error = %v, want ErrUnknownToken", err)
	}
	if err := b.Report(999999, false); !errors.Is(err, ErrUnknownToken) {
		t.Fatalf("unknown Report error = %v, want ErrUnknownToken", err)
	}
}

func TestStaleTokenRejectedAndCannotAffectState(t *testing.T) {
	b, clock := newTestBreaker(2, 10*time.Second)
	stale := mustAllow(t, b) // held across the transition to Open
	mustReport(t, b, mustAllow(t, b), false)
	mustReport(t, b, mustAllow(t, b), false) // trips Open
	if err := b.Report(stale, true); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("Report(stale) error = %v, want ErrStaleToken", err)
	}
	if got := b.State(); got != Open {
		t.Fatalf("State = %v, want Open (stale token must not change state)", got)
	}
	// A stale token is consumed: reporting it again is unknown, and it
	// stays stale in the next generation too.
	if err := b.Report(stale, true); !errors.Is(err, ErrUnknownToken) {
		t.Fatalf("second Report(stale) error = %v, want ErrUnknownToken", err)
	}
	clock.Advance(10 * time.Second)
	probe := mustAllow(t, b)
	mustReport(t, b, probe, true)
	if got := b.State(); got != Closed {
		t.Fatalf("State = %v, want Closed", got)
	}
}

func TestSnapshot(t *testing.T) {
	b, clock := newTestBreaker(3, 10*time.Second)
	pending := mustAllow(t, b)
	mustReport(t, b, mustAllow(t, b), false)
	snap := b.Snapshot()
	if snap.State != Closed || snap.ConsecutiveFailures != 1 || snap.Outstanding != 1 || snap.Probing {
		t.Fatalf("Snapshot = %+v, want {Closed 1 1 false}", snap)
	}
	mustReport(t, b, mustAllow(t, b), false)
	mustReport(t, b, mustAllow(t, b), false) // trips Open; pending is stale
	snap = b.Snapshot()
	if snap.State != Open || snap.Outstanding != 0 {
		t.Fatalf("Snapshot = %+v, want Open with 0 current-generation outstanding", snap)
	}
	clock.Advance(10 * time.Second)
	mustAllow(t, b)
	snap = b.Snapshot()
	if snap.State != HalfOpen || !snap.Probing || snap.Outstanding != 1 {
		t.Fatalf("Snapshot = %+v, want HalfOpen probing with 1 outstanding", snap)
	}
	_ = pending
}

func TestConcurrentAllowReportAndProbeRace(t *testing.T) {
	b, clock := newTestBreaker(1, 10*time.Second)
	mustReport(t, b, mustAllow(t, b), false) // trips Open
	clock.Advance(10 * time.Second)

	// Exactly one of many concurrent Allow calls wins the probe.
	const contenders = 32
	tokens := make(chan uint64, contenders)
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if token, err := b.Allow(); err == nil {
				tokens <- token
			}
		}()
	}
	wg.Wait()
	close(tokens)
	var won []uint64
	for token := range tokens {
		won = append(won, token)
	}
	if len(won) != 1 {
		t.Fatalf("%d goroutines won the probe, want exactly 1", len(won))
	}
	mustReport(t, b, won[0], true)

	// Concurrent Allow+Report in Closed state: all tokens unique.
	const workers = 64
	reported := make(chan uint64, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			token, err := b.Allow()
			if err != nil {
				t.Errorf("Allow() error = %v, want nil", err)
				return
			}
			if err := b.Report(token, true); err != nil {
				t.Errorf("Report() error = %v, want nil", err)
			}
			reported <- token
		}()
	}
	wg.Wait()
	close(reported)
	seen := make(map[uint64]bool)
	for token := range reported {
		if seen[token] {
			t.Errorf("duplicate token %d", token)
		}
		seen[token] = true
	}
	if len(seen) != workers {
		t.Fatalf("got %d unique tokens, want %d", len(seen), workers)
	}
	snap := b.Snapshot()
	if snap.State != Closed || snap.ConsecutiveFailures != 0 || snap.Outstanding != 0 {
		t.Fatalf("Snapshot = %+v, want {Closed 0 0 false}", snap)
	}
}
