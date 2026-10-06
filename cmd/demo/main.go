// Command demo walks through the circuit breaker lifecycle with a
// controllable manual clock: Closed accumulates failures, the breaker
// trips Open, stale tokens are rejected, the cooldown elapses, a single
// HalfOpen probe runs, and its success closes the breaker again.
package main

import (
	"fmt"
	"time"

	"circuitbreaker"
)

// manualClock is a deterministic clock: time only moves when Advance is
// called, so the demo never sleeps.
type manualClock struct{ now time.Time }

func (c *manualClock) Now() time.Time          { return c.now }
func (c *manualClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

func main() {
	clock := &manualClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	breaker := circuitbreaker.NewWithClock(3, 10*time.Second, clock)

	fmt.Println("== 1. Closed: accumulate consecutive failures ==")
	stale, _ := breaker.Allow() // held across the transition; becomes stale
	fmt.Printf("issued token %d (will be kept past the next transition)\n", stale)
	for i := 1; i <= 3; i++ {
		token, _ := breaker.Allow()
		_ = breaker.Report(token, false)
		snap := breaker.Snapshot()
		fmt.Printf("failure %d reported -> state=%s consecutiveFailures=%d\n",
			i, snap.State, snap.ConsecutiveFailures)
	}

	fmt.Println("\n== 2. Open: requests are rejected ==")
	if _, err := breaker.Allow(); err != nil {
		fmt.Printf("Allow() rejected: %v\n", err)
	}

	fmt.Println("\n== 3. Stale token from before the transition ==")
	if err := breaker.Report(stale, true); err != nil {
		fmt.Printf("Report(stale token %d) rejected: %v\n", stale, err)
	}
	fmt.Printf("state is still %s\n", breaker.State())

	fmt.Println("\n== 4. Cooldown elapses: first Allow becomes the HalfOpen probe ==")
	clock.Advance(10 * time.Second)
	probe, err := breaker.Allow()
	fmt.Printf("Allow() -> probe token %d (err=%v), state=%s\n", probe, err, breaker.State())
	if _, err := breaker.Allow(); err != nil {
		fmt.Printf("concurrent Allow() rejected: %v\n", err)
	}

	fmt.Println("\n== 5. Probe succeeds: breaker closes and counters reset ==")
	_ = breaker.Report(probe, true)
	snap := breaker.Snapshot()
	fmt.Printf("snapshot: state=%s consecutiveFailures=%d outstanding=%d probing=%v\n",
		snap.State, snap.ConsecutiveFailures, snap.Outstanding, snap.Probing)
}
