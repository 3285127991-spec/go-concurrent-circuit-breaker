// Command demo walks through one full circuit-breaker cycle with a
// controllable manual clock: failures accumulate in Closed, the breaker
// trips Open, the cooldown leads to HalfOpen, a successful probe recovers
// to Closed, and a stale token from the previous cycle is rejected.
package main

import (
	"errors"
	"fmt"
	"time"

	"example.com/circuitbreaker/breaker"
)

func must(label string, err error) {
	if err != nil {
		panic(fmt.Sprintf("%s: %v", label, err))
	}
}

func main() {
	clock := breaker.NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	b, err := breaker.NewWithClock(2, 30*time.Second, clock)
	must("NewWithClock", err)

	show := func(format string, args ...any) {
		fmt.Printf(format+"\n", args...)
		s := b.Snapshot()
		fmt.Printf("    snapshot: state=%s consecutiveFailures=%d outstanding=%d probing=%t\n",
			s.State, s.ConsecutiveFailures, s.OutstandingRequests, s.HalfOpenProbing)
	}

	fmt.Println("== Closed: failures accumulate ==")
	t1, err := b.Allow()
	must("allow t1", err)
	t2, err := b.Allow()
	must("allow t2", err)
	stale, err := b.Allow()
	must("allow stale", err)
	must("report t1", b.Report(t1, false))
	show("reported t1=fail (1/2)")
	must("report t2", b.Report(t2, false))
	show("reported t2=fail (2/2) -> threshold reached, breaker trips Open")

	fmt.Println()
	fmt.Println("== Open: requests rejected until cooldown ==")
	if _, err := b.Allow(); errors.Is(err, breaker.ErrCircuitOpen) {
		show("allow rejected: %v", err)
	}

	fmt.Println()
	fmt.Println("== Cooldown elapsed: first Allow becomes the HalfOpen probe ==")
	clock.Advance(30 * time.Second)
	probe, err := b.Allow()
	must("allow probe", err)
	show("allow returned probe token %d", probe)
	if _, err := b.Allow(); errors.Is(err, breaker.ErrProbeInFlight) {
		show("concurrent allow rejected: %v", err)
	}

	fmt.Println()
	fmt.Println("== Stale token from the previous cycle is rejected ==")
	if err := b.Report(stale, true); errors.Is(err, breaker.ErrTokenStale) {
		show("report old token rejected: %v", err)
	}

	fmt.Println()
	fmt.Println("== Probe succeeds: breaker closes again ==")
	must("report probe", b.Report(probe, true))
	show("reported probe=success -> Closed, failures reset")

	fmt.Println()
	fmt.Println("== Duplicate report in the same cycle is rejected ==")
	t3, err := b.Allow()
	must("allow t3", err)
	must("report t3", b.Report(t3, true))
	if err := b.Report(t3, true); errors.Is(err, breaker.ErrTokenAlreadyReported) {
		show("re-reporting t3 rejected: %v", err)
	}
}
