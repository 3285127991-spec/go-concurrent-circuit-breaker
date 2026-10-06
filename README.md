# circuitbreaker

A concurrent, in-memory circuit breaker library for Go. No third-party
dependencies, no background goroutines, no network, no persistence.

## Features

- Three states: `Closed`, `Open`, `HalfOpen`
- Token-based request tracking: `Allow` returns a unique non-zero token,
  `Report(token, success)` completes it exactly once
- Generation (epoch) isolation: tokens issued before a state transition
  become stale and can never pollute the new state
- Atomic Open -> HalfOpen transition with exactly one probe request
- Safe for concurrent use from multiple goroutines (verified with
  `go test -race`)
- Injectable `Clock` for deterministic tests and demos

## Usage

```go
b := circuitbreaker.New(3, 10*time.Second) // threshold, open duration

token, err := b.Allow()
if err != nil {
    // ErrOpen (cooling down) or ErrHalfOpenBusy (probe in flight)
    return
}
err = b.Report(token, doRequest() == nil)
// err is ErrUnknownToken (reused/unknown token) or ErrStaleToken
// (token from a previous generation); nil on success.

state := b.State()
snap := b.Snapshot() // {State, ConsecutiveFailures, Outstanding, Probing}
```

For tests, inject a fake clock:

```go
b := circuitbreaker.NewWithClock(3, 10*time.Second, myFakeClock)
```

## Behavior

- **Closed**: all requests allowed. Consecutive failures (counted in
  Report completion order) trip the breaker Open at the threshold; any
  success resets the count to zero.
- **Open**: `Allow` fails with `ErrOpen`. The first `Allow` after
  `openDuration` has elapsed atomically switches to HalfOpen and hands
  out the single probe token.
- **HalfOpen**: only the probe is in flight; other `Allow` calls fail
  with `ErrHalfOpenBusy`. Probe success closes the breaker and resets
  the failure count; probe failure reopens it and restarts the cooldown.
- Tokens still unreported when a transition happens become stale:
  reporting them returns `ErrStaleToken` and has no effect on state.

## Demo

```sh
go run ./cmd/demo
```

Walks through the full lifecycle with a manual clock (no real sleeping):
failure accumulation, Open rejection, stale-token rejection, HalfOpen
probe, and recovery to Closed.

## Test

```sh
go test -race ./...
```

All tests use a fake clock; none sleep.
