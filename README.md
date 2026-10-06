# go-concurrent-circuit-breaker

A concurrent, in-memory circuit breaker library in Go with zero third-party
dependencies. Ships a minimal CLI demo and automated tests; no network, no
background goroutines, no persistence.

## Layout

- `breaker/` - the library (state machine, tokens, clock)
- `cmd/demo/` - scripted CLI demo driven by a manual clock
- `breaker/breaker_test.go` - tests, all on a fake clock (no real sleeps)

## Usage

```go
b, err := breaker.New(3, 30*time.Second)        // uses time.Now
b, err := breaker.NewWithClock(3, 30*time.Second, myClock) // injectable clock

token, err := b.Allow()   // admit a request
if err != nil { /* ErrCircuitOpen or ErrProbeInFlight: reject */ }
err = b.Report(token, ok) // exactly once per token

state := b.State()
snap := b.Snapshot()      // State, ConsecutiveFailures, OutstandingRequests, HalfOpenProbing
```

`failureThreshold` must be a positive integer, `openDuration` positive.

## Semantics

- **Closed**: every `Allow` returns a unique non-zero token. Reports are
  counted in completion order: a success resets the consecutive-failure
  count; reaching `failureThreshold` failures trips the breaker Open.
- **Open**: `Allow` fails with `ErrCircuitOpen`. After `openDuration`
  the first `Allow` atomically switches to HalfOpen and issues the single
  probe token; all other concurrent callers get `ErrProbeInFlight`.
- **HalfOpen**: probe success closes the breaker and resets the count;
  probe failure re-opens it and restarts the cooldown.
- **Tokens** encode a generation that is bumped on every state transition.
  Tokens left over from a previous cycle are stale: `Report` rejects them
  with `ErrTokenStale` and they can never pollute the new state. Repeated
  reports return `ErrTokenAlreadyReported`; never-issued tokens return
  `ErrTokenUnknown`.
- All methods are safe for concurrent use (single mutex, no goroutines).

## Run

```sh
go run ./cmd/demo        # scripted walkthrough, manual clock, no sleeps
go test -race ./...      # 11 tests incl. a basic concurrency scenario
```
