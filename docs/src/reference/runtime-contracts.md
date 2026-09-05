# Runtime contracts

This page describes the current runtime contract. The documented API is the
only supported contract; integrations should depend on exported symbols and
the explicit boundaries described here.

## Common Lisp systems

The library is distributed as the ASDF system `cl-resilience-kit`.

Its runtime dependencies are versioned directly in the system definition:

- `cl-boundary-kit` 2.3.0 or newer;
- `cl-concurrent-kit` 0.6.1 or newer;
- `cl-date-kit` 1.0.0 or newer.

The optional `cl-resilience-kit/observability` system adds direct metrics
integration with `cl-observability-kit`, and the optional
`cl-resilience-kit/dataflow` system adds direct pipeline integration with
`cl-dataflow-kit`. The test system additionally uses `cl-weave`; the production
system does not load the test framework.

## Flake systems

The repository's flake declares these build systems:

- `x86_64-linux`;
- `aarch64-darwin`.

Use the Nix outputs for the supported platform rather than assuming that a
direct local SBCL invocation has the same dependency resolution or runtime
environment.

## Timeouts and cancellation

Deadlines, cancellation, backoff sleeps, limiter refills, breaker reset
windows, and lease expiry are cooperative and injectable.  They do not
forcibly stop arbitrary Lisp code.  `resilience-executor-call` has a separate
best-effort worker-side `hard-timeout`; callers still define cleanup and
side-effect behavior for interrupted work.

## Shared state

Memory state and lease stores are process-local.  A deployment that spans
processes supplies state-store and lease-store implementations backed by a
shared system with the required atomic operations.

## Integration boundaries

Clocks, monotonic unit scales, random sources, sleepers, cancellation tokens,
event handlers, state stores, and lease stores are explicit operation
boundaries.  Keep those boundaries injectable when integrating transports,
workers, or observability systems so behavior remains deterministic and
side effects remain visible to the caller.

## Test boundaries

The test suite uses fake monotonic time, injected random sources, recording
sleepers, `cl-weave` property/concurrency contracts, and continuation-aware
fixtures.  Integrations should use the same boundary objects instead of
relying on wall-clock sleeps or process-global state.
