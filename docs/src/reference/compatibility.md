# Compatibility

## Common Lisp systems

The library is distributed as the ASDF system `cl-resilience-kit`.

Runtime dependencies declared by the system are:

- `cl-boundary-kit`
- `cl-concurrent-kit`
- `cl-date-kit`

The test system additionally depends on `cl-weave`. The implementation keeps
transport and protocol integrations outside the core system.

## Flake systems

The repository's flake currently declares these build systems:

- `x86_64-linux`
- `aarch64-darwin`

Use the Nix outputs for the supported platform rather than assuming that a
local direct-SBCL invocation has the same dependency resolution or runtime
environment.

## Timeouts and cancellation

Deadlines, cancellation, backoff sleeps, limiter refills, breaker reset windows,
and lease expiry are cooperative and injectable. They do not forcibly stop
arbitrary Lisp code. `resilience-executor-call` has a separate best-effort
worker-side `hard-timeout`; callers must still define cleanup and side-effect
behavior for interrupted work.

## Shared state

Memory state and lease stores are process-local. A deployment that spans
processes must supply implementations of the state-store and lease-store
contracts backed by a shared system with the required atomic operations.

## Testing implications

The test suite uses fake monotonic time, injected random sources, recording
sleepers, and `cl-weave`. Keep those boundaries injectable in integrations so
retry, deadline, breaker, limiter, and cancellation behavior can be tested
without relying on wall-clock sleeps.
