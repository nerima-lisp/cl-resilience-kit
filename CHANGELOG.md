# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0]

Initial public release.

### Added

- Retry policies with exponential backoff, delay caps, jitter, deadlines,
  cancellation, fallbacks, and shared retry budgets.
- Local and distributed circuit breakers with half-open probe limits,
  compare-and-swap state, and optional fencing leases.
- Non-blocking and queued bulkheads plus a monotonic token-bucket rate
  limiter.
- Explicit bounded executors, best-effort worker hard-timeouts, delayed
  hedging, and process-local request coalescing.
- Transport-neutral context with correlation, tracing, idempotency, tags,
  and baggage fields.
- Structured events, metrics, observers, lifecycle draining, and health
  registries.
- Explicit composition through `call-with-resilience`, `with-resilience`,
  and continuation-oriented `call-with-resilience/k` / `with-resilience/k`
  APIs; the latter preserve multiple values and keep callback failures
  visible.
- Optional `cl-resilience-kit/observability` system publishing resilience
  event counts and durations through `cl-observability-kit`.
- Optional `cl-resilience-kit/dataflow` system exposing the `cl-dataflow`
  API under `resilience-dataflow`, plus a `define-resilience-pipeline`
  macro that wraps `cl-dataflow` node handlers with `call-with-resilience`.

### Notes

- There is no process-global breaker, limiter, timer, or worker thread;
  clocks, random sources, sleepers, cancellation tokens, event handlers,
  state stores, and lease stores are injected at the operation boundary.
- This is the first tagged release. No prior version was published.

[1.0.0]: https://github.com/nerima-lisp/cl-resilience-kit/releases/tag/v1.0.0
