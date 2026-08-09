# cl-resilience-kit

cl-resilience-kit is a set of composable, dependency-neutral resilience
primitives for Common Lisp. It provides policies and coordination mechanisms
without assuming a particular web framework, RPC protocol, storage backend, or
application lifecycle.

The library keeps policy decisions explicit and makes operational boundaries
injectable. Clocks, random sources, sleepers, cancellation tokens, state
stores, lease stores, and event handlers can therefore be supplied by the
application or by deterministic tests.

## What it provides

- Retry policies with backoff, jitter, deadlines, cancellation, and retry budgets.
- Local and distributed circuit breakers with half-open probes and optional fencing leases.
- Bulkheads, queued bulkheads, token-bucket rate limiting, and bounded executors.
- Hedging and process-local request coalescing for explicitly safe operations.
- Resilience contexts, structured conditions, metrics, observers, lifecycle, and health checks.
- Composition helpers for applying several controls to one operation.

## Design boundary

The kit coordinates resilience behavior; it does not decide whether an
application operation is safe to retry or hedge. The caller supplies that
knowledge through policy flags, classifiers, idempotency keys, or operation
metadata. Timeouts and cancellation are cooperative unless the selected
executor provides a separate best-effort worker boundary.

Start with [Getting started](getting-started.md). Then read [Core concepts](guide/core-concepts.md)
for the invariants shared by the primitives, and use the [API reference](reference/api.md)
when integrating a specific component.
