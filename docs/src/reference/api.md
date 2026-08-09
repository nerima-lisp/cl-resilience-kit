# API reference

The package exports the public API from `cl-resilience-kit`. The signatures
below group the main entry points by concern; inspect the source package
definition for the complete export list.

## Retry and deadlines

| Entry point | Purpose |
| --- | --- |
| `make-retry-policy (&key max-attempts initial-delay multiplier max-delay jitter condition-classifier result-classifier retry-safe-p random-source)` | Build an explicit backoff and classification policy. |
| `call-with-retry (policy thunk &key overall-timeout overall-deadline per-attempt-timeout clock monotonic-units-per-second sleeper operation retry-budget cancellation-token event-handler fallback)` | Invoke a thunk under cooperative retry and deadline controls. |
| `with-retry ((policy &rest options) &body body)` | Macro wrapper for `call-with-retry`. |
| `make-retry-budget (&key limit window clock monotonic-units-per-second)` | Create a process-local retry budget. |
| `make-distributed-retry-budget (&key store key limit window clock monotonic-units-per-second)` | Create a retry budget backed by a state store. |
| `call-with-deadline (thunk &key timeout deadline clock monotonic-units-per-second operation cancellation-token event-handler)` | Apply an inner or absolute cooperative deadline. |
| `with-deadline ((&rest options) &body body)` | Macro wrapper for `call-with-deadline`. |

`overall-timeout` and `overall-deadline` are mutually exclusive. A retry
policy's `max-attempts` includes the initial invocation.

## Admission and coordination

| Entry point | Purpose |
| --- | --- |
| `make-circuit-breaker (&key failure-threshold reset-timeout half-open-probe-limit success-threshold condition-classifier result-classifier clock monotonic-units-per-second)` | Create a process-local `CLOSED`/`OPEN`/`HALF-OPEN` breaker. |
| `circuit-breaker-call (breaker thunk &key operation cancellation-token event-handler)` | Execute through a local breaker. |
| `make-distributed-circuit-breaker (&key store key failure-threshold reset-timeout half-open-probe-limit success-threshold condition-classifier result-classifier clock monotonic-units-per-second lease-store lease-owner lease-ttl)` | Create a breaker whose state is held in an explicit store. |
| `distributed-circuit-breaker-call (breaker thunk &key operation cancellation-token event-handler)` | Execute through a distributed breaker. |
| `make-bulkhead (&key limit)` | Create a no-queue concurrency limit. |
| `bulkhead-call (bulkhead thunk &key operation cancellation-token event-handler timeout)` | Run through a no-queue bulkhead. |
| `make-queued-bulkhead (&key limit max-queue)` | Create a bounded-queue bulkhead. |
| `queued-bulkhead-call (bulkhead thunk &key operation cancellation-token event-handler timeout)` | Run through a queued bulkhead. |
| `make-rate-limiter (&key capacity refill-rate initial-tokens clock monotonic-units-per-second sleeper)` | Create a token-bucket limiter. |
| `rate-limiter-acquire (limiter &key tokens wait-p max-wait signal-on-reject-p operation cancellation-token event-handler)` | Acquire tokens, optionally waiting within a bound. |

## Execution and deduplication

| Entry point | Purpose |
| --- | --- |
| `make-resilience-executor (&key size name queue-capacity)` | Create a bounded worker executor. |
| `resilience-executor-call (executor thunk &key hard-timeout timeout operation clock monotonic-units-per-second)` | Wait for a worker result with caller and best-effort worker limits. |
| `with-resilience-executor (executor-options &body body)` | Evaluate a block on a resilience executor. |
| `call-with-hedging (thunk &key hedge-after max-attempts executor hard-timeout hedge-safe-p idempotency-key cancellation-token operation clock monotonic-units-per-second)` | Start explicitly safe speculative attempts. |
| `with-hedging (options &body body)` | Evaluate a block through the hedging boundary. |
| `make-request-coalescer ()` | Create a process-local request coalescer. |
| `call-with-request-coalescing (coalescer thunk &key key idempotency-fingerprint executor hard-timeout timeout operation clock monotonic-units-per-second)` | Share one in-flight result for a key. |
| `with-request-coalescing (coalescer-options &body body)` | Evaluate a block through the coalescing boundary. |

Hedging requires `hedge-safe-p` or an idempotency key when more than one
attempt is allowed. Coalescing requires a key and treats conflicting
idempotency fingerprints as an error.

## Context, events, and lifecycle

| Entry point | Purpose |
| --- | --- |
| `make-resilience-context (&key operation correlation-id trace-id span-id parent-span-id idempotency-key tags baggage)` | Build transport-neutral operation metadata. |
| `with-resilience-context ((&rest initargs) &body body)` | Bind merged context for a body. |
| `make-resilience-metrics (&key name)` | Create low-cardinality event metrics. |
| `record-resilience-event (metrics event)` | Record one event. |
| `make-resilience-observer (&rest handlers)` | Fan out observations while isolating handler errors. |
| `make-resilience-lifecycle (&key name)` | Create a running/draining/stopped lifecycle boundary. |
| `begin-resilience-drain (lifecycle)` | Stop accepting new operations. |
| `await-resilience-drained (lifecycle &key timeout)` | Wait for active operations to leave. |
| `stop-resilience-lifecycle (lifecycle &key timeout)` | Drain and mark the lifecycle stopped. |
| `make-health-registry (&key name)` | Create a registry of named health checks. |
| `register-health-check (registry name checker)` | Register a function returning a truthy health value. |
| `health-report (registry)` | Return per-check status plists. |

Cancellation is provided by `make-cancellation-token`,
`cancel-cancellation-token`, `cancellation-token-cancelled-p`, and
`check-cancellation-token`.

## Composition

```lisp
(call-with-resilience
 thunk
 &key retry-policy circuit-breaker distributed-circuit-breaker
      bulkhead bulkhead-timeout rate-limiter rate-limit-tokens
      rate-limit-wait-p rate-limit-max-wait
      overall-timeout overall-deadline per-attempt-timeout
      clock monotonic-units-per-second sleeper operation retry-budget
      cancellation-token event-handler fallback context metrics observer
      lifecycle executor executor-timeout hard-timeout hedge-after
      max-hedge-attempts hedge-safe-p request-coalescer idempotency-key
      idempotency-fingerprint)
```

The composition helper propagates the selected operation, context, time, and
event boundaries to the nested controls. Read [Core concepts](../guide/core-concepts.md)
before changing the order of controls for a production operation.
