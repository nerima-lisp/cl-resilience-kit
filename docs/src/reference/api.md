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
`check-cancellation-token`. Cancellation is cooperative: a token is observed at
resilience boundaries and does not interrupt an arbitrary non-cooperating thunk.

## Conditions and events

Operational failures inherit from `resilience-error`. Handle condition classes
and their readers rather than parsing printed reports. The main public boundary
conditions are:

| Condition | Contract data |
| --- | --- |
| `resilience-cancelled` | `resilience-cancelled-reason` and the originating token. |
| `retry-exhausted` | Attempt count, last condition/result, reason, and policy. |
| `retry-classifier-error` | The classifier failure in `retry-classifier-error-cause`; this is re-signaled. |
| `deadline-exceeded` / `attempt-timeout` | Deadline, observed time, stage, attempt, and timeout. |
| `circuit-open` / `bulkhead-rejected` / `rate-limit-exceeded` | Admission state, capacity, token counts, and optional retry delay. |
| `resilience-draining` / `resilience-execution-rejected` / `resilience-execution-timeout` / `resilience-hard-timeout` | Lifecycle, queue rejection, timeout, or backend data. |
| `hedge-unsafe` / `hedge-exhausted` / `idempotency-key-required` / `idempotency-conflict` | The idempotency or speculative-execution failure details. |
| `resilience-store-error` and lease/fencing conditions | Store key, cause, owner, lease, or fencing-token data. |

When a retry policy is configured, `fallback` is called with the terminal
`retry-exhausted`, `deadline-exceeded`, or `resilience-cancelled` condition and
its return values become the operation result. A `retry-classifier-error` is
re-signaled so a broken classifier cannot be hidden by fallback.

`resilience-event` is a best-effort observation. Its `type` and `stage` are
implementation-defined keywords, not a closed enum; consumers should tolerate
new values. Event handlers receive the event object, and handler errors are
swallowed so observation cannot change operation control flow. Use the event
readers (`resilience-event-type`, `resilience-event-operation`,
`resilience-event-attempt`, `resilience-event-stage`, `resilience-event-condition`,
`resilience-event-result`, `resilience-event-delay`,
`resilience-event-reason`, `resilience-event-timestamp`, `resilience-event-context`,
`resilience-event-metadata`, and `resilience-event-duration`) for structured
data.

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

The macro and continuation entry points keep the same option vocabulary:

| Entry point | Purpose |
| --- | --- |
| `with-resilience ((&rest options) &body body)` | Evaluate `body` through `call-with-resilience`. |
| `call-with-resilience/k (thunk on-success on-error &rest options)` | Dispatch every successful value to `on-success`, or one signaled `error` condition to `on-error`. |
| `with-resilience/k ((on-success on-error &rest options) &body body)` | Macro wrapper for the continuation boundary. |

`with-resilience` and `with-resilience/k` validate literal option lists during
macro expansion. Options must be keyword/value pairs from the composition
signature, and duplicate keys are rejected. `call-with-resilience/k` keeps
continuation failures visible: an error raised by either callback escapes the
boundary instead of being sent to the other callback.

The composition helper propagates the selected operation, context, time, and
event boundaries to the nested controls. Read [Core concepts](../guide/core-concepts.md)
before changing the order of controls for a production operation.
