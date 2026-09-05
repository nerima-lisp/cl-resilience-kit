# Architecture

## Source layout

| Area | Source files | Responsibility |
| --- | --- | --- |
| Package and conditions | `src/package.lisp`, `src/conditions-base.lisp`, `src/conditions-retry.lisp`, `src/conditions-isolation.lisp`, `src/conditions-distributed.lisp`, `src/conditions-composition.lisp` | Public exports and structured failure conditions grouped by retry, isolation, distributed coordination, and composition concerns. |
| Data validation | `src/data-validation.lisp` | Shared predicates for validating finite, proper input data. |
| Context and observation | `src/context.lisp`, `src/events.lisp` | Operation metadata, metrics, and event fan-out. |
| Time and cancellation | `src/deadline.lisp`, `src/cancellation.lisp` | Cooperative deadlines, cancellation, and injected monotonic time. |
| Retry | `src/retry-policy.lisp`, `src/retry-scheduling.lisp`, `src/retry-budget-definition.lisp`, `src/retry-budget-execution.lisp`, `src/retry-execution-boundaries.lisp`, `src/retry-execution-recovery.lisp`, `src/retry-execution.lisp`, `src/retry.lisp` | Policy data, option normalization, classifier normalization, backoff scheduling, retry budgets, attempt-boundary helpers, recovery hooks, attempt execution, and macro entry points. |
| Breakers and stores | `src/state-store.lisp`, `src/lease-store-definition.lisp`, `src/lease-store-execution.lisp`, `src/lease-store.lisp`, `src/circuit-breaker-definition.lisp`, `src/circuit-breaker-state.lisp`, `src/circuit-breaker.lisp`, `src/distributed-circuit-breaker-definition.lisp`, `src/distributed-circuit-breaker-state.lisp`, `src/distributed-circuit-breaker-transition.lisp`, `src/distributed-circuit-breaker-execution.lisp`, `src/distributed-circuit-breaker-api.lisp` | State-store and lease-store abstractions plus local and distributed breaker data, state transitions, execution paths, and public APIs. |
| Admission | `src/bulkhead-definition.lisp`, `src/bulkhead-execution.lisp`, `src/bulkhead.lisp`, `src/rate-limiter-definition.lisp`, `src/rate-limiter-execution.lisp`, `src/rate-limiter.lisp` | Bulkhead capacity data, admission control, execution wrappers, public macros, and token-bucket limiter policy and execution. |
| Execution | `src/executor-definition.lisp`, `src/executor-execution.lisp`, `src/executor.lisp`, `src/hedging-execution.lisp`, `src/hedging.lisp`, `src/coalescing-definition.lisp`, `src/coalescing-execution.lisp`, `src/coalescing.lisp` | Worker-boundary data and execution, speculative hedging, and request coalescing definitions and runtime behavior. |
| Lifecycle and composition | `src/lifecycle.lisp`, `src/composition-core.lisp`, `src/composition-plan.lisp`, `src/composition-support.lisp`, `src/composition-runtime-context.lisp`, `src/composition-runtime-execution.lisp`, `src/composition-runtime-dispatch.lisp`, `src/composition.lisp`, `src/composition-macros.lisp` | Shutdown readiness, composition-plan data, primitive composition helpers, runtime context/execution/dispatch assembly, and macro/CPS entry points. |

`src/package.lisp` is the public surface. Internal helpers remain package
private so integrations depend on the exported contracts rather than on state
representation.

Composition macros validate their static option vocabulary at expansion time;
the runtime function remains responsible for executing the control chain. The
continuation boundary is deliberately explicit: successful multiple values go
to the success callback, while operation errors go to the error callback.

## Composition dispatch

Composition calls use the smallest execution path that preserves their
configured boundaries. Literal option lists recognized by the compiler macro
can use a direct core path, a metrics-only path, or a runtime-context path;
calls that need a plan object use the plan path. Distributed breakers,
executors, hedging, request coalescing, and fingerprint-based idempotency are
examples of features that require the plan path.

Metrics-only execution records metrics without activating the runtime event
boundary. A context, observer, lifecycle, event handler, or idempotency key
activates the corresponding runtime boundary when a plan is not required.
Unknown or dynamically shaped option lists continue through ordinary runtime
validation, so dispatch is an implementation detail rather than a separate
API contract.

The retry boundary also has a single-pass path for a non-retry-safe policy with
one allowed attempt when no breaker, bulkhead, limiter, retry budget, event
handler, or fallback requires the retry runtime. Deadline and cancellation
checks remain cooperative on that path.

## Important invariants

### Monotonic time

The same normalized monotonic clock is passed through a composed operation.
Breaker reset decisions, breaker event timestamps, deadlines, rate refills,
and lease expiry therefore use one time base. Tests can replace the clock and
units-per-second without changing policy code.

### Cooperative boundaries

The kit checks cancellation and deadlines at explicit boundaries. It does not
pretend that arbitrary Lisp code can be safely interrupted. The executor's
`hard-timeout` is a best-effort worker boundary with separate cleanup and side
effect responsibilities.

### Distributed compare-and-swap

Distributed breakers and budgets use an explicit `resilience-state-store`.
State transitions must use the store's atomic compare-and-swap contract. A
lease store can provide ownership and fencing tokens for half-open probes, but
it does not remove the need for state-store atomicity.

### Safety is caller knowledge

Retries and hedges can duplicate effects. The library requires a retry safety
declaration, a classifier, an idempotency key, or an equivalent application
decision instead of inferring safety from a generic error type.

## Process boundary

The local breaker, local retry budget, bulkheads, limiter, executor, and request
coalescer are ordinary process-local objects. A shared store is required when
multiple processes must observe one breaker or budget. The core has no protocol
or transport dependency; applications can pass the exported context and event
objects directly into their HTTP, RPC, queue, or tracing stack without an
extra adapter layer.
