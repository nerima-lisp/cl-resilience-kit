# Architecture

## Source layout

| Area | Source files | Responsibility |
| --- | --- | --- |
| Package and conditions | `src/package.lisp`, `src/conditions.lisp` | Public exports and structured failure conditions. |
| Data validation | `src/data-validation.lisp` | Shared predicates for validating finite, proper input data. |
| Context and observation | `src/context.lisp`, `src/events.lisp` | Operation metadata, metrics, and event fan-out. |
| Time and cancellation | `src/deadline.lisp`, `src/cancellation.lisp` | Cooperative deadlines, cancellation, and injected monotonic time. |
| Retry | `src/retry-policy.lisp`, `src/retry-execution.lisp`, `src/retry.lisp`, `src/retry-budget.lisp` | Classification, backoff, attempt execution, and budgets. |
| Breakers and stores | `src/circuit-breaker.lisp`, `src/distributed-circuit-breaker.lisp`, `src/state-store.lisp`, `src/lease-store.lisp` | Local state machines and explicit distributed storage contracts. |
| Admission | `src/bulkhead.lisp`, `src/rate-limiter.lisp` | Concurrency and token admission. |
| Execution | `src/executor.lisp`, `src/hedging.lisp`, `src/coalescing.lisp` | Worker boundaries, speculative attempts, and request sharing. |
| Lifecycle and composition | `src/lifecycle.lisp`, `src/composition-core.lisp`, `src/composition-plan.lisp`, `src/composition-support.lisp`, `src/composition-runtime.lisp`, `src/composition.lisp`, `src/composition-macros.lisp` | Shutdown readiness, plan data, primitive composition, execution-boundary selection, event-handler assembly, and macro/CPS entry points. |

`src/package.lisp` is the public surface. Internal helpers remain package
private so integrations depend on the exported contracts rather than on state
representation.

Composition macros validate their static option vocabulary at expansion time;
the runtime function remains responsible for executing the control chain. The
continuation boundary is deliberately explicit: successful multiple values go
to the success callback, while operation errors go to the error callback.

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
or transport dependency; adapters can translate context and events to an
application's HTTP, RPC, queue, or tracing system.
