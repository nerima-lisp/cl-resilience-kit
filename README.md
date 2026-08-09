# cl-resilience-kit

`cl-resilience-kit` provides generic resilience primitives for Common Lisp
operations that can fail transiently: service calls, database access, queues,
filesystem operations, and other application boundaries. It has no dependency
on HTTP, foreign exchange, PostgreSQL, Redis, or any other protocol package.

The library deliberately separates policy from operation-specific knowledge.
Retry decisions are made by caller-supplied condition and result classifiers;
there is no default policy that retries arbitrary errors.

## Features

- retry policies with maximum attempts, exponential backoff, maximum delay, and
  full, equal, or decorrelated jitter;
- generic retry decisions, including an optional numeric delay hint;
- injected monotonic clock, random source, and sleeper;
- cooperative overall deadlines and cooperative per-attempt timeouts;
- thread-safe local and distributed circuit breakers with closed, open, and
  half-open states, probe limits, compare-and-swap state, and fencing leases;
- non-blocking and bounded-wait queued thread-safe bulkheads;
- a monotonic-clock token-bucket rate limiter with optional caller-thread
  waiting;
- cooperative cancellation tokens with parent-token propagation;
- correlation, tracing, idempotency, tags, and baggage context;
- structured resilience events for attempts, waits, admission, rejection,
  deadlines, cancellation, and fallback;
- metrics and multi-handler observers, shared fixed-window retry budgets, and
  state-store-backed distributed retry budgets;
- an explicit executor with bounded queues, structured rejection, caller wait
  timeouts, and best-effort worker hard-timeout boundaries;
- delayed hedging with an explicit safety/idempotency guard, and process-local
  request coalescing with fingerprint conflict detection;
- lifecycle draining/stopping and readiness/liveness health checks;
- structured conditions for retry exhaustion, classifier failures, deadlines,
  cancellation, circuit-open rejection, bulkhead rejection, and rate-limit
  rejection, execution rejection/timeouts, hard timeouts, hedge exhaustion,
  and idempotency conflicts;
- explicit composition through `CALL-WITH-RESILIENCE` and
  `WITH-RESILIENCE`.

The implementation has no process-global breaker, limiter, timer, or worker
thread. `cl-concurrent-kit` supplies the mutex boundary used by the shared
stateful primitives. Its current portability follows that dependency.

## Retry

```lisp
(let ((policy
        (make-retry-policy
         :max-attempts 4
         :initial-delay 0.05
         :multiplier 2
         :max-delay 2
         :jitter :full
         :retry-safe-p t
         :condition-classifier
         (lambda (condition attempt)
           (declare (ignore attempt))
           (typep condition 'temporary-storage-error))
         :result-classifier
         (lambda (result attempt)
           (declare (ignore attempt))
           (eq result :try-again)))))
  (call-with-retry policy
                   (lambda () (write-to-store object))))
```

For block-oriented code, `WITH-RETRY` is the equivalent macro form:

```lisp
(with-retry (policy :operation :write)
  (write-to-store object))
```

`MAX-ATTEMPTS` includes the initial call. `INITIAL-DELAY` and `MULTIPLIER`
control the exponential schedule, while `MAX-DELAY` caps the calculated delay.
Jitter is applied after the exponential value is capped and is capped again so
that the final delay never exceeds `MAX-DELAY`.

The classifiers may return a boolean, a numeric delay hint, a
`RETRY-DECISION`, or two values `(retry-p delay-hint)`. A delay hint is generic
and numeric; it is treated as a lower bound for the computed backoff and is
still capped by `MAX-DELAY`. An HTTP integration may translate `Retry-After` into
it, but this library does not know about HTTP response objects.

Retry safety is explicit. A policy with no classifier, or with
`RETRY-SAFE-P` false, does not retry. This library has no `IDEMPOTENT-P`
switch: the caller owns the idempotency and recovery decision and must set
`RETRY-SAFE-P` to true only after establishing that repeating the operation is
safe. This is a safety boundary, not a guarantee that an operation is actually
idempotent.

When retrying ends after a retryable failure, `RETRY-EXHAUSTED` contains the
attempt count and last condition/result. A non-retryable condition is re-signaled
unchanged.

`MAKE-RETRY-BUDGET` creates a lock-protected, monotonic fixed-window budget for
sharing a retry allowance across calls. The initial attempt does not consume a
budget token; each authorized retry does. Pass the same budget as
`:RETRY-BUDGET` to multiple `CALL-WITH-RETRY` or `CALL-WITH-RESILIENCE` calls
to bound aggregate retry pressure. `RETRY-BUDGET-USED`,
`RETRY-BUDGET-REMAINING`, and `RETRY-BUDGET-ACQUIRE` expose the budget state for
integrations and instrumentation.

If a retry loop reaches a terminal resilience condition, `:FALLBACK` may be a
function receiving that condition. Its returned values become the result of the
resilience call. The fallback applies to terminal conditions handled by the
retry loop; admission failures that occur outside that loop, such as a
bulkhead rejection in `CALL-WITH-RESILIENCE`, and classifier failures bypass
it. In retry, classifier failures are wrapped in `RETRY-CLASSIFIER-ERROR` with
the original condition available through `RETRY-CLASSIFIER-ERROR-CAUSE`; they
are not silently treated as retryable failures. Circuit-breaker classifier
failures are re-signaled at the circuit boundary with the original condition.

`MAKE-DISTRIBUTED-RETRY-BUDGET` uses the protocol-neutral
`RESILIENCE-STATE-STORE` CAS interface to share a retry allowance across
processes when the caller supplies a durable or network-backed store. The
bundled `MEMORY-STATE-STORE` is useful for tests and single-process sharing;
it is not a distributed backend.

## Cancellation, events, and fallback

`MAKE-CANCELLATION-TOKEN` creates a cooperative cancellation source. A child
token can name `:PARENT` and observes cancellation of that parent. Call
`CANCEL-CANCELLATION-TOKEN` with an optional reason, then pass the token as
`:CANCELLATION-TOKEN` to any of the deadline, retry, circuit-breaker, bulkhead,
rate-limiter, or composition APIs. The core checks cancellation at operation
and wait boundaries; it cannot forcibly interrupt foreign code or a blocking
call that does not cooperate.

Pass `:EVENT-HANDLER` to receive a `RESILIENCE-EVENT` for operational
observation. Events include `:ATTEMPT-START`, `:ATTEMPT-FAILURE`,
`:ATTEMPT-SUCCESS`, `:RETRY-SCHEDULED`, `:RETRY-EXHAUSTED`, `:FALLBACK`,
`:DEADLINE-EXCEEDED`, `:CANCELLED`, `:CIRCUIT-REJECTED`,
`:BULKHEAD-REJECTED`, `:BULKHEAD-ADMITTED`, `:BULKHEAD-RELEASED`,
`:RATE-LIMIT-ACQUIRED`, `:RATE-LIMIT-REJECTED`, `:CIRCUIT-FAILURE`, and
`:CIRCUIT-SUCCESS`. Each event carries the operation and relevant attempt,
condition, result, delay, reason, and timestamp fields when available. Errors
signaled by an observer are isolated from the protected operation. Explicit
non-local exits such as `THROW` propagate, while admission and probe cleanup
still runs.

## Time and timeout semantics

All elapsed-time decisions use the injected monotonic clock. The resilience
core does not use wall-clock time for deadline arithmetic. Use
`cl-boundary-kit:make-fake-clock` and `cl-boundary-kit:advance-fake-clock` to
make policy tests deterministic.

When multiple primitives participate in one composition, inject clocks from the
same monotonic time domain. A real `cl-boundary-kit` clock reports
implementation units, normalized by `INTERNAL-TIME-UNITS-PER-SECOND` by
default. A fake clock whose values are already seconds must be paired with
`:MONOTONIC-UNITS-PER-SECOND 1`.

`WITH-DEADLINE` and `CALL-WITH-DEADLINE` establish a cooperative deadline.
`CALL-WITH-RETRY` accepts `:OVERALL-TIMEOUT`/`:OVERALL-DEADLINE` and
`:PER-ATTEMPT-TIMEOUT`, and reports `DEADLINE-EXCEEDED` or
`ATTEMPT-TIMEOUT` with the stage and attempt where the check failed.

These timeout APIs check the deadline before and after calling user code; they
do not asynchronously interrupt that code. If the operation blocks forever,
the call cannot stop it merely because a deadline was computed. For an
explicit execution boundary, `MAKE-RESILIENCE-EXECUTOR` and
`RESILIENCE-EXECUTOR-CALL` separate the caller's wait timeout from the
backend's best-effort worker `:HARD-TIMEOUT`. A hard timeout is an execution
backend boundary, not a portable guarantee that arbitrary foreign code or a
blocking system call can be forcibly interrupted.

Sleeping is also cooperative. The default sleeper calls `sleep`, while tests
and applications can inject a recording or scheduler-aware sleeper. Deadline
and limiter waits do not create hidden timer threads; an explicit resilience
executor owns only the worker threads requested by its caller.

## Circuit breaker

`MAKE-CIRCUIT-BREAKER` creates an independent state machine:

```text
closed --failure threshold--> open --reset timeout--> half-open
  ^                                             |
  +--------- successful probes -----------------+
                    failed probe -> open
```

In `closed`, classified failures increment the failure count. After the reset
timeout, exactly the configured number of half-open probes may run; excess
callers receive `CIRCUIT-OPEN`. A successful half-open probe closes the breaker
when `SUCCESS-THRESHOLD` is met. A failed probe reopens it. State transitions
and probe reservations are protected by the injected/shared mutex boundary,
and stale completions from an earlier generation cannot corrupt a newer state.
Without a result classifier, returned values are successes; provide one when
the operation reports failure through its result rather than a condition.

`MAKE-DISTRIBUTED-CIRCUIT-BREAKER` stores breaker state through
`RESILIENCE-STATE-STORE` and can use `RESILIENCE-LEASE-STORE` fencing tokens
for coordination. The state-store CAS operation remains the correctness
boundary. The bundled memory stores are process-local; a durable or network
implementation is supplied by the application.

## Bulkhead and rate limiter

`MAKE-BULKHEAD` is a non-blocking concurrency limit. Calls that arrive while
the limit is full receive `BULKHEAD-REJECTED`; work is never queued implicitly.
The in-flight slot is released with `UNWIND-PROTECT`, including on errors.

`MAKE-RATE-LIMITER` is a token bucket. It uses monotonic elapsed time and
returns `(VALUES NIL RETRY-AFTER)` on a normal non-blocking rejection. Set
`:SIGNAL-ON-REJECT-P T` for `RATE-LIMIT-EXCEEDED`, or set `:WAIT-P T` to wait in
the caller using the injected sleeper. Waiting is bounded by `:MAX-WAIT` and
does not create a timer or background worker. Inside an active cooperative
deadline, a wait whose refill cannot complete before the deadline signals
`DEADLINE-EXCEEDED` before sleeping.
Rejection events identify whether the request exceeds capacity, tokens are
insufficient, the wait limit was reached, or the clock made no progress. The
standalone limiter returns values by default; `CALL-WITH-RESILIENCE` defaults
to signaling so its retry policy can classify `RATE-LIMIT-EXCEEDED`.

## Execution, hedging, and request coalescing

`MAKE-RESILIENCE-EXECUTOR` wraps an explicit worker pool. Its queue depth,
capacity, high-water mark, shutdown state, and termination state are exposed;
submissions that cannot be accepted signal `RESILIENCE-EXECUTION-REJECTED`.
`RESILIENCE-EXECUTOR-CALL` and the composition API distinguish a caller wait
timeout (`RESILIENCE-EXECUTION-TIMEOUT`) from a worker hard-timeout
(`RESILIENCE-HARD-TIMEOUT`). The latter is best effort and does not promise
forced interruption of arbitrary code.

`CALL-WITH-HEDGING` starts a primary attempt, waits for `:HEDGE-AFTER`, then
launches additional attempts up to `:MAX-ATTEMPTS`. More than one attempt
requires `:HEDGE-SAFE-P T` or an idempotency key. Loser attempts are not
forcefully cancelled by the portable promise API, so the operation must
tolerate their completion. If all attempts fail, `HEDGE-EXHAUSTED` retains
the individual causes.

`MAKE-REQUEST-COALESCER` and `CALL-WITH-REQUEST-COALESCING` share one
in-flight result for a process-local key. Equal fingerprints join the owner;
a different fingerprint signals `IDEMPOTENCY-CONFLICT`. Owner errors,
timeouts, and non-local exits settle followers and remove the entry so a later
call can retry. Coalescing is an in-flight optimization, not a durable
idempotency-result store.

## Context, lifecycle, and health

`WITH-RESILIENCE-CONTEXT` carries operation, correlation, tracing,
idempotency, tags, and baggage fields through nested calls. A
`RESILIENCE-METRICS` object records event totals and durations, while a
`RESILIENCE-OBSERVER` combines multiple event handlers without allowing an
observer failure to replace the protected operation.

`MAKE-RESILIENCE-LIFECYCLE` supports accepting, draining, and stopped states;
`ENTER-RESILIENCE-LIFECYCLE` rejects new work during drain and
`AWAIT-RESILIENCE-DRAINED` waits for admitted work to leave.
`MAKE-HEALTH-REGISTRY` provides caller-registered readiness and liveness
checks. These are local coordination and reporting primitives; deployment
health endpoints and orchestration remain application-owned.

## Composition

`CALL-WITH-RESILIENCE` composes the policy primitives in this order:

```text
bulkhead (one logical operation)
  -> retry (all attempts)
       -> rate limiter (each attempt)
            -> circuit breaker (each attempt)
                 -> operation
```

This ordering makes a retry attempt consume a rate-limit token and lets each
attempt contribute to breaker health, while the bulkhead slot covers the whole
logical operation. Supply the individual components as keyword arguments;
omitted components add no hidden behavior. Timeout checks remain cooperative
and can be passed through the retry composition. An optional executor or
hard-timeout adds an explicit execution boundary around the composed logical
operation. Hedging and request coalescing are outer execution layers:
coalescing joins before it runs the operation, while hedging runs multiple
copies of the composed operation when explicitly permitted.

The shared `:CANCELLATION-TOKEN`, `:EVENT-HANDLER`, `:RETRY-BUDGET`, and
`:FALLBACK` options are forwarded through the composition so that one logical
operation has one cancellation and observation boundary.

## Protocol and process boundary

The package includes process-local memory implementations and protocol-neutral
interfaces for coordination:

- `RESILIENCE-STATE-STORE` provides versioned get, compare-and-swap update,
  conditional delete, and prefix scan operations;
- `RESILIENCE-LEASE-STORE` provides acquire, renew, release, held checks, and
  monotonically increasing fencing tokens;
- distributed retry budgets and distributed circuit breakers consume those
  interfaces without depending on Redis, PostgreSQL, or a particular RPC
  protocol.

Applications that need cross-process guarantees must supply durable or network
implementations of those interfaces. The bundled memory stores only coordinate
within one process. Cancellation and deadlines remain cooperative, and the
executor's hard timeout is a best-effort backend boundary rather than a
portable forced-interruption guarantee. Protocol-specific classification,
idempotency keys, durable idempotency results, and side-effect recovery remain
caller-owned.

## Avoiding common failure modes

Retries are not a substitute for classification or idempotency design:

- retry storms happen when many callers retry at once; use a bounded attempt
  count, exponential backoff, jitter, an overall deadline, and a shared retry
  budget;
- thundering herd occurs when callers share a deterministic schedule; full or
  equal jitter and a useful random source spread attempts;
- a retry loop must not own an unrelated timeout while its operation owns a
  second timeout. Choose one overall deadline budget and make per-attempt
  bounds explicit, otherwise the budgets can multiply or leave no time for a
  final attempt;
- a timeout does not roll back a side effect. Do not retry a non-idempotent
  write unless the caller has an idempotency key or an equivalent recovery
  guarantee;
- circuit breakers and limiters should be scoped to the resource or dependency
  they protect, rather than silently becoming a process-wide singleton.

## Testing

The test system uses `cl-weave`, fake monotonic time, injected random sources,
and recording sleepers. It covers retry counts and classifiers, delay caps and
jitter, retry budgets, cancellation propagation, event observation, fallback,
deadline and attempt-timeout semantics, structured exhaustion, distributed
state-store budgets and fencing leases, lifecycle drain/stop and health
readiness/liveness, local and distributed circuit-breaker transitions and
probe cleanup, bulkhead rejection and queued admission, token refill and
waiting, executor rejection and timeout boundaries, hedging safety and
exhaustion, request-coalescing sharing/conflicts/cleanup, composition order,
non-idempotent safety, and concurrent state updates.

From the repository root, run:

```sh
sbcl --non-interactive --no-userinit --no-sysinit \
  --load run-tests.lisp
```

The test runner rejects an empty selection, so a zero exit status means the
selected test events passed rather than merely that the runner started.
The Nix test check applies a 120-second execution limit and a 10-second
termination grace period.

To generate an SB-COVER HTML and LCOV report while recompiling the project
from source, pass an output directory (or omit it to use `coverage/`):

```sh
sbcl --script run-coverage.lisp /tmp/cl-resilience-kit-coverage
```

The report is scoped to `src/`; dependency and test files are not counted.
The Nix coverage check applies a 300-second execution limit and the same
10-second termination grace period.
The coverage report is evidence for the selected test run, not a substitute
for exercising platform-specific backends or caller-owned operation
semantics.
