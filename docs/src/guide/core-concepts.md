# Core concepts

## Policy and operation are separate

The library owns coordination; the caller owns the meaning of an operation.
Retry and hedging are never assumed to be safe merely because a condition or
result looks temporary. Use `:retry-safe-p`, condition/result classifiers,
idempotency keys, or explicit operation metadata to make that decision visible.

`max-attempts` counts the initial invocation. A retry classifier receives the
condition or result and the current attempt number. It can return a boolean, a
numeric delay hint, or a `retry-decision` object. This lets applications keep
domain-specific failure classification outside the resilience mechanism.

## Time is monotonic and injectable

Backoff, deadlines, limiter refills, breaker reset windows, and leases use an
injected monotonic clock. The corresponding units-per-second value makes the
clock representation explicit. Retry jitter uses an injected random source and
backoff waits use an injected sleeper.

Local and distributed circuit-breaker events use the same injected monotonic
clock as their state transitions. This keeps event timestamps and transition
decisions on one time base, including in deterministic tests.

Timeouts are cooperative at the operation boundary. A deadline or cancellation
token is observed at documented checkpoints; arbitrary Lisp code is not
forcibly interrupted. `resilience-executor-call` additionally offers a
best-effort worker-side `hard-timeout`, but callers must still treat resource
cleanup and side effects as application concerns.

## State belongs to the selected scope

The local circuit breaker, local retry budget, bulkheads, limiter, executor,
and request coalescer are process-local objects. A distributed circuit breaker
or retry budget receives an explicit state store and key. Correctness depends
on atomic compare-and-swap behavior in that store; a lease store and fencing
token can additionally serialize half-open probes or ownership transitions.

An in-memory store is useful for tests and single-process coordination. It is
not a substitute for a shared durable store across processes.

## Events are observations, not control flow

Event handlers and observers receive operational signals without becoming the
protected operation's failure path. Observer errors are isolated from the
operation. Use metrics and observers to record attempts, transitions, waits,
and cancellations, while keeping the operation's result and conditions under
the operation's own control.

## Composition order matters

The composition helper applies controls in an explicit order. A typical
operation is structured like this:

```text
context
  -> deadline / cancellation
    -> retry budget
      -> circuit breaker
        -> bulkhead
          -> rate limiter
            -> retry policy
              -> operation
```

There is no universally correct order. Choose it according to the resource
that each control protects. Keep one shared clock, cancellation token, event
handler, and operation identity where the surrounding application expects
those signals to describe one logical call.
