(in-package #:cl-resilience-kit)

(defstruct (resilience-plan
            (:constructor %make-resilience-plan))
  "The data boundary for a composed resilience operation.

The plan keeps option collection separate from execution.  Its accessors are
private; callers use CALL-WITH-RESILIENCE and the macro entry points instead."
  thunk
  retry-policy
  circuit-breaker
  distributed-circuit-breaker
  bulkhead
  bulkhead-timeout
  rate-limiter
  (rate-limit-tokens 1d0)
  rate-limit-wait-p
  rate-limit-max-wait
  (rate-limit-signal-on-reject-p t)
  overall-timeout
  overall-deadline
  per-attempt-timeout
  clock
  monotonic-units-per-second
  sleeper
  operation
  retry-budget
  cancellation-token
  event-handler
  fallback
  context
  metrics
  observer
  lifecycle
  executor
  executor-timeout
  hard-timeout
  hedge-after
  max-hedge-attempts
  hedge-safe-p
  request-coalescer
  idempotency-key
  idempotency-fingerprint)
