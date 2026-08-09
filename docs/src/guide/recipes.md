# Recipes

The examples below show the intended boundaries. Application functions such as
`fetch-order` are placeholders; classify their failures using conditions and
results defined by the application.

## Classify retryable failures

Keep the retry decision next to the policy and make retry safety explicit:

```common-lisp
(define-condition temporary-storage-error (error) ())

(let ((policy
        (make-retry-policy
         :max-attempts 4
         :initial-delay 0.01d0
         :multiplier 2d0
         :max-delay 0.5d0
         :jitter :full
         :retry-safe-p t
         :condition-classifier
         (lambda (condition attempt)
           (declare (ignore attempt))
           (typep condition 'temporary-storage-error)))))
  (call-with-retry
   policy
   (lambda ()
     (fetch-order :id 42))))
```

If the operation is not safe to repeat, leave `:retry-safe-p` false and
handle the failure at the operation boundary instead.

## Compose controls around one operation

`call-with-resilience` accepts the controls that should describe one logical
operation. The example uses a local breaker, a no-queue bulkhead, and a
token-bucket limiter:

```common-lisp
(let ((policy (make-retry-policy
               :max-attempts 3
               :retry-safe-p t
               :condition-classifier
               (lambda (condition attempt)
                 (declare (ignore attempt))
                 (typep condition 'temporary-storage-error))))
      (breaker (make-circuit-breaker
                :failure-threshold 5
                :reset-timeout 30d0))
      (bulkhead (make-bulkhead :limit 32))
      (limiter (make-rate-limiter
                :capacity 100
                :refill-rate 25d0)))
  (call-with-resilience
   (lambda () (fetch-order :id 42))
   :retry-policy policy
   :circuit-breaker breaker
   :bulkhead bulkhead
   :rate-limiter limiter
   :rate-limit-wait-p t
   :overall-timeout 2d0
   :operation :fetch-order))
```

The classifier should return true only for failures that are safe to retry.
For queued admission, use
`make-queued-bulkhead` and set `:bulkhead-timeout` at the composition boundary.

## Add a distributed state boundary

The memory stores are useful for a single process and tests. A multi-process
deployment should implement the same store protocols with a shared backend:

```common-lisp
(let* ((state-store (make-memory-state-store))
       (lease-store (make-memory-lease-store))
       (breaker
         (make-distributed-circuit-breaker
          :store state-store
          :key "orders"
          :lease-store lease-store
          :lease-owner "worker-a"
          :lease-ttl 30d0)))
  (distributed-circuit-breaker-call
   breaker
   (lambda () (fetch-order :id 42))
   :operation :fetch-order))
```

Replace the memory implementations with a shared `resilience-state-store`
and, when probe or ownership fencing is needed, a shared
`resilience-lease-store`. Compare-and-swap is the correctness boundary; a
plain read/write adapter is not equivalent.

## Observe without changing operation control flow

Metrics and observers can be combined with an event handler. Observer failures
are isolated by the library:

```common-lisp
(let ((metrics (make-resilience-metrics :name "orders"))
      (observer
        (make-resilience-observer
         (lambda (event)
           (log-resilience-event event)))))
  (call-with-resilience
   (lambda () (fetch-order :id 42))
   :metrics metrics
   :observer observer
   :operation :fetch-order))
```

Use stable operation names for low-cardinality metrics. For a custom event
pipeline, pass `:event-handler` directly or use
`(resilience-metrics-handler metrics)` as a handler.

## Carry context and cancellation

Context is transport-neutral and can carry correlation or idempotency data:

```common-lisp
(with-resilience-context
    (:operation :fetch-order
     :correlation-id "request-42"
     :idempotency-key "order-42")
  (call-with-resilience
   (lambda () (fetch-order :id 42))
   :cancellation-token token))
```

Create `token` with `make-cancellation-token`, optionally passing a parent
token. Operations observe cancellation cooperatively through the composed
boundaries.

## Drain before shutdown

Use a lifecycle boundary to reject new work, wait for active work, and then
stop:

```common-lisp
(let ((lifecycle (make-resilience-lifecycle :name "orders")))
  (call-with-resilience
   (lambda () (fetch-order :id 42))
   :lifecycle lifecycle
   :operation :fetch-order)
  (begin-resilience-drain lifecycle)
  (await-resilience-drained lifecycle :timeout 5d0)
  (stop-resilience-lifecycle lifecycle :timeout 1d0))
```

Register independent checks with `make-health-registry`; `health-report`
returns a plist for each check with `:status` of `:healthy` or `:unhealthy`.

## Hedge or coalesce only safe work

Hedging may start more than one operation, and coalescing shares one result
among callers. Protect the operation with a safety declaration or an
idempotency identity:

```common-lisp
(with-hedging
    (:executor executor
     :hedge-after 0.05d0
     :max-attempts 2
     :hedge-safe-p t
     :operation :fetch-order)
  (fetch-order))

(with-request-coalescing
    (coalescer
     :key "order-42"
     :idempotency-fingerprint "orders-v1"
     :operation :fetch-order)
  (fetch-order))
```

Hedging does not forcefully cancel loser work. Request coalescing is
process-local, requires a key, and rejects conflicting fingerprints.
