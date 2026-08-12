(in-package #:resilience-kit)

(defun %run-resilience-base (plan active-token active-handler)
  "Run PLAN through the primitive resilience controls."
  (let ((operation-thunk
          (lambda ()
            (%call-with-resilience-core
             (resilience-plan-thunk plan)
             :retry-policy (resilience-plan-retry-policy plan)
             :circuit-breaker (resilience-plan-circuit-breaker plan)
             :bulkhead (resilience-plan-bulkhead plan)
             :bulkhead-timeout (resilience-plan-bulkhead-timeout plan)
             :rate-limiter (resilience-plan-rate-limiter plan)
             :rate-limit-tokens (resilience-plan-rate-limit-tokens plan)
             :rate-limit-wait-p (resilience-plan-rate-limit-wait-p plan)
             :rate-limit-max-wait (resilience-plan-rate-limit-max-wait plan)
             :rate-limit-signal-on-reject-p
             (resilience-plan-rate-limit-signal-on-reject-p plan)
             :overall-timeout (resilience-plan-overall-timeout plan)
             :overall-deadline (resilience-plan-overall-deadline plan)
             :per-attempt-timeout (resilience-plan-per-attempt-timeout plan)
             :clock (resilience-plan-clock plan)
             :monotonic-units-per-second
             (resilience-plan-monotonic-units-per-second plan)
             :sleeper (resilience-plan-sleeper plan)
             :operation (resilience-plan-operation plan)
             :retry-budget (resilience-plan-retry-budget plan)
             :cancellation-token active-token
             :event-handler active-handler
             :fallback (resilience-plan-fallback plan)))))
    (if (resilience-plan-distributed-circuit-breaker plan)
        (distributed-circuit-breaker-call
         (resilience-plan-distributed-circuit-breaker plan)
         operation-thunk
         :operation (resilience-plan-operation plan)
         :cancellation-token active-token
         :event-handler active-handler)
        (funcall operation-thunk))))

(defun %run-resilience-execution (plan active-token active-handler)
  "Select PLAN's outer execution boundary and run it."
  (let ((run-base
          (lambda ()
            (%run-resilience-base plan active-token active-handler))))
    (cond
      ((resilience-plan-request-coalescer plan)
       (call-with-request-coalescing
        (resilience-plan-request-coalescer plan) run-base
        :key (resilience-plan-idempotency-key plan)
        :idempotency-fingerprint
        (resilience-plan-idempotency-fingerprint plan)
        :executor (resilience-plan-executor plan)
        :hard-timeout (resilience-plan-hard-timeout plan)
        :timeout (or (resilience-plan-executor-timeout plan)
                     (resilience-plan-overall-timeout plan))
        :operation (resilience-plan-operation plan)
        :clock (resilience-plan-clock plan)
        :monotonic-units-per-second
        (resilience-plan-monotonic-units-per-second plan)))
      ((and (resilience-plan-max-hedge-attempts plan)
            (> (resilience-plan-max-hedge-attempts plan) 1))
       (call-with-hedging
        run-base
        :hedge-after (or (resilience-plan-hedge-after plan) 0d0)
        :max-attempts (resilience-plan-max-hedge-attempts plan)
        :executor (resilience-plan-executor plan)
        :hard-timeout (resilience-plan-hard-timeout plan)
        :hedge-safe-p (resilience-plan-hedge-safe-p plan)
        :idempotency-key (resilience-plan-idempotency-key plan)
        :cancellation-token active-token
        :operation (resilience-plan-operation plan)
        :clock (resilience-plan-clock plan)
        :monotonic-units-per-second
        (resilience-plan-monotonic-units-per-second plan)))
      ((resilience-plan-executor plan)
       (resilience-executor-call
        (resilience-plan-executor plan) run-base
        :hard-timeout (resilience-plan-hard-timeout plan)
        :timeout (or (resilience-plan-executor-timeout plan)
                     (resilience-plan-overall-timeout plan))
        :operation (resilience-plan-operation plan)
        :clock (resilience-plan-clock plan)
        :monotonic-units-per-second
        (resilience-plan-monotonic-units-per-second plan)))
      ((resilience-plan-hard-timeout plan)
       (%run-with-hard-timeout
        run-base
        (resilience-plan-hard-timeout plan)
        (resilience-plan-operation plan)
        :thread
        :clock (resilience-plan-clock plan)
        :monotonic-units-per-second
        (resilience-plan-monotonic-units-per-second plan)))
      (t
       (funcall run-base)))))
