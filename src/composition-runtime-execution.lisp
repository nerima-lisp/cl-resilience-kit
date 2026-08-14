(in-package #:resilience-kit)

(defun %run-resilience-base (plan active-token active-handler)
  "Run PLAN through the primitive resilience controls."
  (let* ((thunk (resilience-plan-thunk plan))
         (retry-policy (resilience-plan-retry-policy plan))
         (circuit-breaker (resilience-plan-circuit-breaker plan))
         (bulkhead (resilience-plan-bulkhead plan))
         (bulkhead-timeout (resilience-plan-bulkhead-timeout plan))
         (rate-limiter (resilience-plan-rate-limiter plan))
         (rate-limit-tokens (resilience-plan-rate-limit-tokens plan))
         (rate-limit-wait-p (resilience-plan-rate-limit-wait-p plan))
         (rate-limit-max-wait (resilience-plan-rate-limit-max-wait plan))
         (rate-limit-signal-on-reject-p
           (resilience-plan-rate-limit-signal-on-reject-p plan))
         (overall-timeout (resilience-plan-overall-timeout plan))
         (overall-deadline (resilience-plan-overall-deadline plan))
         (per-attempt-timeout (resilience-plan-per-attempt-timeout plan))
         (clock (resilience-plan-clock plan))
         (units (resilience-plan-monotonic-units-per-second plan))
         (sleeper (resilience-plan-sleeper plan))
         (operation (resilience-plan-operation plan))
         (retry-budget (resilience-plan-retry-budget plan))
         (fallback (resilience-plan-fallback plan))
         (distributed-circuit-breaker
           (resilience-plan-distributed-circuit-breaker plan)))
    (if distributed-circuit-breaker
        (distributed-circuit-breaker-call
         distributed-circuit-breaker
         (lambda ()
           (%call-with-resilience-core
            thunk
            :retry-policy retry-policy
            :circuit-breaker circuit-breaker
            :bulkhead bulkhead
            :bulkhead-timeout bulkhead-timeout
            :rate-limiter rate-limiter
            :rate-limit-tokens rate-limit-tokens
            :rate-limit-wait-p rate-limit-wait-p
            :rate-limit-max-wait rate-limit-max-wait
            :rate-limit-signal-on-reject-p
            rate-limit-signal-on-reject-p
            :overall-timeout overall-timeout
            :overall-deadline overall-deadline
            :per-attempt-timeout per-attempt-timeout
            :clock clock
            :monotonic-units-per-second
            units
            :sleeper sleeper
            :operation operation
            :retry-budget retry-budget
            :cancellation-token active-token
            :event-handler active-handler
            :fallback fallback))
         :operation operation
         :cancellation-token active-token
         :event-handler active-handler)
        (%call-with-resilience-core
         thunk
         :retry-policy retry-policy
         :circuit-breaker circuit-breaker
         :bulkhead bulkhead
         :bulkhead-timeout bulkhead-timeout
         :rate-limiter rate-limiter
         :rate-limit-tokens rate-limit-tokens
         :rate-limit-wait-p rate-limit-wait-p
         :rate-limit-max-wait rate-limit-max-wait
         :rate-limit-signal-on-reject-p
         rate-limit-signal-on-reject-p
         :overall-timeout overall-timeout
         :overall-deadline overall-deadline
         :per-attempt-timeout per-attempt-timeout
         :clock clock
         :monotonic-units-per-second
         units
         :sleeper sleeper
         :operation operation
         :retry-budget retry-budget
         :cancellation-token active-token
         :event-handler active-handler
         :fallback fallback))))

(defun %run-resilience-execution (plan active-token active-handler)
  "Select PLAN's outer execution boundary and run it."
  (let* ((request-coalescer (resilience-plan-request-coalescer plan))
         (idempotency-key (resilience-plan-idempotency-key plan))
         (idempotency-fingerprint
           (resilience-plan-idempotency-fingerprint plan))
         (executor (resilience-plan-executor plan))
         (hard-timeout (resilience-plan-hard-timeout plan))
         (overall-timeout (resilience-plan-overall-timeout plan))
         (executor-timeout (resilience-plan-executor-timeout plan))
         (operation (resilience-plan-operation plan))
         (clock (resilience-plan-clock plan))
         (units (resilience-plan-monotonic-units-per-second plan))
         (wait-timeout (or executor-timeout overall-timeout))
         (max-hedge-attempts (resilience-plan-max-hedge-attempts plan))
         (hedge-after (resilience-plan-hedge-after plan))
         (hedge-safe-p (resilience-plan-hedge-safe-p plan)))
    (cond
      (request-coalescer
       (call-with-request-coalescing
        request-coalescer
        (lambda ()
          (%run-resilience-base plan active-token active-handler))
        :key idempotency-key
        :idempotency-fingerprint
        idempotency-fingerprint
        :executor executor
        :hard-timeout hard-timeout
        :timeout wait-timeout
        :operation operation
        :clock clock
        :monotonic-units-per-second
        units))
      ((and max-hedge-attempts
            (> max-hedge-attempts 1))
       (call-with-hedging
        (lambda ()
          (%run-resilience-base plan active-token active-handler))
        :hedge-after (or hedge-after 0d0)
        :max-attempts max-hedge-attempts
        :executor executor
        :hard-timeout hard-timeout
        :hedge-safe-p hedge-safe-p
        :idempotency-key idempotency-key
        :cancellation-token active-token
        :operation operation
        :clock clock
        :monotonic-units-per-second
        units))
      (executor
       (resilience-executor-call
        executor
        (lambda ()
          (%run-resilience-base plan active-token active-handler))
        :hard-timeout hard-timeout
        :timeout wait-timeout
        :operation operation
        :clock clock
        :monotonic-units-per-second
        units))
      (hard-timeout
       (%run-with-hard-timeout
        (lambda ()
          (%run-resilience-base plan active-token active-handler))
        hard-timeout
        operation
        :thread
        :clock clock
        :monotonic-units-per-second
        units))
      (t
       (%run-resilience-base plan active-token active-handler)))))
