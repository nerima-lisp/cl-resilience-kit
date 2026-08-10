(in-package #:cl-resilience-kit)

(defun %call-with-resilience-core
    (thunk &key retry-policy circuit-breaker bulkhead bulkhead-timeout rate-limiter
                 (rate-limit-tokens 1d0) rate-limit-wait-p rate-limit-max-wait
                 (rate-limit-signal-on-reject-p t)
                 overall-timeout overall-deadline per-attempt-timeout
                 clock monotonic-units-per-second sleeper operation
                 retry-budget cancellation-token event-handler fallback)
  "Compose the resilience controls around THUNK.

The fixed nesting order is BULKHEAD (the complete logical operation) -> RETRY
(the attempt loop) -> RATE-LIMITER (each attempt) -> CIRCUIT-BREAKER (each
attempt) -> THUNK.  The supplied clock, unit scale, and sleeper should be the
same boundary objects used by the configured primitives when a deterministic
composition is required.

When RATE-LIMITER is present, rejection signals RATE-LIMIT-EXCEEDED by
default.  Set RATE-LIMIT-SIGNAL-ON-REJECT-P to NIL to return
`(VALUES NIL RETRY-AFTER)` from the composed operation without signaling.
The retry result classifier receives the first value, NIL; use signal mode
when the retry policy needs to classify RATE-LIMIT-EXCEEDED and its
retry-after hint."
  (check-type thunk function)
  (let* ((active-token (%active-cancellation-token cancellation-token))
         (attempt-thunk
          (lambda ()
            (flet ((run-breaker ()
                     (if circuit-breaker
                         (circuit-breaker-call circuit-breaker thunk
                                               :operation operation
                                               :cancellation-token
                                               active-token
                                               :event-handler event-handler)
                         (funcall thunk))))
              (if rate-limiter
                  (multiple-value-bind (acquired-p retry-after)
                      (rate-limiter-acquire
                       rate-limiter
                       :tokens rate-limit-tokens
                       :wait-p rate-limit-wait-p
                       :max-wait rate-limit-max-wait
                       :signal-on-reject-p rate-limit-signal-on-reject-p
                       :operation operation
                       :cancellation-token active-token
                       :event-handler event-handler)
                    (if acquired-p
                        (run-breaker)
                        (values nil retry-after)))
                  (run-breaker)))))
        (policy (or retry-policy
                    (make-retry-policy :max-attempts 1))))
    (flet ((run-chain ()
             (call-with-retry
              policy attempt-thunk
              :overall-timeout overall-timeout
              :overall-deadline overall-deadline
              :per-attempt-timeout per-attempt-timeout
              :clock clock
              :monotonic-units-per-second monotonic-units-per-second
              :sleeper sleeper
              :operation operation
              :retry-budget retry-budget
              :cancellation-token active-token
              :event-handler event-handler
              :fallback fallback)))
      (if bulkhead
          (progn
            ;; The bulkhead wraps the complete logical operation. Its
            ;; post-return cancellation check must not turn a value returned
            ;; by RETRY's fallback into a second cancellation. Check before
            ;; admission, then let the inner retry boundary own the token.
            (when active-token
              (check-cancellation-token active-token))
            (let ((*resilience-cancellation-token* nil))
              (bulkhead-call bulkhead #'run-chain
                             :operation operation
                             :cancellation-token nil
                             :event-handler event-handler
                             :timeout bulkhead-timeout)))
          (run-chain)))))
