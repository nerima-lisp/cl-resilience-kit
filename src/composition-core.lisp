(in-package #:resilience-kit)

(defparameter +single-attempt-retry-policy+
  (load-time-value
   (make-retry-policy :max-attempts 1)
   t))

(declaim (inline %call-with-resilience-retry
                 %call-with-resilience-single-pass-eligible-p
                 %call-with-resilience-core*))

(defun %call-with-resilience-retry
    (policy attempt-thunk overall-timeout overall-deadline per-attempt-timeout
     clock monotonic-units-per-second sleeper operation retry-budget
     cancellation-token event-handler fallback)
  (%call-with-retry*
   policy attempt-thunk overall-timeout overall-deadline per-attempt-timeout
   clock monotonic-units-per-second sleeper operation retry-budget
   cancellation-token event-handler fallback))

(defun %call-with-resilience-single-pass-eligible-p
    (policy circuit-breaker bulkhead rate-limiter retry-budget
     event-handler fallback)
  (and (null circuit-breaker)
       (null bulkhead)
       (null rate-limiter)
       (null retry-budget)
       (null event-handler)
       (null fallback)
       (= (%retry-policy-max-attempts policy) 1)
       (not (%retry-policy-retry-safe-p policy))))

(defun %call-with-resilience-core*
    (thunk retry-policy circuit-breaker bulkhead bulkhead-timeout rate-limiter
     rate-limit-tokens rate-limit-wait-p rate-limit-max-wait
     rate-limit-signal-on-reject-p overall-timeout overall-deadline
     per-attempt-timeout clock monotonic-units-per-second sleeper operation
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
  (let ((policy (or retry-policy
                    +single-attempt-retry-policy+)))
    (check-type policy retry-policy)
    (when cancellation-token
      (check-type cancellation-token cancellation-token))
    (when fallback
      (check-type fallback function))
    (when retry-budget
      (check-type retry-budget retry-budget))
    (when (and overall-timeout overall-deadline)
      (error "Specify OVERALL-TIMEOUT or OVERALL-DEADLINE, not both."))
    (when overall-timeout
      (setf overall-timeout
            (%ensure-non-negative-real overall-timeout "OVERALL-TIMEOUT")))
    (when overall-deadline
      (unless (%finite-real-p overall-deadline)
        (error "OVERALL-DEADLINE must be a finite real, got ~S."
               overall-deadline))
      (setf overall-deadline (float overall-deadline 1d0)))
    (when per-attempt-timeout
      (setf per-attempt-timeout
            (%ensure-non-negative-real per-attempt-timeout
                                       "PER-ATTEMPT-TIMEOUT")))
    (let* ((active-token (%active-cancellation-token cancellation-token)))
      (if (%call-with-resilience-single-pass-eligible-p
           policy circuit-breaker bulkhead rate-limiter retry-budget
           event-handler fallback)
          (let* ((active-clock (%active-clock clock))
                 (units
                   (%active-monotonic-units-per-second
                    monotonic-units-per-second))
                 (requested-deadline
                   (cond (overall-timeout
                          (+ (%monotonic-seconds active-clock units)
                             overall-timeout))
                         (overall-deadline overall-deadline)
                         (t nil)))
                 (effective-deadline
                   (%effective-deadline requested-deadline)))
            (%call-with-retry-single-pass
             thunk per-attempt-timeout operation active-clock units
             effective-deadline active-token))
          (let ((attempt-thunk
          (cond
            ((and rate-limiter circuit-breaker)
             (if rate-limit-signal-on-reject-p
                 (lambda ()
                   (rate-limiter-acquire
                    rate-limiter
                    :tokens rate-limit-tokens
                    :wait-p rate-limit-wait-p
                    :max-wait rate-limit-max-wait
                    :signal-on-reject-p t
                    :operation operation
                    :cancellation-token active-token
                    :event-handler event-handler)
                   (circuit-breaker-call circuit-breaker thunk
                                         :operation operation
                                         :cancellation-token active-token
                                         :event-handler event-handler))
                 (lambda ()
                   (multiple-value-bind (acquired-p retry-after)
                       (rate-limiter-acquire
                        rate-limiter
                        :tokens rate-limit-tokens
                        :wait-p rate-limit-wait-p
                        :max-wait rate-limit-max-wait
                        :signal-on-reject-p nil
                        :operation operation
                        :cancellation-token active-token
                        :event-handler event-handler)
                     (if acquired-p
                         (circuit-breaker-call circuit-breaker thunk
                                               :operation operation
                                               :cancellation-token active-token
                                               :event-handler event-handler)
                         (values nil retry-after))))))
            (rate-limiter
             (if rate-limit-signal-on-reject-p
                 (lambda ()
                   (rate-limiter-acquire
                    rate-limiter
                    :tokens rate-limit-tokens
                    :wait-p rate-limit-wait-p
                    :max-wait rate-limit-max-wait
                    :signal-on-reject-p t
                    :operation operation
                    :cancellation-token active-token
                    :event-handler event-handler)
                   (funcall thunk))
                 (lambda ()
                   (multiple-value-bind (acquired-p retry-after)
                       (rate-limiter-acquire
                        rate-limiter
                        :tokens rate-limit-tokens
                        :wait-p rate-limit-wait-p
                        :max-wait rate-limit-max-wait
                        :signal-on-reject-p nil
                        :operation operation
                        :cancellation-token active-token
                        :event-handler event-handler)
                     (if acquired-p
                         (funcall thunk)
                         (values nil retry-after))))))
            (circuit-breaker
             (lambda ()
               (circuit-breaker-call circuit-breaker thunk
                                     :operation operation
                                     :cancellation-token active-token
                                     :event-handler event-handler)))
            (t thunk))))
            (if bulkhead
                (let ((run-chain
                        (lambda ()
                          (%call-with-resilience-retry
                           policy attempt-thunk
                           overall-timeout overall-deadline per-attempt-timeout
                           clock monotonic-units-per-second sleeper operation
                           retry-budget active-token event-handler fallback))))
                  ;; The bulkhead wraps the complete logical operation. Its
                  ;; post-return cancellation check must not turn a value returned
                  ;; by RETRY's fallback into a second cancellation. Check before
                  ;; admission, then let the inner retry boundary own the token.
                  (when active-token
                    (check-cancellation-token active-token))
                  (let ((*resilience-cancellation-token* nil))
                    (bulkhead-call bulkhead run-chain
                                   :operation operation
                                   :cancellation-token nil
                                   :event-handler event-handler
                                   :timeout bulkhead-timeout)))
                 (%call-with-resilience-retry
                  policy attempt-thunk
                  overall-timeout overall-deadline per-attempt-timeout
                  clock monotonic-units-per-second sleeper operation
                  retry-budget active-token event-handler fallback)))))))

(defun %call-with-resilience-core
    (thunk &key retry-policy circuit-breaker bulkhead bulkhead-timeout rate-limiter
                 (rate-limit-tokens 1d0) rate-limit-wait-p rate-limit-max-wait
                 (rate-limit-signal-on-reject-p t)
                 overall-timeout overall-deadline per-attempt-timeout
                 clock monotonic-units-per-second sleeper operation
                 retry-budget cancellation-token event-handler fallback)
  (%call-with-resilience-core*
   thunk retry-policy circuit-breaker bulkhead bulkhead-timeout rate-limiter
   rate-limit-tokens rate-limit-wait-p rate-limit-max-wait
   rate-limit-signal-on-reject-p overall-timeout overall-deadline
   per-attempt-timeout clock monotonic-units-per-second sleeper operation
   retry-budget cancellation-token event-handler fallback))
