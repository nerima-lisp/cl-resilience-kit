(in-package #:cl-resilience-kit)

(defun %combined-resilience-handler (event-handler metrics observer)
  (let ((handlers
          (remove nil
                  (list event-handler
                        (and (null event-handler)
                             *resilience-event-handler*)
                        (and metrics (resilience-metrics-handler metrics))
                        (and observer (resilience-observer-handler observer))))))
    (cond ((null handlers) nil)
          ((null (rest handlers)) (first handlers))
          (t (lambda (event)
               (dolist (handler handlers)
                 (funcall handler event)))))))

(defun call-with-resilience
    (thunk &key retry-policy circuit-breaker distributed-circuit-breaker
                 bulkhead bulkhead-timeout rate-limiter
                 (rate-limit-tokens 1d0) rate-limit-wait-p rate-limit-max-wait
                 (rate-limit-signal-on-reject-p t)
                 overall-timeout overall-deadline per-attempt-timeout
                 clock monotonic-units-per-second sleeper operation
                 retry-budget cancellation-token event-handler fallback
                 context metrics observer lifecycle executor executor-timeout
                 hard-timeout hedge-after max-hedge-attempts hedge-safe-p
                 request-coalescer idempotency-key idempotency-fingerprint)
  "Compose local and distributed resilience controls around THUNK.

The existing retry/bulkhead/rate-limit/circuit-breaker contract is preserved.
EXECUTOR, HARD-TIMEOUT, HEDGE-AFTER, and REQUEST-COALESCER add optional
execution-boundary controls; hedging and coalescing require idempotency
protection and do not cancel already running loser work."
  (check-type thunk function)
  (when context
    (check-type context resilience-context))
  (when lifecycle
    (check-type lifecycle resilience-lifecycle))
  (when executor
    (check-type executor resilience-executor))
  (let* ((base-context
           (merge-resilience-context
            (current-resilience-context)
            (make-resilience-context
             :operation operation
             :idempotency-key idempotency-key)))
         (active-context (if context
                            (merge-resilience-context base-context context)
                            base-context))
         (active-handler (%combined-resilience-handler
                          event-handler metrics observer))
         (active-token (%active-cancellation-token cancellation-token))
         (entered-p nil)
         (started (%now :clock clock
                        :monotonic-units-per-second
                        monotonic-units-per-second)))
    (when lifecycle
      (enter-resilience-lifecycle lifecycle :operation operation)
      (setf entered-p t))
    (unwind-protect
         (let ((*resilience-context* active-context)
               (*resilience-event-handler* active-handler))
           (labels ((run-base ()
                      (let ((operation-thunk
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
                                 monotonic-units-per-second
                                 :sleeper sleeper
                                 :operation operation
                                 :retry-budget retry-budget
                                 :cancellation-token active-token
                                 :event-handler active-handler
                                 :fallback fallback))))
                        (if distributed-circuit-breaker
                            (distributed-circuit-breaker-call
                             distributed-circuit-breaker operation-thunk
                             :operation operation
                             :cancellation-token active-token
                             :event-handler active-handler)
                            (funcall operation-thunk))))
                    (run-execution ()
                      (cond
                        (request-coalescer
                         (call-with-request-coalescing
                          request-coalescer #'run-base
                          :key idempotency-key
                          :idempotency-fingerprint idempotency-fingerprint
                          :executor executor
                          :hard-timeout hard-timeout
                          :timeout (or executor-timeout overall-timeout)
                          :operation operation
                          :clock clock
                          :monotonic-units-per-second
                          monotonic-units-per-second))
                        ((and max-hedge-attempts (> max-hedge-attempts 1))
                         (call-with-hedging
                          #'run-base
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
                          monotonic-units-per-second))
                        (executor
                         (resilience-executor-call
                          executor #'run-base
                          :hard-timeout hard-timeout
                          :timeout (or executor-timeout overall-timeout)
                          :operation operation
                          :clock clock
                          :monotonic-units-per-second
                          monotonic-units-per-second))
                        (hard-timeout
                         (%run-with-hard-timeout
                          #'run-base hard-timeout operation :thread
                          :clock clock
                          :monotonic-units-per-second
                          monotonic-units-per-second))
                        (t (run-base)))))
             (handler-case
                 (multiple-value-prog1
                     (run-execution)
                   (%emit-resilience-event
                    active-handler :operation-complete
                    :operation operation
                    :stage :composition
                    :context active-context
                    :duration (- (%now :clock clock
                                       :monotonic-units-per-second
                                       monotonic-units-per-second)
                                 started)
                    :clock clock
                    :monotonic-units-per-second
                    monotonic-units-per-second))
               (error (condition)
                 (%emit-resilience-event
                  active-handler :operation-failed
                  :operation operation
                  :stage :composition
                  :condition condition
                  :context active-context
                  :duration (- (%now :clock clock
                                     :monotonic-units-per-second
                                     monotonic-units-per-second)
                               started)
                  :clock clock
                  :monotonic-units-per-second
                  monotonic-units-per-second)
                 (error condition)))))
      (when entered-p
        (leave-resilience-lifecycle lifecycle)))))
