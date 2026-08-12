(in-package #:resilience-kit)

;;; Distributed circuit breaker execution

(defun distributed-circuit-breaker-call
    (breaker thunk &key operation cancellation-token event-handler)
  "Call THUNK through a versioned, shared circuit-breaker state."
  (%call-through-circuit-breaker
   breaker 'distributed-circuit-breaker
   thunk operation cancellation-token event-handler
   (lambda ()
     (%distributed-circuit-breaker-begin breaker operation))
   #'%distributed-circuit-breaker-finish
   #'%distributed-circuit-breaker-finish-classified
   (distributed-circuit-breaker-condition-classifier breaker)
   (distributed-circuit-breaker-result-classifier breaker)
   (distributed-circuit-breaker-clock breaker)
   (distributed-circuit-breaker-monotonic-units-per-second breaker)))
