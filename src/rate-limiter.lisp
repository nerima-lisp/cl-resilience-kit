(in-package #:resilience-kit)

(defmacro with-rate-limiter
    ((limiter &key (tokens 1d0) wait-p max-wait
                      (signal-on-reject-p nil) operation
                      cancellation-token event-handler)
     &body body)
  "Acquire tokens before evaluating BODY."
  `(when (rate-limiter-acquire ,limiter
                               :tokens ,tokens
                               :wait-p ,wait-p
                               :max-wait ,max-wait
                               :signal-on-reject-p ,signal-on-reject-p
                               :operation ,operation
                               :cancellation-token ,cancellation-token
                               :event-handler ,event-handler)
     (progn ,@body)))
