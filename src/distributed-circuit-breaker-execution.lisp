(in-package #:cl-resilience-kit)

;;; Distributed circuit breaker execution

(defun distributed-circuit-breaker-call
    (breaker thunk &key operation cancellation-token event-handler)
  "Call THUNK through a versioned, shared circuit-breaker state."
  (check-type breaker distributed-circuit-breaker)
  (check-type thunk function)
  (when cancellation-token
    (check-type cancellation-token cancellation-token))
  (let ((active-token (%active-cancellation-token cancellation-token))
        (active-handler (%active-event-handler event-handler))
        (finished-p nil))
    (when active-token
      (check-cancellation-token active-token))
    (multiple-value-bind (admitted token rejection)
        (%distributed-circuit-breaker-begin breaker operation)
      (unless admitted
        (%emit-resilience-event
         active-handler :circuit-rejected
         :operation operation :condition rejection :reason :open
         :clock (distributed-circuit-breaker-clock breaker)
         :monotonic-units-per-second
         (distributed-circuit-breaker-monotonic-units-per-second breaker))
        (error rejection))
      (unwind-protect
           (let ((*resilience-cancellation-token* active-token)
                 (*resilience-event-handler* active-handler))
             (multiple-value-bind (returned operation-condition)
                 (handler-case
                     (progn
                       (%check-active-cancellation-token)
                       (let ((returned (multiple-value-list (funcall thunk))))
                         (%check-active-cancellation-token)
                         (values returned nil)))
                   (error (condition)
                     (values nil condition)))
               (if operation-condition
                   (if (typep operation-condition 'resilience-cancelled)
                       (progn
                         (%distributed-circuit-breaker-finish breaker token nil)
                         (setf finished-p t)
                         (%emit-resilience-event
                          active-handler :cancelled
                          :operation operation :condition operation-condition
                          :reason (resilience-cancelled-reason
                                   operation-condition)
                          :clock (distributed-circuit-breaker-clock breaker)
                          :monotonic-units-per-second
                          (distributed-circuit-breaker-monotonic-units-per-second breaker))
                         (error operation-condition))
                       (multiple-value-bind (failed-p classifier-error)
                           (%distributed-circuit-breaker-finish-classified
                            breaker token
                            (distributed-circuit-breaker-condition-classifier
                             breaker)
                            operation-condition)
                         (setf finished-p t)
                         (%emit-resilience-event
                          active-handler :circuit-failure
                          :operation operation :condition operation-condition
                          :reason (if failed-p :classified-failure :observed-error)
                          :clock (distributed-circuit-breaker-clock breaker)
                          :monotonic-units-per-second
                          (distributed-circuit-breaker-monotonic-units-per-second breaker))
                         (when classifier-error
                           (error classifier-error))
                         (error operation-condition)))
                   (multiple-value-bind (failed-p classifier-error)
                       (%distributed-circuit-breaker-finish-classified
                        breaker token
                        (distributed-circuit-breaker-result-classifier breaker)
                        (first returned))
                     (setf finished-p t)
                     (if failed-p
                         (%emit-resilience-event
                          active-handler :circuit-failure
                          :operation operation :result (first returned)
                          :reason :classified-failure
                          :clock (distributed-circuit-breaker-clock breaker)
                          :monotonic-units-per-second
                          (distributed-circuit-breaker-monotonic-units-per-second breaker))
                         (%emit-resilience-event
                          active-handler :circuit-success
                          :operation operation :result (first returned)
                          :clock (distributed-circuit-breaker-clock breaker)
                          :monotonic-units-per-second
                          (distributed-circuit-breaker-monotonic-units-per-second breaker)))
                     (when classifier-error
                       (error classifier-error))
                     (apply #'values returned)))))
        (unless finished-p
          (%distributed-circuit-breaker-finish breaker token t))))))
