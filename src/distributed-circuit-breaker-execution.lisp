(in-package #:resilience-kit)

;;; Distributed circuit breaker execution

(defun %emit-distributed-circuit-breaker-event
    (active-handler event operation condition result reason clock units)
  (%emit-resilience-event*
   active-handler event operation nil nil condition result nil reason
   nil nil nil nil
   clock units))

(defun distributed-circuit-breaker-call
    (breaker thunk &key operation cancellation-token event-handler)
  "Call THUNK through a versioned, shared circuit-breaker state."
  (check-type breaker distributed-circuit-breaker)
  (check-type thunk function)
  (when cancellation-token
    (check-type cancellation-token cancellation-token))
  (let ((active-token (%active-cancellation-token cancellation-token))
        (active-handler (%active-event-handler event-handler))
        (clock (%distributed-circuit-breaker-clock breaker))
        (units
          (%distributed-circuit-breaker-monotonic-units-per-second breaker))
        (condition-classifier
          (%distributed-circuit-breaker-condition-classifier breaker))
        (result-classifier
          (%distributed-circuit-breaker-result-classifier breaker))
        (finished-p nil))
    (when active-token
      (%check-cancellation-token active-token))
    (multiple-value-bind (admitted token-state token-generation rejection)
        (%distributed-circuit-breaker-begin breaker operation)
      (unless admitted
        (%emit-distributed-circuit-breaker-event
         active-handler :circuit-rejected operation rejection nil :open
         clock units)
        (error rejection))
      (unwind-protect
           (let ((*resilience-cancellation-token* active-token)
                 (*resilience-event-handler* active-handler))
             (handler-case
               (progn
                 (%check-active-cancellation-token)
                 (let* ((returned (%pack-resilience-values (funcall thunk)))
                        (ignored (%check-active-cancellation-token))
                        (result (%resilience-primary-value returned)))
                   (declare (ignore ignored))
                   (multiple-value-bind (failed-p classifier-error)
                       (%distributed-circuit-breaker-finish-classified
                       breaker token-state token-generation
                        result-classifier
                        result)
                     (setf finished-p t)
                     (if failed-p
                         (%emit-distributed-circuit-breaker-event
                          active-handler :circuit-failure operation nil result
                          :classified-failure clock units)
                         (%emit-distributed-circuit-breaker-event
                          active-handler :circuit-success operation nil result
                          nil clock units))
                     (when classifier-error
                       (error classifier-error))
                     (%unpack-resilience-values returned))))
               (resilience-cancelled (condition)
                 (%distributed-circuit-breaker-finish
                  breaker token-state token-generation nil)
                 (setf finished-p t)
                 (%emit-distributed-circuit-breaker-event
                  active-handler :cancelled operation condition nil
                  (resilience-cancelled-reason condition) clock units)
                 (error condition))
               (error (condition)
                 (multiple-value-bind (failed-p classifier-error)
                     (%distributed-circuit-breaker-finish-classified
                      breaker token-state token-generation
                      condition-classifier
                      condition)
                   (setf finished-p t)
                   (%emit-distributed-circuit-breaker-event
                    active-handler :circuit-failure operation condition nil
                    (if failed-p :classified-failure :observed-error)
                    clock units)
                   (when classifier-error
                     (error classifier-error))
                   (error condition)))))
        (unless finished-p
          (%distributed-circuit-breaker-finish
           breaker token-state token-generation t))))))
