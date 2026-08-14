(in-package #:resilience-kit)

(defun %emit-circuit-breaker-event
    (active-handler event operation condition result reason clock units)
  (%emit-resilience-event*
   active-handler event operation nil nil condition result nil reason
   nil nil nil nil
   clock units))

(defun circuit-breaker-call
    (breaker thunk &key operation cancellation-token event-handler)
  "Call THUNK when admitted, otherwise signal CIRCUIT-OPEN.

The operation's error or first result is classified after the call and the
reservation is always released, including when a classifier itself fails or
THUNK exits through a non-local transfer.  Cancellation is cooperative and
does not count as a circuit failure."
  (check-type breaker circuit-breaker)
  (check-type thunk function)
  (when cancellation-token
    (check-type cancellation-token cancellation-token))
  (let ((active-token (%active-cancellation-token cancellation-token))
        (active-handler (%active-event-handler event-handler))
        (clock (%circuit-breaker-clock breaker))
        (units (%circuit-breaker-monotonic-units-per-second breaker))
        (condition-classifier (%circuit-breaker-condition-classifier breaker))
        (result-classifier (%circuit-breaker-result-classifier breaker))
        (finished-p nil))
    (when active-token
      (%check-cancellation-token active-token))
    (multiple-value-bind (admitted token-state token-generation rejection)
        (%circuit-breaker-begin breaker operation)
      (unless admitted
        (%emit-circuit-breaker-event
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
                        ;; A cooperative operation may observe cancellation
                        ;; while it is returning.  Treat that as cancellation,
                        ;; not as a circuit failure.
                        (ignored (%check-active-cancellation-token))
                        (result (%resilience-primary-value returned)))
                   (declare (ignore ignored))
                   (multiple-value-bind (failed-p classifier-error)
                     (%circuit-breaker-finish-classified
                        breaker token-state token-generation
                        result-classifier
                        result)
                     (setf finished-p t)
                     (if failed-p
                         (%emit-circuit-breaker-event
                          active-handler :circuit-failure operation nil result
                          :classified-failure clock units)
                         (%emit-circuit-breaker-event
                          active-handler :circuit-success operation nil result
                          nil clock units))
                     (when classifier-error
                       (error classifier-error))
                     (%unpack-resilience-values returned))))
               (resilience-cancelled (condition)
                 ;; Caller cancellation must not poison the circuit.
                 (%circuit-breaker-finish
                  breaker token-state token-generation nil)
                 (setf finished-p t)
                 (%emit-circuit-breaker-event
                  active-handler :cancelled operation condition nil
                  (resilience-cancelled-reason condition) clock units)
                 (error condition))
               (error (condition)
                 (multiple-value-bind (failed-p classifier-error)
                     (%circuit-breaker-finish-classified
                      breaker token-state token-generation
                      condition-classifier
                      condition)
                   (setf finished-p t)
                   (%emit-circuit-breaker-event
                    active-handler :circuit-failure operation condition nil
                    (if failed-p :classified-failure :observed-error)
                    clock units)
                   (when classifier-error
                     (error classifier-error))
                   (error condition)))))
        (unless finished-p
          ;; This also covers THROW, RETURN-FROM, and any other non-local exit
          ;; from THUNK or a callback after admission.
          (%circuit-breaker-finish
           breaker token-state token-generation t))))))

(defun circuit-breaker-reset (breaker)
  "Force BREAKER to CLOSED and invalidate older in-flight completions."
  (check-type breaker circuit-breaker)
  (cl-concurrent-kit:with-lock-held ((%circuit-breaker-lock breaker))
    (setf (%circuit-breaker-state breaker) :closed
          (%circuit-breaker-failure-count breaker) 0
          (%circuit-breaker-opened-at breaker) nil
          (%circuit-breaker-active-probes breaker) 0
          (%circuit-breaker-half-open-successes breaker) 0
          (%circuit-breaker-generation breaker)
          (1+ (%circuit-breaker-generation breaker))))
  breaker)

(defmacro with-circuit-breaker ((breaker &rest options) &body body)
  "Evaluate BODY through CIRCUIT-BREAKER-CALL."
  `(circuit-breaker-call ,breaker (lambda () ,@body) ,@options))
