(in-package #:cl-resilience-kit)

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
        (finished-p nil))
    (when active-token
      (check-cancellation-token active-token))
    (multiple-value-bind (admitted token-state token-generation rejection)
        (%circuit-breaker-begin breaker operation)
      (unless admitted
        (%emit-resilience-event
         active-handler :circuit-rejected
         :operation operation :condition rejection :reason :open
         :clock (circuit-breaker-clock breaker)
         :monotonic-units-per-second
         (circuit-breaker-monotonic-units-per-second breaker))
        (error rejection))
      (unwind-protect
           (let ((*resilience-cancellation-token* active-token)
                 (*resilience-event-handler* active-handler))
             (multiple-value-bind (returned operation-condition)
                 (handler-case
                     (progn
                       (%check-active-cancellation-token)
                       (let ((returned (multiple-value-list (funcall thunk))))
                         ;; A cooperative operation may observe cancellation
                         ;; while it is returning.  Treat that as cancellation,
                         ;; not as a circuit failure.
                         (%check-active-cancellation-token)
                         (values returned nil)))
                   (error (condition)
                     (values nil condition)))
               (if operation-condition
                   (if (typep operation-condition 'resilience-cancelled)
                       (progn
                         ;; Caller cancellation must not poison the circuit.
                         (%circuit-breaker-finish
                          breaker token-state token-generation nil)
                         (setf finished-p t)
                         (%emit-resilience-event
                          active-handler :cancelled
                          :operation operation :condition operation-condition
                          :reason (resilience-cancelled-reason
                                   operation-condition)
                          :clock (circuit-breaker-clock breaker)
                          :monotonic-units-per-second
                          (circuit-breaker-monotonic-units-per-second breaker))
                         (error operation-condition))
                       (multiple-value-bind (failed-p classifier-error)
                           (%circuit-breaker-finish-classified
                            breaker token-state token-generation
                            (circuit-breaker-condition-classifier breaker)
                            operation-condition)
                         (setf finished-p t)
                         (%emit-resilience-event
                          active-handler :circuit-failure
                          :operation operation :condition operation-condition
                          :reason (if failed-p :classified-failure :observed-error)
                          :clock (circuit-breaker-clock breaker)
                          :monotonic-units-per-second
                          (circuit-breaker-monotonic-units-per-second breaker))
                         (when classifier-error
                           (error classifier-error))
                         (error operation-condition)))
                   (multiple-value-bind (failed-p classifier-error)
                       (%circuit-breaker-finish-classified
                        breaker token-state token-generation
                        (circuit-breaker-result-classifier breaker)
                        (first returned))
                     (setf finished-p t)
                     (if failed-p
                         (%emit-resilience-event
                          active-handler :circuit-failure
                          :operation operation :result (first returned)
                          :reason :classified-failure
                          :clock (circuit-breaker-clock breaker)
                          :monotonic-units-per-second
                          (circuit-breaker-monotonic-units-per-second breaker))
                         (%emit-resilience-event
                          active-handler :circuit-success
                          :operation operation :result (first returned)
                          :clock (circuit-breaker-clock breaker)
                          :monotonic-units-per-second
                          (circuit-breaker-monotonic-units-per-second breaker)))
                     (when classifier-error
                       (error classifier-error))
                     (apply #'values returned)))))
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
