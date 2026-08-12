(in-package #:resilience-kit)

(defun %call-through-circuit-breaker
    (breaker breaker-type thunk operation cancellation-token event-handler
     begin-function finish-function finish-classified-function
     condition-classifier result-classifier clock
     monotonic-units-per-second)
  (unless (typep breaker breaker-type)
    (error 'type-error :datum breaker :expected-type breaker-type))
  (check-type thunk function)
  (when cancellation-token
    (check-type cancellation-token cancellation-token))
  (let ((active-token (%active-cancellation-token cancellation-token))
        (active-handler (%active-event-handler event-handler))
        (finished-p nil))
    (when active-token
      (check-cancellation-token active-token))
    (multiple-value-bind (admitted token-state token-generation rejection)
        (funcall begin-function)
      (unless admitted
        (%emit-resilience-event
         active-handler :circuit-rejected
         :operation operation :condition rejection :reason :open
         :clock clock
         :monotonic-units-per-second monotonic-units-per-second)
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
                         (funcall finish-function
                                  breaker token-state token-generation nil)
                         (setf finished-p t)
                         (%emit-resilience-event
                          active-handler :cancelled
                          :operation operation
                          :condition operation-condition
                          :reason
                          (resilience-cancelled-reason operation-condition)
                          :clock clock
                          :monotonic-units-per-second
                          monotonic-units-per-second)
                         (error operation-condition))
                       (multiple-value-bind (failed-p classifier-error)
                           (funcall finish-classified-function
                                    breaker token-state token-generation
                                    condition-classifier operation-condition)
                         (setf finished-p t)
                         (%emit-resilience-event
                          active-handler :circuit-failure
                          :operation operation
                          :condition operation-condition
                          :reason
                          (if failed-p
                              :classified-failure
                              :observed-error)
                          :clock clock
                          :monotonic-units-per-second
                          monotonic-units-per-second)
                         (when classifier-error
                           (error classifier-error))
                         (error operation-condition)))
                   (multiple-value-bind (failed-p classifier-error)
                       (funcall finish-classified-function
                                breaker token-state token-generation
                                result-classifier
                                (first returned))
                     (setf finished-p t)
                     (if failed-p
                         (%emit-resilience-event
                          active-handler :circuit-failure
                          :operation operation :result (first returned)
                          :reason :classified-failure
                          :clock clock
                          :monotonic-units-per-second
                          monotonic-units-per-second)
                         (%emit-resilience-event
                          active-handler :circuit-success
                          :operation operation :result (first returned)
                          :clock clock
                          :monotonic-units-per-second
                          monotonic-units-per-second))
                     (when classifier-error
                       (error classifier-error))
                     (apply #'values returned)))))
        (unless finished-p
          (funcall finish-function
                   breaker token-state token-generation t))))))

(defun circuit-breaker-call
    (breaker thunk &key operation cancellation-token event-handler)
  "Call THUNK when admitted, otherwise signal CIRCUIT-OPEN.

The operation's error or first result is classified after the call and the
reservation is always released, including when a classifier itself fails or
THUNK exits through a non-local transfer.  Cancellation is cooperative and
  does not count as a circuit failure."
  (%call-through-circuit-breaker
   breaker 'circuit-breaker thunk operation cancellation-token event-handler
   (lambda ()
     (%circuit-breaker-begin breaker operation))
   #'%circuit-breaker-finish
   #'%circuit-breaker-finish-classified
   (circuit-breaker-condition-classifier breaker)
   (circuit-breaker-result-classifier breaker)
   (circuit-breaker-clock breaker)
   (circuit-breaker-monotonic-units-per-second breaker)))

(defun circuit-breaker-reset (breaker)
  "Force BREAKER to CLOSED and invalidate older in-flight completions."
  (check-type breaker circuit-breaker)
  (with-lock-held ((%circuit-breaker-lock breaker))
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
  `(circuit-breaker-call ,breaker
                         (lambda () ,@body)
                         ,@options))
