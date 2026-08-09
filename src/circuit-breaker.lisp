(in-package #:cl-resilience-kit)

(defclass circuit-breaker ()
  ((failure-threshold
    :initarg :failure-threshold
    :reader circuit-breaker-failure-threshold)
   (reset-timeout
    :initarg :reset-timeout
    :reader circuit-breaker-reset-timeout)
   (half-open-probe-limit
    :initarg :half-open-probe-limit
    :reader circuit-breaker-half-open-probe-limit)
   (success-threshold
    :initarg :success-threshold
    :reader circuit-breaker-success-threshold)
   (condition-classifier
    :initarg :condition-classifier
    :reader circuit-breaker-condition-classifier)
   (result-classifier
    :initarg :result-classifier
    :reader circuit-breaker-result-classifier)
   (clock
    :initarg :clock
    :reader circuit-breaker-clock)
   (monotonic-units-per-second
    :initarg :monotonic-units-per-second
    :reader circuit-breaker-monotonic-units-per-second)
   (lock
    :initarg :lock
    :reader %circuit-breaker-lock)
   (%state
    :initform :closed
    :accessor %circuit-breaker-state)
   (%failure-count
    :initform 0
    :accessor %circuit-breaker-failure-count)
   (%opened-at
    :initform nil
    :accessor %circuit-breaker-opened-at)
   (%active-probes
    :initform 0
    :accessor %circuit-breaker-active-probes)
   (%half-open-successes
    :initform 0
    :accessor %circuit-breaker-half-open-successes)
   (%generation
    :initform 0
    :accessor %circuit-breaker-generation)))

(defun %validate-positive-integer (value name)
  (unless (and (integerp value) (>= value 1))
    (error "~A must be a positive integer, got ~S." name value))
  value)

(defun %circuit-breaker-now (breaker)
  (%monotonic-seconds
   (circuit-breaker-clock breaker)
   (circuit-breaker-monotonic-units-per-second breaker)))

(defun make-circuit-breaker
    (&key (failure-threshold 5)
          (reset-timeout 30)
          (half-open-probe-limit 1)
          (success-threshold 1)
          condition-classifier
          result-classifier
          clock
          monotonic-units-per-second)
  "Create a CLOSED circuit breaker.

CONDITION-CLASSIFIER and RESULT-CLASSIFIER receive `(value 1)`, where VALUE is
the condition or first returned value and the second argument identifies this
single circuit-breaker call.  A non-NIL classifier result is a failure.  The
  default condition classifier counts every error and the default result
  classifier treats returned results as successes.  State reservations and completion
updates are protected by one native CL-CONCURRENT-KIT lock; HALF-OPEN-PROBE-
LIMIT therefore bounds concurrent probes, not merely sequential calls."
  (%validate-positive-integer failure-threshold "FAILURE-THRESHOLD")
  (unless (and (%finite-real-p reset-timeout) (plusp reset-timeout))
    (error "RESET-TIMEOUT must be a positive real, got ~S." reset-timeout))
  (%validate-positive-integer half-open-probe-limit
                             "HALF-OPEN-PROBE-LIMIT")
  (%validate-positive-integer success-threshold "SUCCESS-THRESHOLD")
  (%function-or-nil condition-classifier "CONDITION-CLASSIFIER")
  (%function-or-nil result-classifier "RESULT-CLASSIFIER")
  (make-instance
   'circuit-breaker
   :failure-threshold failure-threshold
   :reset-timeout (float reset-timeout 1d0)
   :half-open-probe-limit half-open-probe-limit
   :success-threshold success-threshold
   :condition-classifier
   (or condition-classifier
       (lambda (condition attempt)
         (declare (ignore condition attempt))
         t))
   :result-classifier result-classifier
   :clock (%active-clock clock)
   :monotonic-units-per-second
   (%active-monotonic-units-per-second monotonic-units-per-second)
   :lock (cl-concurrent-kit:make-lock :name "cl-resilience-kit.circuit-breaker")))

(defun circuit-breaker-state (breaker)
  (check-type breaker circuit-breaker)
  (cl-concurrent-kit:with-lock-held ((%circuit-breaker-lock breaker))
    (%circuit-breaker-state breaker)))

(defun circuit-breaker-failure-count (breaker)
  (check-type breaker circuit-breaker)
  (cl-concurrent-kit:with-lock-held ((%circuit-breaker-lock breaker))
    (%circuit-breaker-failure-count breaker)))

(defun circuit-breaker-opened-at (breaker)
  (check-type breaker circuit-breaker)
  (cl-concurrent-kit:with-lock-held ((%circuit-breaker-lock breaker))
    (%circuit-breaker-opened-at breaker)))

(defun circuit-breaker-active-probes (breaker)
  (check-type breaker circuit-breaker)
  (cl-concurrent-kit:with-lock-held ((%circuit-breaker-lock breaker))
    (%circuit-breaker-active-probes breaker)))

(defun circuit-breaker-generation (breaker)
  (check-type breaker circuit-breaker)
  (cl-concurrent-kit:with-lock-held ((%circuit-breaker-lock breaker))
    (%circuit-breaker-generation breaker)))

(defun %circuit-open-condition
    (state retry-at generation operation)
  (make-condition
   'circuit-open
   :message (if (eq state :half-open)
                "The circuit breaker has no available half-open probe."
                "The circuit breaker is open.")
   :operation operation
   :state state
   :retry-at retry-at
   :generation generation))

(defun %circuit-breaker-begin (breaker operation)
  (cl-concurrent-kit:with-lock-held ((%circuit-breaker-lock breaker))
    (let ((now (%circuit-breaker-now breaker)))
      (case (%circuit-breaker-state breaker)
        (:closed
         (values t
                 :closed
                 (%circuit-breaker-generation breaker)
                 nil))
        (:open
         (let* ((opened-at (%circuit-breaker-opened-at breaker))
                (retry-at (and opened-at
                               (+ opened-at
                                  (circuit-breaker-reset-timeout breaker)))))
           (if (and retry-at (>= now retry-at))
               (progn
                 (setf (%circuit-breaker-state breaker) :half-open
                       (%circuit-breaker-generation breaker)
                       (1+ (%circuit-breaker-generation breaker))
                 (%circuit-breaker-active-probes breaker) 1
                       (%circuit-breaker-half-open-successes breaker) 0)
                 (values t
                         :half-open
                         (%circuit-breaker-generation breaker)
                         nil))
               (values nil nil nil
                       (%circuit-open-condition
                        :open retry-at (%circuit-breaker-generation breaker)
                        operation)))))
        (:half-open
         (if (< (%circuit-breaker-active-probes breaker)
                (circuit-breaker-half-open-probe-limit breaker))
             (progn
               (incf (%circuit-breaker-active-probes breaker))
               (values t
                       :half-open
                       (%circuit-breaker-generation breaker)
                       nil))
             (values nil nil nil
                     (%circuit-open-condition
                      :half-open nil (%circuit-breaker-generation breaker)
                      operation))))
        (otherwise
         (error "Unknown circuit breaker state ~S."
                (%circuit-breaker-state breaker)))))))

(defun %circuit-breaker-open! (breaker now)
  (setf (%circuit-breaker-state breaker) :open
        (%circuit-breaker-opened-at breaker) now
        (%circuit-breaker-failure-count breaker) 0
        (%circuit-breaker-active-probes breaker) 0
        (%circuit-breaker-half-open-successes breaker) 0
        (%circuit-breaker-generation breaker)
        (1+ (%circuit-breaker-generation breaker))))

(defun %circuit-breaker-finish
    (breaker token-state token-generation failed-p)
  (cl-concurrent-kit:with-lock-held ((%circuit-breaker-lock breaker))
    ;; A completion from an obsolete generation cannot overwrite a newer
    ;; reset, close, or reopen transition.
    (when (= token-generation (%circuit-breaker-generation breaker))
      (case token-state
          (:closed
           (when (eq (%circuit-breaker-state breaker) :closed)
             (if failed-p
                 (progn
                   (incf (%circuit-breaker-failure-count breaker))
                   (when (>= (%circuit-breaker-failure-count breaker)
                             (circuit-breaker-failure-threshold breaker))
                     (%circuit-breaker-open!
                      breaker (%circuit-breaker-now breaker))))
                 (setf (%circuit-breaker-failure-count breaker) 0))))
          (:half-open
           (when (eq (%circuit-breaker-state breaker) :half-open)
             (setf (%circuit-breaker-active-probes breaker)
                   (max 0 (1- (%circuit-breaker-active-probes breaker))))
             (if failed-p
                 (%circuit-breaker-open!
                  breaker (%circuit-breaker-now breaker))
                 (progn
                   (incf (%circuit-breaker-half-open-successes breaker))
                   (when (>= (%circuit-breaker-half-open-successes breaker)
                             (circuit-breaker-success-threshold breaker))
                     (setf (%circuit-breaker-state breaker) :closed
                           (%circuit-breaker-opened-at breaker) nil
                           (%circuit-breaker-failure-count breaker) 0
                           (%circuit-breaker-active-probes breaker) 0
                           (%circuit-breaker-half-open-successes breaker) 0
                           (%circuit-breaker-generation breaker)
                           (1+ (%circuit-breaker-generation breaker))))))))))))

(defun %circuit-breaker-finish-classified
    (breaker token-state token-generation classifier value)
  (let ((failed-p nil)
        (classifier-error nil))
    (handler-case
        (setf failed-p
              (and classifier
                   (not (null (funcall classifier value 1)))))
      (error (condition)
        (setf failed-p t
              classifier-error condition)))
    (%circuit-breaker-finish
     breaker token-state token-generation failed-p)
    (values failed-p classifier-error)))

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
