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
                 (list :closed (%circuit-breaker-generation breaker))
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
                         (list :half-open
                               (%circuit-breaker-generation breaker))
                         nil))
               (values nil nil
                       (%circuit-open-condition
                        :open retry-at (%circuit-breaker-generation breaker)
                        operation)))))
        (:half-open
         (if (< (%circuit-breaker-active-probes breaker)
                (circuit-breaker-half-open-probe-limit breaker))
             (progn
               (incf (%circuit-breaker-active-probes breaker))
               (values t
                       (list :half-open
                             (%circuit-breaker-generation breaker))
                       nil))
             (values nil nil
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

(defun %circuit-breaker-finish (breaker token failed-p)
  (cl-concurrent-kit:with-lock-held ((%circuit-breaker-lock breaker))
    (let ((token-state (first token))
          (token-generation (second token)))
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
                           (1+ (%circuit-breaker-generation breaker)))))))))))))

(defun %circuit-breaker-finish-classified
    (breaker token classifier value)
  (let ((failed-p nil)
        (classifier-error nil))
    (handler-case
        (setf failed-p
              (and classifier
                   (not (null (funcall classifier value 1)))))
      (error (condition)
        (setf failed-p t
              classifier-error condition)))
    (%circuit-breaker-finish breaker token failed-p)
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
    (multiple-value-bind (admitted token rejection)
        (%circuit-breaker-begin breaker operation)
      (unless admitted
        (%emit-resilience-event
         active-handler :circuit-rejected
         :operation operation :condition rejection :reason :open
         :clock *resilience-clock*
         :monotonic-units-per-second
         *resilience-monotonic-units-per-second*)
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
                         (%circuit-breaker-finish breaker token nil)
                         (setf finished-p t)
                         (%emit-resilience-event
                          active-handler :cancelled
                          :operation operation :condition operation-condition
                          :reason (resilience-cancelled-reason
                                   operation-condition)
                          :clock *resilience-clock*
                          :monotonic-units-per-second
                          *resilience-monotonic-units-per-second*)
                         (error operation-condition))
                       (multiple-value-bind (failed-p classifier-error)
                           (%circuit-breaker-finish-classified
                            breaker token
                            (circuit-breaker-condition-classifier breaker)
                            operation-condition)
                         (setf finished-p t)
                         (%emit-resilience-event
                          active-handler :circuit-failure
                          :operation operation :condition operation-condition
                          :reason (if failed-p :classified-failure :observed-error)
                          :clock *resilience-clock*
                          :monotonic-units-per-second
                          *resilience-monotonic-units-per-second*)
                         (when classifier-error
                           (error classifier-error))
                         (error operation-condition)))
                   (multiple-value-bind (failed-p classifier-error)
                       (%circuit-breaker-finish-classified
                        breaker token
                        (circuit-breaker-result-classifier breaker)
                        (first returned))
                     (setf finished-p t)
                     (if failed-p
                         (%emit-resilience-event
                          active-handler :circuit-failure
                          :operation operation :result (first returned)
                          :reason :classified-failure
                          :clock *resilience-clock*
                          :monotonic-units-per-second
                          *resilience-monotonic-units-per-second*)
                         (%emit-resilience-event
                          active-handler :circuit-success
                          :operation operation :result (first returned)
                          :clock *resilience-clock*
                          :monotonic-units-per-second
                          *resilience-monotonic-units-per-second*))
                     (when classifier-error
                       (error classifier-error))
                     (apply #'values returned)))))
        (unless finished-p
          ;; This also covers THROW, RETURN-FROM, and any other non-local exit
          ;; from THUNK or a callback after admission.
          (%circuit-breaker-finish breaker token t))))))

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

;;; Distributed circuit breaker

(defclass distributed-circuit-breaker ()
  ((failure-threshold
    :initarg :failure-threshold
    :reader distributed-circuit-breaker-failure-threshold)
   (reset-timeout
    :initarg :reset-timeout
    :reader distributed-circuit-breaker-reset-timeout)
   (half-open-probe-limit
    :initarg :half-open-probe-limit
    :reader distributed-circuit-breaker-half-open-probe-limit)
   (success-threshold
    :initarg :success-threshold
    :reader distributed-circuit-breaker-success-threshold)
   (condition-classifier
    :initarg :condition-classifier
    :reader distributed-circuit-breaker-condition-classifier)
   (result-classifier
    :initarg :result-classifier
    :reader distributed-circuit-breaker-result-classifier)
   (clock
    :initarg :clock
    :reader distributed-circuit-breaker-clock)
   (monotonic-units-per-second
    :initarg :monotonic-units-per-second
    :reader distributed-circuit-breaker-monotonic-units-per-second)
   (store
    :initarg :store
    :reader distributed-circuit-breaker-store)
   (key
    :initarg :key
    :reader distributed-circuit-breaker-key)
   (lease-store
    :initarg :lease-store
    :reader distributed-circuit-breaker-lease-store)
   (lease-owner
    :initarg :lease-owner
    :reader distributed-circuit-breaker-lease-owner)
   (lease-ttl
    :initarg :lease-ttl
    :reader distributed-circuit-breaker-lease-ttl)))

(defun make-distributed-circuit-breaker
    (&key store key
          (failure-threshold 5)
          (reset-timeout 30)
          (half-open-probe-limit 1)
          (success-threshold 1)
          condition-classifier
          result-classifier
          clock
          monotonic-units-per-second
          lease-store
          lease-owner
          (lease-ttl 30))
  "Create a circuit breaker whose state is stored with versioned CAS.

STORE must provide the RESILIENCE-STATE-STORE protocol.  LEASE-STORE is an
optional fencing lease used to serialize the read/modify/write transitions
when a deployment wants an additional ownership boundary; the state-store
CAS remains the correctness boundary."
  (check-type store resilience-state-store)
  (check-type key string)
  (when (zerop (length key))
    (error "KEY must not be empty."))
  (%validate-positive-integer failure-threshold "FAILURE-THRESHOLD")
  (unless (and (%finite-real-p reset-timeout) (plusp reset-timeout))
    (error "RESET-TIMEOUT must be a positive real, got ~S." reset-timeout))
  (%validate-positive-integer half-open-probe-limit
                             "HALF-OPEN-PROBE-LIMIT")
  (%validate-positive-integer success-threshold "SUCCESS-THRESHOLD")
  (%function-or-nil condition-classifier "CONDITION-CLASSIFIER")
  (%function-or-nil result-classifier "RESULT-CLASSIFIER")
  (when lease-store
    (check-type lease-store resilience-lease-store)
    (when (null lease-owner)
      (error "LEASE-OWNER is required when LEASE-STORE is supplied."))
    (%ensure-positive-real lease-ttl "LEASE-TTL"))
  (make-instance
   'distributed-circuit-breaker
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
   :store store
   :key key
   :lease-store lease-store
   :lease-owner lease-owner
   :lease-ttl (float lease-ttl 1d0)))

(defun %distributed-circuit-breaker-now (breaker)
  (%monotonic-seconds
   (distributed-circuit-breaker-clock breaker)
   (distributed-circuit-breaker-monotonic-units-per-second breaker)))

(defun %distributed-circuit-breaker-default-state ()
  (list :state :closed
        :failure-count 0
        :opened-at nil
        :active-probes 0
        :half-open-successes 0
        :generation 0))

(defun %distributed-circuit-breaker-store-error (breaker message &optional cause)
  (error 'resilience-store-error
         :key (distributed-circuit-breaker-key breaker)
         :cause cause
         :message message))

(defun %distributed-circuit-breaker-normalize-state (breaker value)
  (unless (listp value)
    (%distributed-circuit-breaker-store-error
     breaker "The distributed circuit-breaker state is not a property list."
     value))
  (let ((state (getf value :state))
        (failure-count (getf value :failure-count))
        (opened-at (getf value :opened-at))
        (active-probes (getf value :active-probes))
        (half-open-successes (getf value :half-open-successes))
        (generation (getf value :generation)))
    (unless (member state '(:closed :open :half-open))
      (%distributed-circuit-breaker-store-error
       breaker "The distributed circuit-breaker state has an invalid state."
       value))
    (dolist (entry (list (cons :failure-count failure-count)
                         (cons :active-probes active-probes)
                         (cons :half-open-successes half-open-successes)
                         (cons :generation generation)))
      (unless (and (integerp (cdr entry)) (>= (cdr entry) 0))
        (%distributed-circuit-breaker-store-error
         breaker
         (format nil "The distributed circuit-breaker field ~S is invalid."
                 (car entry))
         value)))
    (when (and opened-at (not (%finite-real-p opened-at)))
      (%distributed-circuit-breaker-store-error
       breaker "The distributed circuit-breaker opened-at value is invalid."
       value))
    (list :state state
          :failure-count failure-count
          :opened-at opened-at
          :active-probes active-probes
          :half-open-successes half-open-successes
          :generation generation)))

(defun %distributed-circuit-breaker-read (breaker)
  (let ((store (distributed-circuit-breaker-store breaker))
        (key (distributed-circuit-breaker-key breaker)))
    (loop repeat 64 do
      (multiple-value-bind (value version)
          (state-store-get store key)
        (if version
            (return-from %distributed-circuit-breaker-read
              (values (%distributed-circuit-breaker-normalize-state
                       breaker value)
                      version))
            (handler-case
                (let* ((initial (%distributed-circuit-breaker-default-state))
                       (new-version
                         (state-store-put-if-version
                          store key initial nil)))
                  (return-from %distributed-circuit-breaker-read
                    (values initial new-version)))
              (resilience-store-conflict () nil)))))
    (%distributed-circuit-breaker-store-error
     breaker "Could not initialize or read the distributed circuit-breaker state.")))

(defun %distributed-circuit-breaker-with-lease (breaker thunk)
  (let ((lease-store (distributed-circuit-breaker-lease-store breaker)))
    (if lease-store
        (let ((lease
                (acquire-resilience-lease
                 lease-store
                 (distributed-circuit-breaker-key breaker)
                 (distributed-circuit-breaker-lease-owner breaker)
                 :ttl (distributed-circuit-breaker-lease-ttl breaker))))
          (unwind-protect
               (funcall thunk)
            (when lease
              (release-resilience-lease lease :ignore-lost-p t))))
        (funcall thunk))))

(defun %distributed-circuit-breaker-open-condition
    (state retry-at generation operation)
  (make-condition
   'circuit-open
   :message (if (eq state :half-open)
                "The distributed circuit breaker has no available half-open probe."
                "The distributed circuit breaker is open.")
   :operation operation
   :state state
   :retry-at retry-at
   :generation generation))

(defun %distributed-circuit-breaker-begin (breaker operation)
  (%distributed-circuit-breaker-with-lease
   breaker
   (lambda ()
     (let ((store (distributed-circuit-breaker-store breaker))
           (key (distributed-circuit-breaker-key breaker)))
       (loop repeat 64 do
         (multiple-value-bind (state version)
             (%distributed-circuit-breaker-read breaker)
           (let* ((now (%distributed-circuit-breaker-now breaker))
                  (current-state (getf state :state))
                  (generation (getf state :generation)))
             (case current-state
               (:closed
                (return-from %distributed-circuit-breaker-begin
                  (values t
                          (list :state :closed
                                :generation generation
                                :version version)
                          nil)))
               (:open
                (let* ((opened-at (getf state :opened-at))
                       (retry-at (and opened-at
                                      (+ opened-at
                                         (distributed-circuit-breaker-reset-timeout
                                          breaker)))))
                  (if (and retry-at (>= now retry-at))
                      (let ((next (copy-list state)))
                        (setf (getf next :state) :half-open
                              (getf next :active-probes) 1
                              (getf next :half-open-successes) 0
                              (getf next :failure-count) 0
                              (getf next :generation) (1+ generation))
                        (handler-case
                            (let ((new-version
                                    (state-store-put-if-version
                                     store key next version)))
                              (return-from %distributed-circuit-breaker-begin
                                (values t
                                        (list :state :half-open
                                              :generation (getf next :generation)
                                              :version new-version)
                                        nil)))
                          (resilience-store-conflict () nil)))
                      (return-from %distributed-circuit-breaker-begin
                        (values nil nil
                                (%distributed-circuit-breaker-open-condition
                                 :open retry-at generation operation))))))
               (:half-open
                (if (< (getf state :active-probes)
                       (distributed-circuit-breaker-half-open-probe-limit
                        breaker))
                    (let ((next (copy-list state)))
                      (incf (getf next :active-probes))
                      (handler-case
                          (let ((new-version
                                  (state-store-put-if-version
                                   store key next version)))
                            (return-from %distributed-circuit-breaker-begin
                              (values t
                                      (list :state :half-open
                                            :generation generation
                                            :version new-version)
                                      nil)))
                        (resilience-store-conflict () nil)))
                    (return-from %distributed-circuit-breaker-begin
                      (values nil nil
                              (%distributed-circuit-breaker-open-condition
                               :half-open nil generation operation)))))
               (otherwise
                (%distributed-circuit-breaker-store-error
                 breaker "The distributed circuit-breaker state is invalid.")))))))
       (%distributed-circuit-breaker-store-error
        breaker "Could not reserve a distributed circuit-breaker call."))))

(defun %distributed-circuit-breaker-finish
    (breaker token failed-p)
  (%distributed-circuit-breaker-with-lease
   breaker
   (lambda ()
     (let ((store (distributed-circuit-breaker-store breaker))
           (key (distributed-circuit-breaker-key breaker))
           (token-state (getf token :state))
           (token-generation (getf token :generation)))
       (loop repeat 64 do
         (multiple-value-bind (state version)
             (%distributed-circuit-breaker-read breaker)
           (when (/= token-generation (getf state :generation))
             (return-from %distributed-circuit-breaker-finish nil))
           (when (not (eq token-state (getf state :state)))
             (return-from %distributed-circuit-breaker-finish nil))
           (let ((next (copy-list state)))
             (case token-state
               (:closed
                (if failed-p
                    (let ((failure-count (1+ (getf state :failure-count))))
                      (if (>= failure-count
                              (distributed-circuit-breaker-failure-threshold
                               breaker))
                          (setf (getf next :state) :open
                                (getf next :opened-at)
                                (%distributed-circuit-breaker-now breaker)
                                (getf next :failure-count) 0
                                (getf next :active-probes) 0
                                (getf next :half-open-successes) 0
                                (getf next :generation)
                                (1+ token-generation))
                          (setf (getf next :failure-count) failure-count)))
                    (setf (getf next :failure-count) 0)))
               (:half-open
                (setf (getf next :active-probes)
                      (max 0 (1- (getf state :active-probes))))
                (if failed-p
                    (setf (getf next :state) :open
                          (getf next :opened-at)
                          (%distributed-circuit-breaker-now breaker)
                          (getf next :failure-count) 0
                          (getf next :active-probes) 0
                          (getf next :half-open-successes) 0
                          (getf next :generation) (1+ token-generation))
                    (let ((successes (1+ (getf state :half-open-successes))))
                      (if (>= successes
                              (distributed-circuit-breaker-success-threshold
                               breaker))
                          (setf (getf next :state) :closed
                                (getf next :opened-at) nil
                                (getf next :failure-count) 0
                                (getf next :active-probes) 0
                                (getf next :half-open-successes) 0
                                (getf next :generation) (1+ token-generation))
                          (setf (getf next :half-open-successes) successes)))))
               (otherwise
                (%distributed-circuit-breaker-store-error
                 breaker "The distributed circuit-breaker token is invalid.")))
             (handler-case
                 (return-from %distributed-circuit-breaker-finish
                   (state-store-put-if-version store key next version))
               (resilience-store-conflict () nil)))))
       (%distributed-circuit-breaker-store-error
        breaker "Could not record the distributed circuit-breaker result.")))))

(defun %distributed-circuit-breaker-finish-classified
    (breaker token classifier value)
  (let ((failed-p nil)
        (classifier-error nil))
    (handler-case
        (setf failed-p
              (and classifier
                   (not (null (funcall classifier value 1)))))
      (error (condition)
        (setf failed-p t
              classifier-error condition)))
    (%distributed-circuit-breaker-finish breaker token failed-p)
    (values failed-p classifier-error)))

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
         :clock *resilience-clock*
         :monotonic-units-per-second
         *resilience-monotonic-units-per-second*)
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
                          :clock *resilience-clock*
                          :monotonic-units-per-second
                          *resilience-monotonic-units-per-second*)
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
                          :clock *resilience-clock*
                          :monotonic-units-per-second
                          *resilience-monotonic-units-per-second*)
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
                          :clock *resilience-clock*
                          :monotonic-units-per-second
                          *resilience-monotonic-units-per-second*)
                         (%emit-resilience-event
                          active-handler :circuit-success
                          :operation operation :result (first returned)
                          :clock *resilience-clock*
                          :monotonic-units-per-second
                          *resilience-monotonic-units-per-second*))
                     (when classifier-error
                       (error classifier-error))
                     (apply #'values returned)))))
        (unless finished-p
          (%distributed-circuit-breaker-finish breaker token t))))))

(defun %distributed-circuit-breaker-snapshot (breaker)
  (nth-value 0 (%distributed-circuit-breaker-read breaker)))

(defun distributed-circuit-breaker-state (breaker)
  (check-type breaker distributed-circuit-breaker)
  (getf (%distributed-circuit-breaker-snapshot breaker) :state))

(defun distributed-circuit-breaker-failure-count (breaker)
  (check-type breaker distributed-circuit-breaker)
  (getf (%distributed-circuit-breaker-snapshot breaker) :failure-count))

(defun distributed-circuit-breaker-opened-at (breaker)
  (check-type breaker distributed-circuit-breaker)
  (getf (%distributed-circuit-breaker-snapshot breaker) :opened-at))

(defun distributed-circuit-breaker-active-probes (breaker)
  (check-type breaker distributed-circuit-breaker)
  (getf (%distributed-circuit-breaker-snapshot breaker) :active-probes))

(defun distributed-circuit-breaker-generation (breaker)
  (check-type breaker distributed-circuit-breaker)
  (getf (%distributed-circuit-breaker-snapshot breaker) :generation))

(defun distributed-circuit-breaker-reset (breaker)
  "Force the shared breaker to CLOSED and invalidate in-flight generations."
  (check-type breaker distributed-circuit-breaker)
  (%distributed-circuit-breaker-with-lease
   breaker
   (lambda ()
     (let ((store (distributed-circuit-breaker-store breaker))
           (key (distributed-circuit-breaker-key breaker)))
       (loop repeat 64 do
         (multiple-value-bind (state version)
             (%distributed-circuit-breaker-read breaker)
           (let ((next (copy-list state)))
             (setf (getf next :state) :closed
                   (getf next :failure-count) 0
                   (getf next :opened-at) nil
                   (getf next :active-probes) 0
                   (getf next :half-open-successes) 0
                   (getf next :generation) (1+ (getf state :generation)))
             (handler-case
                 (progn
                   (state-store-put-if-version store key next version)
                   (return-from distributed-circuit-breaker-reset breaker))
               (resilience-store-conflict () nil)))))
       (%distributed-circuit-breaker-store-error
        breaker "Could not reset the distributed circuit-breaker state.")))))

(defmacro with-distributed-circuit-breaker ((breaker &rest options) &body body)
  "Evaluate BODY through DISTRIBUTED-CIRCUIT-BREAKER-CALL."
  `(distributed-circuit-breaker-call
    ,breaker (lambda () ,@body) ,@options))
