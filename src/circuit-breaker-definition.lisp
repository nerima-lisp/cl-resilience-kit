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
