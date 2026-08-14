(in-package #:resilience-kit)

;;; Distributed circuit breaker data and construction

(defclass distributed-circuit-breaker ()
  ((failure-threshold
    :initarg :failure-threshold
    :reader distributed-circuit-breaker-failure-threshold
    :reader %distributed-circuit-breaker-failure-threshold)
   (reset-timeout
    :initarg :reset-timeout
    :reader distributed-circuit-breaker-reset-timeout
    :reader %distributed-circuit-breaker-reset-timeout)
   (half-open-probe-limit
    :initarg :half-open-probe-limit
    :reader distributed-circuit-breaker-half-open-probe-limit
    :reader %distributed-circuit-breaker-half-open-probe-limit)
   (success-threshold
    :initarg :success-threshold
    :reader distributed-circuit-breaker-success-threshold
    :reader %distributed-circuit-breaker-success-threshold)
   (condition-classifier
    :initarg :condition-classifier
    :reader distributed-circuit-breaker-condition-classifier
    :reader %distributed-circuit-breaker-condition-classifier)
   (result-classifier
    :initarg :result-classifier
    :reader distributed-circuit-breaker-result-classifier
    :reader %distributed-circuit-breaker-result-classifier)
   (clock
    :initarg :clock
    :reader distributed-circuit-breaker-clock
    :reader %distributed-circuit-breaker-clock)
   (monotonic-units-per-second
    :initarg :monotonic-units-per-second
    :reader distributed-circuit-breaker-monotonic-units-per-second
    :reader %distributed-circuit-breaker-monotonic-units-per-second)
   (store
    :initarg :store
    :reader distributed-circuit-breaker-store
    :reader %distributed-circuit-breaker-store)
   (key
    :initarg :key
    :reader distributed-circuit-breaker-key
    :reader %distributed-circuit-breaker-key)
   (lease-store
    :initarg :lease-store
    :reader distributed-circuit-breaker-lease-store
    :reader %distributed-circuit-breaker-lease-store)
   (lease-owner
    :initarg :lease-owner
    :reader distributed-circuit-breaker-lease-owner
    :reader %distributed-circuit-breaker-lease-owner)
   (lease-ttl
    :initarg :lease-ttl
    :reader distributed-circuit-breaker-lease-ttl
    :reader %distributed-circuit-breaker-lease-ttl)))

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
