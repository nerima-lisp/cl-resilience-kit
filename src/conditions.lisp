(in-package #:cl-resilience-kit)

(define-condition resilience-error (error)
  ((operation
    :initarg :operation
    :initform nil
    :reader resilience-error-operation)
   (message
    :initarg :message
    :initform "A resilience policy rejected or interrupted the operation."
    :reader resilience-error-message))
  (:report
   (lambda (condition stream)
     (format stream "~A~@[ (operation ~A)~]"
             (resilience-error-message condition)
             (resilience-error-operation condition)))))

(define-condition resilience-cancelled (resilience-error)
  ((token
    :initarg :token
    :initform nil
    :reader resilience-cancelled-token)
   (reason
    :initarg :reason
    :initform :cancelled
    :reader resilience-cancelled-reason)))

(define-condition retry-exhausted (resilience-error)
  ((attempts
    :initarg :attempts
    :reader retry-exhausted-attempts)
   (last-condition
    :initarg :last-condition
    :initform nil
    :reader retry-exhausted-last-condition)
   (last-result
    :initarg :last-result
    :initform nil
    :reader retry-exhausted-last-result)
   (reason
    :initarg :reason
    :initform :max-attempts
    :reader retry-exhausted-reason)
   (policy
    :initarg :policy
    :reader retry-exhausted-policy)))

(define-condition retry-classifier-error (resilience-error)
  ((cause
    :initarg :cause
    :reader retry-classifier-error-cause))
  (:report
   (lambda (condition stream)
     (format stream "The retry classifier failed: ~A"
             (retry-classifier-error-cause condition)))))

(define-condition deadline-exceeded (resilience-error)
  ((deadline
    :initarg :deadline
    :reader deadline-exceeded-deadline)
   (observed-at
    :initarg :observed-at
    :reader deadline-exceeded-observed-at)
   (stage
    :initarg :stage
    :initform :operation
    :reader deadline-exceeded-stage)
   (attempt
    :initarg :attempt
    :initform nil
    :reader deadline-exceeded-attempt)))

(define-condition attempt-timeout (deadline-exceeded)
  ((timeout
    :initarg :timeout
    :reader attempt-timeout-timeout)))

(define-condition circuit-open (resilience-error)
  ((state
    :initarg :state
    :initform :open
    :reader circuit-open-state)
   (retry-at
    :initarg :retry-at
    :initform nil
    :reader circuit-open-retry-at)
   (generation
    :initarg :generation
    :initform nil
    :reader circuit-open-generation)))

(define-condition bulkhead-rejected (resilience-error)
  ((limit
    :initarg :limit
    :reader bulkhead-rejected-limit)
   (in-flight
    :initarg :in-flight
    :reader bulkhead-rejected-in-flight)))

(define-condition rate-limit-exceeded (resilience-error)
  ((requested-tokens
    :initarg :requested-tokens
    :reader rate-limit-exceeded-requested-tokens)
   (available-tokens
    :initarg :available-tokens
    :reader rate-limit-exceeded-available-tokens)
   (retry-after
    :initarg :retry-after
    :initform nil
    :reader rate-limit-exceeded-retry-after)))

(define-condition resilience-store-error (resilience-error)
  ((key
    :initarg :key
    :reader resilience-store-error-key)
   (cause
    :initarg :cause
    :initform nil
    :reader resilience-store-error-cause)))

(define-condition resilience-store-conflict (resilience-store-error)
  ((expected-version
    :initarg :expected-version
    :initform nil
    :reader resilience-store-conflict-expected-version)
   (actual-version
    :initarg :actual-version
    :initform nil
    :reader resilience-store-conflict-actual-version)))

(define-condition resilience-lease-unavailable (resilience-error)
  ((key
    :initarg :key
    :reader resilience-lease-unavailable-key)
   (owner
    :initarg :owner
    :reader resilience-lease-unavailable-owner)
   (retry-after
    :initarg :retry-after
    :initform nil
    :reader resilience-lease-unavailable-retry-after)))

(define-condition resilience-lease-lost (resilience-error)
  ((key
    :initarg :key
    :reader resilience-lease-lost-key)
   (owner
    :initarg :owner
    :reader resilience-lease-lost-owner)
   (fencing-token
    :initarg :fencing-token
    :initform nil
    :reader resilience-lease-lost-fencing-token)))

(define-condition stale-fencing-token (resilience-error)
  ((key
    :initarg :key
    :reader stale-fencing-token-key)
   (fencing-token
    :initarg :fencing-token
    :reader stale-fencing-token-fencing-token)
   (current-fencing-token
    :initarg :current-fencing-token
    :reader stale-fencing-token-current-fencing-token)))

(define-condition resilience-draining (resilience-error)
  ((state
    :initarg :state
    :initform :draining
    :reader resilience-draining-state)))

(define-condition resilience-execution-rejected (resilience-error)
  ((reason
    :initarg :reason
    :initform :rejected
    :reader resilience-execution-rejected-reason)
   (queue-size
    :initarg :queue-size
    :initform nil
    :reader resilience-execution-rejected-queue-size)))

(define-condition resilience-execution-timeout (resilience-error)
  ((timeout
    :initarg :timeout
    :reader resilience-execution-timeout-timeout)
   (backend
    :initarg :backend
    :initform nil
    :reader resilience-execution-timeout-backend)))

(define-condition resilience-hard-timeout (attempt-timeout)
  ((backend
    :initarg :backend
    :initform nil
    :reader resilience-hard-timeout-backend)))

(define-condition hedge-unsafe (resilience-error)
  ((reason
    :initarg :reason
    :initform :non-idempotent-operation
    :reader hedge-unsafe-reason)))

(define-condition hedge-exhausted (resilience-error)
  ((causes
    :initarg :causes
    :initform nil
    :reader hedge-exhausted-causes)
   (attempts
    :initarg :attempts
    :reader hedge-exhausted-attempts)))

(define-condition idempotency-key-required (resilience-error)
  ())

(define-condition idempotency-conflict (resilience-error)
  ((key
    :initarg :key
    :reader idempotency-conflict-key)
   (existing-value
    :initarg :existing-value
    :initform nil
    :reader idempotency-conflict-existing-value)))
