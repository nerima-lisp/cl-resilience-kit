(in-package #:resilience-kit)

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
