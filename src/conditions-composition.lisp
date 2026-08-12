(in-package #:resilience-kit)

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
