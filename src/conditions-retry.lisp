(in-package #:resilience-kit)

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
