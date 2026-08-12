(in-package #:resilience-kit)

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

(define-condition resilience-draining (resilience-error)
  ((state
    :initarg :state
    :initform :draining
    :reader resilience-draining-state)))

(define-condition resilience-observer-warning (warning)
  ((phase
    :initarg :phase
    :reader resilience-observer-warning-phase)
   (handler
    :initarg :handler
    :reader resilience-observer-warning-handler)
   (event
    :initarg :event
    :reader resilience-observer-warning-event)
   (cause
    :initarg :cause
    :reader resilience-observer-warning-cause))
  (:report
   (lambda (condition stream)
     (format stream
             "Resilience ~A failed for ~S: ~A"
             (resilience-observer-warning-phase condition)
             (resilience-observer-warning-handler condition)
             (resilience-observer-warning-cause condition)))))
