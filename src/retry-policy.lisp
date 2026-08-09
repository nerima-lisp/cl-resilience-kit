(in-package #:cl-resilience-kit)

(defstruct (retry-decision
            (:constructor make-retry-decision
                (&key (retry-p nil) delay-hint reason)))
  "The normalized answer returned by a retry classifier."
  retry-p
  delay-hint
  reason)

(defclass retry-policy ()
  ((max-attempts
    :initarg :max-attempts
    :reader retry-policy-max-attempts)
   (initial-delay
    :initarg :initial-delay
    :reader retry-policy-initial-delay)
   (multiplier
    :initarg :multiplier
    :reader retry-policy-multiplier)
   (max-delay
    :initarg :max-delay
    :reader retry-policy-max-delay)
   (jitter
    :initarg :jitter
    :reader retry-policy-jitter)
   (retry-safe-p
    :initarg :retry-safe-p
    :reader retry-policy-retry-safe-p)
   (condition-classifier
    :initarg :condition-classifier
    :reader retry-policy-condition-classifier)
   (result-classifier
    :initarg :result-classifier
    :reader retry-policy-result-classifier)
   (random-source
    :initarg :random-source
    :reader retry-policy-random-source)))

(defun %function-or-nil (value name)
  (when (and value (not (functionp value)))
    (error "~A must be a function or NIL, got ~S." name value))
  value)
