(in-package #:resilience-kit)

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

(defmacro %ensure-retry-policy-option (test format-control value)
  `(unless ,test
     (error ,format-control ,value)))

(defun %normalize-jitter (jitter)
  (let ((value (or jitter :none)))
    (%ensure-retry-policy-option
     (member value '(:none :full :equal :decorrelated) :test #'eq)
     "JITTER must be one of :NONE, :FULL, :EQUAL, or :DECORRELATED, got ~S."
     jitter)
    value))

(defun %retry-policy-initargs
    (&key (max-attempts 1)
          (initial-delay 0)
          (multiplier 2)
          max-delay
          (jitter :none)
          condition-classifier
          result-classifier
          (retry-safe-p nil)
          random-source)
  (%ensure-retry-policy-option
   (and (integerp max-attempts) (>= max-attempts 1))
   "MAX-ATTEMPTS must be a positive integer, got ~S."
   max-attempts)
  (%ensure-retry-policy-option
   (and (%finite-real-p initial-delay) (>= initial-delay 0))
   "INITIAL-DELAY must be a non-negative real, got ~S."
   initial-delay)
  (%ensure-retry-policy-option
   (and (%finite-real-p multiplier) (>= multiplier 1))
   "MULTIPLIER must be a real greater than or equal to 1, got ~S."
   multiplier)
  (%ensure-retry-policy-option
   (or (null max-delay)
       (and (%finite-real-p max-delay) (>= max-delay 0)))
   "MAX-DELAY must be NIL or a non-negative real, got ~S."
   max-delay)
  (%function-or-nil condition-classifier "CONDITION-CLASSIFIER")
  (%function-or-nil result-classifier "RESULT-CLASSIFIER")
  (list :max-attempts max-attempts
        :initial-delay (float initial-delay 1d0)
        :multiplier (float multiplier 1d0)
        :max-delay (and max-delay (float max-delay 1d0))
        :jitter (%normalize-jitter jitter)
        :retry-safe-p (not (null retry-safe-p))
        :condition-classifier condition-classifier
        :result-classifier result-classifier
        :random-source (%active-random-source random-source)))

(defun make-retry-policy
    (&key (max-attempts 1)
          (initial-delay 0)
          (multiplier 2)
          max-delay
          (jitter :none)
          condition-classifier
          result-classifier
          (retry-safe-p nil)
          random-source)
  "Create an explicit, fail-closed retry policy.

MAX-ATTEMPTS includes the initial call. A classifier is called with
`(value attempt)` and may return a boolean, a non-negative numeric delay hint,
or a RETRY-DECISION. No classifier is consulted unless RETRY-SAFE-P is true;
callers must opt in explicitly for operations whose effects may be repeated."
  (apply #'make-instance
         'retry-policy
         (%retry-policy-initargs
          :max-attempts max-attempts
          :initial-delay initial-delay
          :multiplier multiplier
          :max-delay max-delay
          :jitter jitter
          :condition-classifier condition-classifier
          :result-classifier result-classifier
          :retry-safe-p retry-safe-p
          :random-source random-source)))

(defun %classifier-decision (classifier value attempt)
  (handler-case
      (if classifier
          (destructuring-bind (answer &optional delay-hint reason)
              (multiple-value-list (funcall classifier value attempt))
            (cond ((typep answer 'retry-decision)
                   answer)
                  ((and answer (%finite-real-p answer))
                   (make-retry-decision :retry-p t :delay-hint answer
                                        :reason reason))
                  (t
                   (make-retry-decision :retry-p (not (null answer))
                                        :delay-hint delay-hint
                                        :reason reason))))
          (make-retry-decision :reason :no-classifier))
    (error (condition)
      (error 'retry-classifier-error
             :message "The retry classifier signaled an error."
             :cause condition))))

(defun %retry-decision-for
    (policy attempt &key condition (result nil result-p))
  (if (retry-policy-retry-safe-p policy)
      (cond (condition
             (%classifier-decision
              (retry-policy-condition-classifier policy) condition attempt))
            (result-p
             (%classifier-decision
              (retry-policy-result-classifier policy) result attempt))
            (t
             (make-retry-decision :reason :no-classifiable-value)))
      (make-retry-decision :reason :not-retry-safe)))

(defun retry-policy-should-retry-p
    (policy attempt &key condition (result nil result-p))
  "Classify one result or condition and return two values.

The first value is false when the policy rejects retrying or when ATTEMPT is
already the final allowed attempt. The second value is the classifier's
normalized RETRY-DECISION, even at the attempt limit, so callers can explain
why an operation was exhausted. CONDITION takes precedence over RESULT; it is
an error to supply neither value."
  (check-type policy retry-policy)
  (%ensure-retry-policy-option
   (and (integerp attempt) (>= attempt 1))
   "ATTEMPT must be a positive integer, got ~S."
   attempt)
  (unless (or condition result-p)
    (error "Supply CONDITION or RESULT to RETRY-POLICY-SHOULD-RETRY-P."))
  (let ((decision (%retry-decision-for policy attempt
                                       :condition condition
                                       :result result)))
    (values (and (< attempt (retry-policy-max-attempts policy))
                 (retry-decision-retry-p decision))
            decision)))
