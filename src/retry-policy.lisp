(in-package #:resilience-kit)

(defstruct (retry-decision
             (:constructor %make-retry-decision (retry-p delay-hint reason))
             (:predicate retry-decision-p))
  "The normalized answer returned by a retry classifier."
  retry-p
  delay-hint
  reason)

(setf (fdefinition 'make-retry-decision)
      (lambda (&rest options)
        (when (oddp (length options))
          (error "MAKE-RETRY-DECISION requires an even number of keyword arguments, got ~S."
                 options))
        (let ((retry-p nil)
              (delay-hint nil)
              (reason nil))
          (declare (optimize (speed 3) (safety 1) (debug 0)))
          (loop for (key value) on options by #'cddr
                do (case key
                     (:retry-p
                      (setf retry-p value))
                     (:delay-hint
                      (setf delay-hint value))
                     (:reason
                      (setf reason value))
                     (otherwise
                      (error "Unknown MAKE-RETRY-DECISION keyword ~S." key))))
          (%make-retry-decision retry-p delay-hint reason))))

(defstruct (retry-policy
             (:constructor %make-retry-policy
                 (max-attempts initial-delay multiplier max-delay jitter
                  retry-safe-p condition-classifier result-classifier
                  random-source))
             (:conc-name %retry-policy-)
             (:predicate retry-policy-p))
  "Validated retry-policy configuration."
  max-attempts
  initial-delay
  multiplier
  max-delay
  jitter
  retry-safe-p
  condition-classifier
  result-classifier
  random-source)

(defun retry-policy-max-attempts (policy)
  (%retry-policy-max-attempts policy))

(defun retry-policy-initial-delay (policy)
  (%retry-policy-initial-delay policy))

(defun retry-policy-multiplier (policy)
  (%retry-policy-multiplier policy))

(defun retry-policy-max-delay (policy)
  (%retry-policy-max-delay policy))

(defun retry-policy-jitter (policy)
  (%retry-policy-jitter policy))

(defun retry-policy-retry-safe-p (policy)
  (%retry-policy-retry-safe-p policy))

(defun retry-policy-condition-classifier (policy)
  (%retry-policy-condition-classifier policy))

(defun retry-policy-result-classifier (policy)
  (%retry-policy-result-classifier policy))

(defun retry-policy-random-source (policy)
  (%retry-policy-random-source policy))

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
  (let ((args (%retry-policy-initargs
               :max-attempts max-attempts
               :initial-delay initial-delay
               :multiplier multiplier
               :max-delay max-delay
               :jitter jitter
               :condition-classifier condition-classifier
               :result-classifier result-classifier
               :retry-safe-p retry-safe-p
               :random-source random-source)))
    (%make-retry-policy (getf args :max-attempts)
                        (getf args :initial-delay)
                        (getf args :multiplier)
                        (getf args :max-delay)
                        (getf args :jitter)
                        (getf args :retry-safe-p)
                        (getf args :condition-classifier)
                        (getf args :result-classifier)
                        (getf args :random-source))))

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
