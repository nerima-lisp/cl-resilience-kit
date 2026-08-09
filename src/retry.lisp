(in-package #:cl-resilience-kit)

(defun %normalize-jitter (jitter)
  (let ((value (or jitter :none)))
    (unless (member value '(:none :full :equal :decorrelated) :test #'eq)
      (error "JITTER must be one of :NONE, :FULL, :EQUAL, or :DECORRELATED, got ~S."
             jitter))
    value))

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

MAX-ATTEMPTS includes the initial call.  A classifier is called with
`(value attempt)` and may return a boolean, a non-negative numeric delay hint,
or a RETRY-DECISION.  No classifier is consulted unless RETRY-SAFE-P is true;
callers must opt in explicitly for operations whose effects may be repeated."
  (unless (and (integerp max-attempts) (>= max-attempts 1))
    (error "MAX-ATTEMPTS must be a positive integer, got ~S." max-attempts))
  (unless (and (%finite-real-p initial-delay) (>= initial-delay 0))
    (error "INITIAL-DELAY must be a non-negative real, got ~S." initial-delay))
  (unless (and (%finite-real-p multiplier) (>= multiplier 1))
    (error "MULTIPLIER must be a real greater than or equal to 1, got ~S."
           multiplier))
  (unless (or (null max-delay)
              (and (%finite-real-p max-delay) (>= max-delay 0)))
    (error "MAX-DELAY must be NIL or a non-negative real, got ~S." max-delay))
  (%function-or-nil condition-classifier "CONDITION-CLASSIFIER")
  (%function-or-nil result-classifier "RESULT-CLASSIFIER")
  (make-instance
   'retry-policy
   :max-attempts max-attempts
   :initial-delay (float initial-delay 1d0)
   :multiplier (float multiplier 1d0)
   :max-delay (and max-delay (float max-delay 1d0))
   :jitter (%normalize-jitter jitter)
   :retry-safe-p (not (null retry-safe-p))
   :condition-classifier condition-classifier
   :result-classifier result-classifier
   :random-source (%active-random-source random-source)))

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
already the final allowed attempt.  The second value is the classifier's
normalized RETRY-DECISION, even at the attempt limit, so callers can explain
why an operation was exhausted.  CONDITION takes precedence over RESULT; it
is an error to supply neither value."
  (check-type policy retry-policy)
  (unless (and (integerp attempt) (>= attempt 1))
    (error "ATTEMPT must be a positive integer, got ~S." attempt))
  (unless (or condition result-p)
    (error "Supply CONDITION or RESULT to RETRY-POLICY-SHOULD-RETRY-P."))
  (let ((decision (%retry-decision-for policy attempt
                                       :condition condition
                                       :result result)))
    (values (and (< attempt (retry-policy-max-attempts policy))
                 (retry-decision-retry-p decision))
            decision)))

(defun %capped-delay (policy delay)
  (let ((non-negative (%ensure-non-negative-real delay "RETRY-DELAY")))
    (if (retry-policy-max-delay policy)
        (min (retry-policy-max-delay policy) non-negative)
        non-negative)))

(defun %bounded-product (left right)
  (let ((left (float left 1d0))
        (right (float right 1d0)))
    (if (or (zerop left) (zerop right)
            (<= left (/ most-positive-double-float right)))
        (* left right)
        most-positive-double-float)))

(defun %bounded-exponential-delay (policy retry-number)
  (let* ((limit (or (retry-policy-max-delay policy)
                    most-positive-double-float))
         (value (min limit (retry-policy-initial-delay policy)))
         (multiplier (retry-policy-multiplier policy)))
    (cond ((zerop value) 0d0)
          ((= multiplier 1d0) value)
          (t
           ;; Exponentiation by squaring keeps a very large retry number from
           ;; turning delay calculation into an unbounded linear loop.
           (let ((exponent (1- retry-number))
                 (factor multiplier))
             (loop while (plusp exponent)
                   do (when (oddp exponent)
                        (setf value
                              (min limit (%bounded-product value factor))))
                      (when (or (= value limit)
                                (= exponent 1))
                        (return))
                      (setf exponent (floor exponent 2)
                            factor
                            (min limit (%bounded-product factor factor))))
             value)))))

(defun %random-below (random-source upper-bound)
  (if (<= upper-bound 0d0)
      0d0
      (* upper-bound (%random-unit random-source))))

(defun compute-backoff-delay (policy retry-number &key previous-delay)
  "Compute the delay before RETRY-NUMBER, whose first value is one.

The exponential base is capped by MAX-DELAY before jitter and the final
result is capped again.  Jitter is one of :NONE, :FULL, :EQUAL, or
:DECORRELATED and consumes only the injected boundary random source."
  (check-type policy retry-policy)
  (unless (and (integerp retry-number) (>= retry-number 1))
    (error "RETRY-NUMBER must be a positive integer, got ~S." retry-number))
  (when (and previous-delay
             (or (not (%finite-real-p previous-delay))
                 (minusp previous-delay)))
    (error "PREVIOUS-DELAY must be NIL or a non-negative real, got ~S."
           previous-delay))
  (let* ((base (%bounded-exponential-delay policy retry-number))
         (jitter (retry-policy-jitter policy))
         (random-source (retry-policy-random-source policy)))
    (%capped-delay
     policy
     (case jitter
       (:none base)
       (:full (%random-below random-source base))
       (:equal (+ (/ base 2d0)
                  (%random-below random-source (/ base 2d0))))
       (:decorrelated
        (let* ((lower (retry-policy-initial-delay policy))
               (previous (or previous-delay lower))
               (upper (max lower (%bounded-product 3d0 previous)))
               (upper (if (retry-policy-max-delay policy)
                          (min upper (retry-policy-max-delay policy))
                          upper)))
          (if (<= upper lower)
              lower
              (+ lower (%random-below random-source (- upper lower))))))
       (otherwise
        (error "Unknown jitter strategy ~S." jitter))))))

(defun %delay-with-hint (policy attempt previous-delay decision)
  (let ((computed (compute-backoff-delay policy attempt
                                         :previous-delay previous-delay))
        (hint (retry-decision-delay-hint decision)))
    (when (and hint (or (not (%finite-real-p hint)) (minusp hint)))
      (error "A retry delay hint must be a non-negative real, got ~S." hint))
    (%capped-delay policy (if hint (max computed hint) computed))))
