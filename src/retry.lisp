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

(defclass retry-budget ()
  ((limit
    :initarg :limit
    :reader retry-budget-limit)
   (window
    :initarg :window
    :reader retry-budget-window)
   (clock
    :initarg :clock
    :reader %retry-budget-clock)
   (monotonic-units-per-second
    :initarg :monotonic-units-per-second
    :reader %retry-budget-monotonic-units-per-second)
   (%used
    :initform 0
    :accessor %retry-budget-used)
   (%window-start
    :initarg :window-start
    :accessor %retry-budget-window-start)
   (%lock
    :initarg :lock
    :reader %retry-budget-lock)))

(defun make-retry-budget
    (&key limit window clock monotonic-units-per-second)
  "Create a lock-protected monotonic fixed-window retry budget.

LIMIT is the maximum number of authorized retries in each WINDOW.  The
initial attempt does not consume a token; RETRY-BUDGET-ACQUIRE consumes one
only when it returns true.  The optional CLOCK and unit scale are captured at
construction time so tests and applications can inject a monotonic source."
  (unless (and (integerp limit) (>= limit 1))
    (error "LIMIT must be a positive integer, got ~S." limit))
  (unless (and (%finite-real-p window) (plusp window))
    (error "WINDOW must be a positive real, got ~S." window))
  (let* ((active-clock (%active-clock clock))
         (units (%active-monotonic-units-per-second
                 monotonic-units-per-second)))
    (make-instance
     'retry-budget
     :limit limit
     :window (float window 1d0)
     :clock active-clock
     :monotonic-units-per-second units
     :window-start (%monotonic-seconds active-clock units)
     :lock (cl-concurrent-kit:make-lock :name "cl-resilience-kit.retry-budget"))))

(defclass distributed-retry-budget (retry-budget)
  ((store
    :initarg :store
    :reader distributed-retry-budget-store)
   (key
    :initarg :key
    :reader distributed-retry-budget-key)))

(defun make-distributed-retry-budget
    (&key store key limit window clock monotonic-units-per-second)
  "Create a retry budget backed by a compare-and-set STATE-STORE.

The store value is an implementation detail consisting of a window start and
the consumed token count.  Every successful acquisition is committed with a
compare-and-set, so independent processes sharing STORE and KEY observe one
budget.  The store must provide atomic versioned writes; a plain key/value
store is not sufficient for this contract."
  (check-type store resilience-state-store)
  (check-type key string)
  (unless (and (integerp limit) (>= limit 1))
    (error "LIMIT must be a positive integer, got ~S." limit))
  (unless (and (%finite-real-p window) (plusp window))
    (error "WINDOW must be a positive real, got ~S." window))
  (let* ((active-clock (%active-clock clock))
         (units (%active-monotonic-units-per-second
                 monotonic-units-per-second)))
    (make-instance
     'distributed-retry-budget
     :limit limit
     :window (float window 1d0)
     :clock active-clock
     :monotonic-units-per-second units
     :window-start (%monotonic-seconds active-clock units)
     :lock (cl-concurrent-kit:make-lock
            :name "cl-resilience-kit.distributed-retry-budget")
     :store store
     :key key)))

(defun %distributed-retry-budget-now (budget)
  (%monotonic-seconds
   (%retry-budget-clock budget)
   (%retry-budget-monotonic-units-per-second budget)))

(defun %distributed-retry-budget-state (budget value now)
  (let ((window-start (and (listp value) (getf value :window-start)))
        (used (and (listp value) (getf value :used))))
    (if (and (%finite-real-p window-start)
             (integerp used)
             (>= used 0))
        (if (>= (- now window-start) (retry-budget-window budget))
            (list :window-start now :used 0)
            (list :window-start (float window-start 1d0) :used used))
        (if (null value)
            (list :window-start now :used 0)
            (error 'resilience-store-error
                   :key (distributed-retry-budget-key budget)
                   :message "The distributed retry budget record is malformed.
Expected a plist with numeric :WINDOW-START and non-negative integer :USED.")))))

(defun %distributed-retry-budget-read (budget)
  (multiple-value-bind (value version)
      (state-store-get (distributed-retry-budget-store budget)
                       (distributed-retry-budget-key budget))
    (let ((now (%distributed-retry-budget-now budget)))
      (values (%distributed-retry-budget-state budget value now)
              version
              now))))

(defun %distributed-retry-budget-write (budget state version)
  (state-store-put-if-version
   (distributed-retry-budget-store budget)
   (distributed-retry-budget-key budget)
   state
   version))

(defun %distributed-retry-budget-update (budget update)
  (loop repeat 64
        do (multiple-value-bind (state version now)
               (%distributed-retry-budget-read budget)
             (multiple-value-bind (new-state result)
                 (funcall update state now)
               (if (null new-state)
                   (return-from %distributed-retry-budget-update result)
                   (handler-case
                       (progn
                         (%distributed-retry-budget-write budget
                                                           new-state
                                                           version)
                         (return-from %distributed-retry-budget-update result))
                     (resilience-store-conflict (condition)
                       (declare (ignore condition))))))))
  (error 'resilience-store-error
         :key (distributed-retry-budget-key budget)
         :message "The distributed retry budget could not commit after repeated store conflicts."))

(defun %retry-budget-refresh! (budget now)
  (when (>= (- now (%retry-budget-window-start budget))
            (retry-budget-window budget))
    (setf (%retry-budget-window-start budget) now
          (%retry-budget-used budget) 0))
  budget)

(defun retry-budget-used (budget)
  "Return the number of retry tokens consumed in the active window."
  (check-type budget retry-budget)
  (when (typep budget 'distributed-retry-budget)
    (cl-concurrent-kit:with-lock-held ((%retry-budget-lock budget))
      (multiple-value-bind (state version now)
          (%distributed-retry-budget-read budget)
        (declare (ignore version))
        (return-from retry-budget-used
          (getf (%distributed-retry-budget-state budget state now)
                :used)))))
  (cl-concurrent-kit:with-lock-held ((%retry-budget-lock budget))
    (%retry-budget-refresh!
     budget
     (%monotonic-seconds
      (%retry-budget-clock budget)
      (%retry-budget-monotonic-units-per-second budget)))
    (%retry-budget-used budget)))

(defun retry-budget-remaining (budget)
  "Return the number of retry tokens still available in the window."
  (check-type budget retry-budget)
  (when (typep budget 'distributed-retry-budget)
    (cl-concurrent-kit:with-lock-held ((%retry-budget-lock budget))
      (multiple-value-bind (state version now)
          (%distributed-retry-budget-read budget)
        (declare (ignore version))
        (return-from retry-budget-remaining
          (- (retry-budget-limit budget)
             (getf (%distributed-retry-budget-state budget state now)
                   :used))))))
  (cl-concurrent-kit:with-lock-held ((%retry-budget-lock budget))
    (%retry-budget-refresh!
     budget
     (%monotonic-seconds
      (%retry-budget-clock budget)
      (%retry-budget-monotonic-units-per-second budget)))
    (- (retry-budget-limit budget) (%retry-budget-used budget))))

(defun retry-budget-acquire (budget)
  "Authorize one retry and consume one token, or return NIL when exhausted."
  (check-type budget retry-budget)
  (when (typep budget 'distributed-retry-budget)
    (return-from retry-budget-acquire
      (cl-concurrent-kit:with-lock-held ((%retry-budget-lock budget))
        (%distributed-retry-budget-update
         budget
         (lambda (state now)
           (declare (ignore now))
           (let* ((used (getf state :used))
                  (limit (retry-budget-limit budget)))
             (if (< used limit)
                 (values (list :window-start (getf state :window-start)
                               :used (1+ used))
                         t)
                 (values nil nil))))))))
  (cl-concurrent-kit:with-lock-held ((%retry-budget-lock budget))
    (%retry-budget-refresh!
     budget
     (%monotonic-seconds
      (%retry-budget-clock budget)
      (%retry-budget-monotonic-units-per-second budget)))
    (if (< (%retry-budget-used budget) (retry-budget-limit budget))
        (progn
          (incf (%retry-budget-used budget))
          t)
        nil)))

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
