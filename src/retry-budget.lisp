(in-package #:cl-resilience-kit)

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
  (let ((window-start (and (%proper-list-p value)
                           (getf value :window-start)))
        (used (and (%proper-list-p value) (getf value :used))))
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
