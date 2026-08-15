(in-package #:resilience-kit)

(defun %retry-budget-now (budget)
  (%monotonic-seconds
   (%retry-budget-clock budget)
   (%retry-budget-monotonic-units-per-second budget)))

(defun %make-distributed-retry-budget-state (window-start used)
  (cons window-start used))

(defun %distributed-retry-budget-state-window-start (state)
  (car state))

(defun %distributed-retry-budget-state-used (state)
  (cdr state))

(defun %distributed-retry-budget-state (budget value now)
  (let (window-start used)
    (cond
      ((consp value)
       (if (keywordp (car value))
           (when (%proper-list-p value)
             (setf window-start (getf value :window-start)
                   used (getf value :used)))
           (setf window-start (car value)
                 used (cdr value))))
      ((null value)))
    (if (and (%finite-real-p window-start)
             (integerp used)
             (>= used 0)
             (<= used (%retry-budget-limit budget)))
        (let ((effective-window-start (min (float window-start 1d0) now)))
          (if (>= (- now effective-window-start) (%retry-budget-window budget))
              (%make-distributed-retry-budget-state now 0)
              (%make-distributed-retry-budget-state effective-window-start used)))
        (if (null value)
            (%make-distributed-retry-budget-state now 0)
            (error 'resilience-store-error
                   :key (%distributed-retry-budget-key budget)
                   :message "The distributed retry budget record is malformed.
Expected a numeric window-start and a non-negative integer used value
that does not exceed the budget limit.")))))

(defun %distributed-retry-budget-read (budget)
  (let ((store (%distributed-retry-budget-store budget))
        (key (%distributed-retry-budget-key budget))
        (now (%retry-budget-now budget)))
    (multiple-value-bind (value version)
        (state-store-get store key)
      (values (%distributed-retry-budget-state budget value now)
              version
              now))))

(defun %distributed-retry-budget-write (budget state version)
  (let ((store (%distributed-retry-budget-store budget))
        (key (%distributed-retry-budget-key budget)))
    (state-store-put-if-version store key state version)))

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
         :key (%distributed-retry-budget-key budget)
         :message "The distributed retry budget could not commit after repeated store conflicts."))

(defun %retry-budget-refresh! (budget now)
  (when (>= (- now (%retry-budget-window-start budget))
            (%retry-budget-window budget))
    (setf (%retry-budget-window-start budget) now
          (%retry-budget-used budget) 0))
  budget)

(defun retry-budget-used (budget)
  "Return the number of retry tokens consumed in the active window."
  (check-type budget retry-budget)
  (when (typep budget 'distributed-retry-budget)
    (cl-concurrent-kit:with-lock-held ((%retry-budget-lock budget))
      (multiple-value-bind (state version)
          (%distributed-retry-budget-read budget)
        (declare (ignore version))
        (let ((used (%distributed-retry-budget-state-used state)))
          (return-from retry-budget-used used)))))
  (cl-concurrent-kit:with-lock-held ((%retry-budget-lock budget))
    (%retry-budget-refresh! budget (%retry-budget-now budget))
    (%retry-budget-used budget)))

(defun retry-budget-remaining (budget)
  "Return the number of retry tokens still available in the window."
  (check-type budget retry-budget)
  (when (typep budget 'distributed-retry-budget)
    (cl-concurrent-kit:with-lock-held ((%retry-budget-lock budget))
      (let ((limit (%retry-budget-limit budget)))
        (multiple-value-bind (state version)
            (%distributed-retry-budget-read budget)
          (declare (ignore version))
          (let ((used (%distributed-retry-budget-state-used state)))
            (return-from retry-budget-remaining
              (- limit used)))))))
  (cl-concurrent-kit:with-lock-held ((%retry-budget-lock budget))
    (let ((limit (%retry-budget-limit budget)))
      (%retry-budget-refresh! budget (%retry-budget-now budget))
      (- limit (%retry-budget-used budget)))))

(defun %retry-budget-acquire (budget)
  (when (typep budget 'distributed-retry-budget)
    (return-from %retry-budget-acquire
      (cl-concurrent-kit:with-lock-held ((%retry-budget-lock budget))
        (let ((limit (%retry-budget-limit budget)))
          (%distributed-retry-budget-update
           budget
           (lambda (state now)
             (declare (ignore now))
             (let* ((used (%distributed-retry-budget-state-used state))
                    (window-start
                      (%distributed-retry-budget-state-window-start state)))
               (if (< used limit)
                   (values (%make-distributed-retry-budget-state
                            window-start
                            (1+ used))
                           t)
                   (values nil nil)))))))))
  (cl-concurrent-kit:with-lock-held ((%retry-budget-lock budget))
    (let ((limit (%retry-budget-limit budget)))
      (%retry-budget-refresh! budget (%retry-budget-now budget))
      (if (< (%retry-budget-used budget) limit)
          (progn
            (incf (%retry-budget-used budget))
            t)
          nil))))

(defun retry-budget-acquire (budget)
  "Authorize one retry and consume one token, or return NIL when exhausted."
  (check-type budget retry-budget)
  (%retry-budget-acquire budget))
