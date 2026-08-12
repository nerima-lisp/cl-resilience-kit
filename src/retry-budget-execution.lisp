(in-package #:resilience-kit)

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

(defun retry-budget-used (budget)
  "Return the number of retry tokens consumed in the active window."
  (check-type budget retry-budget)
  (when (typep budget 'distributed-retry-budget)
    (with-lock-held ((%retry-budget-lock budget))
      (multiple-value-bind (state version now)
          (%distributed-retry-budget-read budget)
        (declare (ignore version))
        (return-from retry-budget-used
          (getf (%distributed-retry-budget-state budget state now)
                :used)))))
  (with-lock-held ((%retry-budget-lock budget))
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
    (with-lock-held ((%retry-budget-lock budget))
      (multiple-value-bind (state version now)
          (%distributed-retry-budget-read budget)
        (declare (ignore version))
        (return-from retry-budget-remaining
          (- (retry-budget-limit budget)
             (getf (%distributed-retry-budget-state budget state now)
                   :used))))))
  (with-lock-held ((%retry-budget-lock budget))
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
      (with-lock-held ((%retry-budget-lock budget))
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
  (with-lock-held ((%retry-budget-lock budget))
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
