(in-package #:resilience-kit)

(defun %queued-bulkhead-wait-remaining (active-token admission-deadline)
  (let* ((deadline-remaining
           (deadline-remaining
            :clock *resilience-clock*
            :monotonic-units-per-second
            *resilience-monotonic-units-per-second*))
         (local-remaining
           (and admission-deadline
                (- admission-deadline
                   (%now :clock *resilience-clock*
                         :monotonic-units-per-second
                         *resilience-monotonic-units-per-second*)))))
    (cond
      ((and deadline-remaining local-remaining)
       (min deadline-remaining local-remaining))
      (deadline-remaining deadline-remaining)
      (local-remaining local-remaining)
      (active-token 0.05d0)
      (t nil))))

(defun %queued-bulkhead-admit
    (bulkhead operation active-token admission-deadline)
  (let ((admitted-p nil)
        (observed-in-flight 0)
        (rejection nil)
        (queued-event-p nil))
    (let ((waiting-p nil))
      (labels ((release-waiter ()
                 ;; This function is called only while BULKHEAD's lock is
                 ;; held.  Clearing the flag makes cleanup idempotent across
                 ;; normal returns and non-local exits.
                 (when waiting-p
                   (decf (%queued-bulkhead-waiting bulkhead))
                   (setf waiting-p nil)))
               (set-rejection (reason)
                 (setf rejection
                       (%queued-bulkhead-rejection
                        operation
                        reason
                        (%queued-bulkhead-waiting bulkhead))))
               (wait-expired-p (remaining)
                 (and remaining (<= remaining 0d0))))
        (block nil
          (unwind-protect
               (progn
                 (with-lock-held ((%bulkhead-lock bulkhead))
                   (loop
                     (setf observed-in-flight (%bulkhead-in-flight bulkhead))
                     (cond
                       ((< observed-in-flight (bulkhead-limit bulkhead))
                        (incf (%bulkhead-in-flight bulkhead))
                        (setf admitted-p t)
                        (return))
                       ((not waiting-p)
                        (if (>= (%queued-bulkhead-waiting bulkhead)
                                (queued-bulkhead-max-queue bulkhead))
                            (progn
                              (set-rejection :queue-full)
                              (return))
                            (progn
                              (incf (%queued-bulkhead-waiting bulkhead))
                              (setf waiting-p t
                                    queued-event-p t))))
                       (t
                        (when active-token
                          (check-cancellation-token active-token))
                        (let ((remaining
                                (%queued-bulkhead-wait-remaining
                                 active-token admission-deadline)))
                          (when (wait-expired-p remaining)
                            (set-rejection :queue-timeout)
                            (return))
                          (unless (condition-wait
                                   (%queued-bulkhead-condition-variable bulkhead)
                                   (%bulkhead-lock bulkhead)
                                   :timeout remaining)
                            (when (wait-expired-p remaining)
                              (set-rejection :queue-timeout)
                              (return))))))))
                 (release-waiter))
            (when waiting-p
              (with-lock-held ((%bulkhead-lock bulkhead))
                (release-waiter)))))))
    (values admitted-p observed-in-flight rejection queued-event-p)))
