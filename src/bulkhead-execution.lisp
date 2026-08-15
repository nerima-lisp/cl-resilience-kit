(in-package #:resilience-kit)

(defun %emit-bulkhead-event
    (active-handler event operation result condition reason clock units)
  (%emit-resilience-event*
   active-handler event operation nil :bulkhead condition result nil reason
   nil nil nil nil
   clock units))

(defun bulkhead-call
    (bulkhead thunk &key operation cancellation-token event-handler timeout)
  "Admit THUNK when a slot is available, releasing it on every exit path."
  (check-type bulkhead bulkhead)
  (check-type thunk function)
  (when (typep bulkhead 'queued-bulkhead)
    (return-from bulkhead-call
      (queued-bulkhead-call
       bulkhead thunk
       :operation operation
       :cancellation-token cancellation-token
       :event-handler event-handler
       :timeout timeout)))
  (when cancellation-token
    (check-type cancellation-token cancellation-token))
  (let ((active-token (%active-cancellation-token cancellation-token))
        (active-handler (%active-event-handler event-handler))
        (lock (%bulkhead-lock bulkhead))
        (limit (%bulkhead-limit bulkhead))
        (clock (%active-clock *resilience-clock*))
        (units (%active-monotonic-units-per-second
                *resilience-monotonic-units-per-second*))
        (admitted-p nil)
        (observed-in-flight 0))
    (when active-token
      (%check-cancellation-token active-token))
    (cl-concurrent-kit:with-lock-held (lock)
      (setf observed-in-flight (%bulkhead-in-flight bulkhead))
      (when (< observed-in-flight limit)
        (incf (%bulkhead-in-flight bulkhead))
        (setf admitted-p t)))
    (unless admitted-p
      (%emit-bulkhead-event
       active-handler :bulkhead-rejected operation nil nil :capacity clock units)
      (error 'bulkhead-rejected
             :message "The bulkhead has no available capacity."
             :operation operation
             :limit limit
             :in-flight observed-in-flight))
    (let ((*resilience-cancellation-token* active-token)
          (*resilience-event-handler* active-handler))
      (unwind-protect
           (progn
             ;; Keep the admission event inside the cleanup boundary. An
             ;; observer may intentionally leave through THROW, and the slot
             ;; must still be released.
             (%emit-bulkhead-event
              active-handler :bulkhead-admitted operation t nil nil clock units)
             (when active-token
               (%check-cancellation-token active-token))
             (multiple-value-prog1
                 (funcall thunk)
               ;; Observe cancellation after a cooperative return while the
               ;; UNWIND-PROTECT still guarantees that the slot is released.
               (when active-token
                 (%check-cancellation-token active-token))))
        (cl-concurrent-kit:with-lock-held (lock)
          (decf (%bulkhead-in-flight bulkhead)))
        (%emit-bulkhead-event
         active-handler :bulkhead-released operation nil nil nil clock units)))))

(defun queued-bulkhead-call
    (bulkhead thunk &key operation cancellation-token event-handler timeout)
  "Wait for a queued bulkhead slot, then call THUNK in the caller thread.

TIMEOUT is a relative number of seconds for admission only.  The active
resilience deadline, when present, is also honored.  Queueing does not create
worker threads and does not cancel a call that has already been admitted."
  (check-type bulkhead queued-bulkhead)
  (check-type thunk function)
  (when timeout
    (%ensure-non-negative-real timeout "TIMEOUT"))
  (when cancellation-token
    (check-type cancellation-token cancellation-token))
  (let* ((active-token (%active-cancellation-token cancellation-token))
         (active-handler (%active-event-handler event-handler))
         (lock (%bulkhead-lock bulkhead))
         (condition-variable (%queued-bulkhead-condition-variable bulkhead))
         (limit (%bulkhead-limit bulkhead))
         (max-queue (%queued-bulkhead-max-queue bulkhead))
         (clock (%active-clock *resilience-clock*))
         (units (%active-monotonic-units-per-second
                 *resilience-monotonic-units-per-second*))
         (started-at (and timeout
                          (%monotonic-seconds clock units)))
         (admission-deadline (and timeout (+ started-at (float timeout 1d0))))
         (deadline *resilience-deadline*)
         (admitted-p nil)
         (observed-in-flight 0)
         (rejection nil)
         (queued-event-p nil))
    (when active-token
      (%check-cancellation-token active-token))
    (let ((waiting-p nil))
      (labels ((release-waiter ()
                 ;; This function is called only while BULKHEAD's lock is
                 ;; held.  Clearing the flag makes cleanup idempotent across
                 ;; normal returns and non-local exits.
                 (when waiting-p
                   (decf (%queued-bulkhead-waiting bulkhead))
                   (setf waiting-p nil))))
        (unwind-protect
             (progn
               (cl-concurrent-kit:with-lock-held (lock)
                 (loop
                   (let ((waiting-count (%queued-bulkhead-waiting bulkhead)))
                     (setf observed-in-flight (%bulkhead-in-flight bulkhead))
                     (cond
                       ((< observed-in-flight limit)
                        (incf (%bulkhead-in-flight bulkhead))
                        (setf admitted-p t)
                        (return))
                       ((not waiting-p)
                        (if (>= waiting-count max-queue)
                            (progn
                              (setf rejection
                                    (%queued-bulkhead-rejection
                                     operation :queue-full
                                     waiting-count))
                              (return))
                            (progn
                              (incf (%queued-bulkhead-waiting bulkhead))
                              (setf waiting-p t
                                    queued-event-p t))))
                       (t
                        (when active-token
                          (%check-cancellation-token active-token))
                        (let* ((timed-wait-p
                                 (or deadline admission-deadline))
                               (now (and timed-wait-p
                                         (%monotonic-seconds clock units)))
                               (deadline-remaining
                                 (and deadline
                                      now
                                      (%deadline-remaining-at
                                       now
                                       deadline)))
                               (local-remaining
                                 (and now
                                      admission-deadline
                                      (- admission-deadline now)))
                               (remaining
                                 (cond
                                   ((and deadline-remaining local-remaining)
                                    (min deadline-remaining local-remaining))
                                   (deadline-remaining deadline-remaining)
                                   (local-remaining local-remaining)
                                   (active-token 0.05d0)
                                   (t nil))))
                          (when (and remaining (<= remaining 0d0))
                            (setf rejection
                                  (%queued-bulkhead-rejection
                                   operation :queue-timeout
                                   waiting-count))
                            (return))
                          (unless (cl-concurrent-kit:condition-wait
                                   condition-variable
                                   lock
                                   :timeout remaining)
                            (when (and remaining (<= remaining 0d0))
                              (setf rejection
                                    (%queued-bulkhead-rejection
                                     operation :queue-timeout
                                     waiting-count))
                              (return))))))))
                 (release-waiter))
               ;; User callbacks must not run while the admission lock is
               ;; held; observers commonly inspect queue-size here.
               (when queued-event-p
                 (%emit-bulkhead-event
                  active-handler :bulkhead-queued operation nil nil nil clock units)))
          (when waiting-p
            (cl-concurrent-kit:with-lock-held (lock)
              (release-waiter))))))
    (unless admitted-p
      (%emit-bulkhead-event
       active-handler :bulkhead-rejected operation nil rejection
       (and rejection
            (resilience-execution-rejected-reason rejection))
       clock units)
      (error rejection))
    (let ((*resilience-cancellation-token* active-token)
          (*resilience-event-handler* active-handler))
      (unwind-protect
           (progn
             (%emit-bulkhead-event
              active-handler :bulkhead-admitted operation t nil nil clock units)
             (when active-token
               (%check-cancellation-token active-token))
             (multiple-value-prog1
                 (funcall thunk)
               (when active-token
                 (%check-cancellation-token active-token))))
        (cl-concurrent-kit:with-lock-held (lock)
          (decf (%bulkhead-in-flight bulkhead))
          (cl-concurrent-kit:condition-notify
           condition-variable))
        (%emit-bulkhead-event
         active-handler :bulkhead-released operation nil nil nil clock units)))))
