(in-package #:cl-resilience-kit)

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
        (admitted-p nil)
        (observed-in-flight 0))
    (when active-token
      (check-cancellation-token active-token))
    (cl-concurrent-kit:with-lock-held ((%bulkhead-lock bulkhead))
      (setf observed-in-flight (%bulkhead-in-flight bulkhead))
      (when (< observed-in-flight (bulkhead-limit bulkhead))
        (incf (%bulkhead-in-flight bulkhead))
        (setf admitted-p t)))
    (unless admitted-p
      (%emit-resilience-event
       active-handler :bulkhead-rejected
       :operation operation :reason :capacity
       :clock *resilience-clock*
       :monotonic-units-per-second
       *resilience-monotonic-units-per-second*)
      (error 'bulkhead-rejected
             :message "The bulkhead has no available capacity."
             :operation operation
             :limit (bulkhead-limit bulkhead)
             :in-flight observed-in-flight))
    (let ((*resilience-cancellation-token* active-token)
          (*resilience-event-handler* active-handler))
      (unwind-protect
           (progn
             ;; Keep the admission event inside the cleanup boundary. An
             ;; observer may intentionally leave through THROW, and the slot
             ;; must still be released.
             (%emit-resilience-event
              active-handler :bulkhead-admitted
              :operation operation :result t
              :clock *resilience-clock*
              :monotonic-units-per-second
              *resilience-monotonic-units-per-second*)
             (when active-token
               (check-cancellation-token active-token))
             (multiple-value-prog1
                 (funcall thunk)
               ;; Observe cancellation after a cooperative return while the
               ;; UNWIND-PROTECT still guarantees that the slot is released.
               (when active-token
                 (check-cancellation-token active-token))))
        (cl-concurrent-kit:with-lock-held ((%bulkhead-lock bulkhead))
          (decf (%bulkhead-in-flight bulkhead)))
        (%emit-resilience-event
         active-handler :bulkhead-released
         :operation operation
         :clock *resilience-clock*
         :monotonic-units-per-second
         *resilience-monotonic-units-per-second*)))))

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
         (started-at (%now :clock *resilience-clock*
                           :monotonic-units-per-second
                           *resilience-monotonic-units-per-second*))
         (admission-deadline (and timeout (+ started-at (float timeout 1d0))))
         (admitted-p nil)
         (observed-in-flight 0)
         (rejection nil)
         (queued-event-p nil))
    (when active-token
      (check-cancellation-token active-token))
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
               (cl-concurrent-kit:with-lock-held ((%bulkhead-lock bulkhead))
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
                            (setf rejection
                                  (%queued-bulkhead-rejection
                                   operation :queue-full
                                   (%queued-bulkhead-waiting bulkhead)))
                            (return))
                          (progn
                            (incf (%queued-bulkhead-waiting bulkhead))
                            (setf waiting-p t
                                  queued-event-p t))))
                     (t
                      (when active-token
                        (check-cancellation-token active-token))
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
                                             *resilience-monotonic-units-per-second*))))
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
                                 (%queued-bulkhead-waiting bulkhead)))
                          (return))
                        (unless (cl-concurrent-kit:condition-wait
                                 (%queued-bulkhead-condition-variable bulkhead)
                                 (%bulkhead-lock bulkhead)
                                 :timeout remaining)
                          (when (and remaining (<= remaining 0d0))
                            (setf rejection
                                  (%queued-bulkhead-rejection
                                   operation :queue-timeout
                                   (%queued-bulkhead-waiting bulkhead)))
                            (return)))))))
                 (release-waiter))
               ;; User callbacks must not run while the admission lock is
               ;; held; observers commonly inspect queue-size here.
               (when queued-event-p
                 (%emit-resilience-event
                  active-handler :bulkhead-queued
                  :operation operation
                  :clock *resilience-clock*
                  :monotonic-units-per-second
                  *resilience-monotonic-units-per-second*)))
          (when waiting-p
            (cl-concurrent-kit:with-lock-held ((%bulkhead-lock bulkhead))
              (release-waiter))))))
    (unless admitted-p
      (%emit-resilience-event
       active-handler :bulkhead-rejected
       :operation operation
       :condition rejection
       :reason (and rejection
                    (resilience-execution-rejected-reason rejection))
       :clock *resilience-clock*
       :monotonic-units-per-second
       *resilience-monotonic-units-per-second*)
      (error rejection))
    (let ((*resilience-cancellation-token* active-token)
          (*resilience-event-handler* active-handler))
      (unwind-protect
           (progn
             (%emit-resilience-event
              active-handler :bulkhead-admitted
              :operation operation :result t
              :clock *resilience-clock*
              :monotonic-units-per-second
              *resilience-monotonic-units-per-second*)
             (when active-token
               (check-cancellation-token active-token))
             (multiple-value-prog1
                 (funcall thunk)
               (when active-token
                 (check-cancellation-token active-token))))
        (cl-concurrent-kit:with-lock-held ((%bulkhead-lock bulkhead))
          (decf (%bulkhead-in-flight bulkhead))
          (cl-concurrent-kit:condition-notify
           (%queued-bulkhead-condition-variable bulkhead)))
        (%emit-resilience-event
         active-handler :bulkhead-released
         :operation operation
         :clock *resilience-clock*
         :monotonic-units-per-second
         *resilience-monotonic-units-per-second*)))))
