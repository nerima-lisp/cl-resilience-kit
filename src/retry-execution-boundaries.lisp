(in-package #:resilience-kit)

(declaim (inline %execute-attempt-without-cancellation*
                 %execute-attempt*))

(defun %execute-attempt-without-cancellation*
    (thunk clock units overall-deadline per-attempt-timeout operation attempt)
  (if (and (null overall-deadline)
           (null per-attempt-timeout))
      (funcall thunk)
      (let* ((started-at (%monotonic-seconds clock units))
             (attempt-deadline
               (and per-attempt-timeout (+ started-at per-attempt-timeout)))
             (effective-deadline
               (cond ((and overall-deadline attempt-deadline)
                      (min overall-deadline attempt-deadline))
                     (overall-deadline overall-deadline)
                     (attempt-deadline attempt-deadline)
                     (t nil)))
             (attempt-timeout-p
               (and attempt-deadline
                    (or (null overall-deadline)
                        (<= attempt-deadline overall-deadline)))))
        (when (and effective-deadline
                   (>= started-at effective-deadline))
          (if attempt-timeout-p
              (error (%make-attempt-timeout clock units effective-deadline
                                            per-attempt-timeout operation attempt
                                            :before-attempt))
              (%signal-deadline-exceeded clock units effective-deadline
                                         :operation operation
                                         :stage :before-attempt
                                         :attempt attempt)))
        ;; THUNK observes the attempt-scoped deadline (the earlier of the
        ;; overall and per-attempt deadlines) through *RESILIENCE-DEADLINE*,
        ;; not whatever wider deadline the caller bound around the whole
        ;; retry call.
        (let ((*resilience-deadline* effective-deadline))
          ;; Run post-call checks only after a normal return. A cleanup check
          ;; that signals here would otherwise mask the condition raised by
          ;; THUNK.
          (multiple-value-prog1
              (funcall thunk)
            (let ((finished-at (%monotonic-seconds clock units)))
              (when (and effective-deadline
                         (>= finished-at effective-deadline))
                (if attempt-timeout-p
                    (error (%make-attempt-timeout clock units effective-deadline
                                                  per-attempt-timeout operation attempt
                                                  :attempt))
                    (%signal-deadline-exceeded clock units effective-deadline
                                               :operation operation
                                               :stage :attempt
                                               :attempt attempt)))))))))

(defun %make-attempt-timeout
    (clock units deadline timeout operation attempt stage)
  (make-condition
   'attempt-timeout
   :message "The per-attempt timeout elapsed."
   :operation operation
   :deadline deadline
   :observed-at (%monotonic-seconds clock units)
   :stage stage
   :attempt attempt
   :timeout timeout))

(defun %execute-attempt*
    (thunk clock units overall-deadline per-attempt-timeout operation attempt)
  (%check-active-cancellation-token)
  (multiple-value-prog1
      (%execute-attempt-without-cancellation*
       thunk clock units overall-deadline per-attempt-timeout operation attempt)
    (%check-active-cancellation-token)))

(defun %execute-attempt
    (thunk clock units overall-deadline per-attempt-timeout operation attempt)
  (let ((*resilience-clock* clock)
        (*resilience-monotonic-units-per-second* units)
        (*resilience-deadline* overall-deadline))
    (%execute-attempt*
     thunk clock units overall-deadline per-attempt-timeout operation attempt)))

(defun %make-retry-exhausted
    (policy attempt last-condition last-result reason operation)
  (make-condition
   'retry-exhausted
   :message (if (= attempt 1)
                "Retry attempts were exhausted after 1 attempt."
                (concatenate 'string
                             "Retry attempts were exhausted after "
                             (write-to-string attempt)
                             " attempts."))
   :operation operation
   :attempts attempt
   :last-condition last-condition
   :last-result last-result
   :reason (or reason :max-attempts)
   :policy policy))

(defun %emit-retry-boundary-event
    (event-handler event operation attempt condition result reason clock units)
  (%emit-resilience-event*
   event-handler event operation attempt nil condition result nil reason
   nil nil nil nil
   clock units))

(setf (fdefinition '%ensure-retry-delay-before-deadline)
      (lambda (delay clock units deadline operation attempt)
        (when deadline
          (let ((remaining (- deadline (%monotonic-seconds clock units))))
            (when (>= delay (if (minusp remaining) 0d0 remaining))
              (%signal-deadline-exceeded clock units deadline
                                         :operation operation
                                         :stage :backoff
                                         :attempt attempt))))))

(defun %ensure-retry-budget-available
    (retry-budget policy attempt last-condition last-result operation
     event-handler clock units)
  (unless (or (null retry-budget)
              (%retry-budget-acquire retry-budget))
    (let ((condition
            (%make-retry-exhausted
             policy attempt last-condition last-result
             :retry-budget-exhausted operation)))
      (%emit-retry-boundary-event
       event-handler :retry-exhausted operation attempt condition nil
       :retry-budget-exhausted clock units)
      (error condition))))

(defun %retry-delay-or-deadline
    (policy attempt previous-delay decision clock units deadline sleeper
     operation last-condition last-result retry-budget event-handler)
  (let* ((delay (%delay-with-hint policy attempt previous-delay decision))
         (reason (retry-decision-reason decision)))
    (%ensure-retry-delay-before-deadline
     delay clock units deadline operation attempt)
    (%ensure-retry-budget-available
     retry-budget policy attempt last-condition last-result operation
     event-handler clock units)
    (%emit-retry-boundary-event
     event-handler :retry-scheduled operation attempt nil delay reason
     clock units)
    (%check-active-cancellation-token)
    (when (> delay 0d0)
      (%sleep sleeper delay))
    (%check-active-cancellation-token)
    delay))

(defun %invoke-retry-fallback
    (fallback condition event-handler operation attempt clock units)
  (if fallback
      (progn
        (%emit-retry-boundary-event
         event-handler :fallback operation attempt condition nil nil
         clock units)
        (funcall fallback condition))
      (error condition)))
