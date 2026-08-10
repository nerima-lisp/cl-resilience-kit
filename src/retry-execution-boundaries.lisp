(in-package #:cl-resilience-kit)

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

(defun %execute-attempt
    (thunk clock units overall-deadline per-attempt-timeout operation attempt)
  (%check-active-cancellation-token)
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
                    (<= attempt-deadline overall-deadline))))
         (*resilience-clock* clock)
         (*resilience-monotonic-units-per-second* units)
         (*resilience-deadline* effective-deadline))
    (when (and effective-deadline
               (>= (%monotonic-seconds clock units) effective-deadline))
      (if attempt-timeout-p
          (error (%make-attempt-timeout clock units effective-deadline
                                        per-attempt-timeout operation attempt
                                        :before-attempt))
          (%signal-deadline-exceeded clock units effective-deadline
                                     :operation operation
                                     :stage :before-attempt
                                     :attempt attempt)))
    ;; Run post-call checks only after a normal return. A cleanup check that
    ;; signals here would otherwise mask the condition raised by THUNK.
    (multiple-value-prog1
        (funcall thunk)
      (%check-active-cancellation-token)
      (when (and effective-deadline
                 (>= (%monotonic-seconds clock units) effective-deadline))
        (if attempt-timeout-p
            (error (%make-attempt-timeout clock units effective-deadline
                                          per-attempt-timeout operation attempt
                                          :attempt))
            (%signal-deadline-exceeded clock units effective-deadline
                                       :operation operation
                                       :stage :attempt
                                       :attempt attempt))))))

(defun %make-retry-exhausted
    (policy attempt last-condition last-result reason operation)
  (make-condition
   'retry-exhausted
   :message (format nil "Retry attempts were exhausted after ~D attempt~:P."
                    attempt)
   :operation operation
   :attempts attempt
   :last-condition last-condition
   :last-result last-result
   :reason (or reason :max-attempts)
   :policy policy))

(defun %retry-delay-or-deadline
    (policy attempt previous-delay decision clock units deadline sleeper
     operation last-condition last-result retry-budget event-handler)
  (let ((delay (%delay-with-hint policy attempt previous-delay decision)))
    (when (and deadline
               (>= delay
                   (max 0d0 (- deadline (%monotonic-seconds clock units)))))
      (%signal-deadline-exceeded clock units deadline
                                 :operation operation
                                 :stage :backoff
                                 :attempt attempt))
    (unless (or (null retry-budget)
                (retry-budget-acquire retry-budget))
      (let ((condition
              (%make-retry-exhausted
               policy attempt last-condition last-result
               :retry-budget-exhausted operation)))
        (%emit-resilience-event
         event-handler :retry-exhausted
         :operation operation :attempt attempt :condition condition
         :reason :retry-budget-exhausted :clock clock
         :monotonic-units-per-second units)
        (error condition)))
    (%emit-resilience-event
     event-handler :retry-scheduled
     :operation operation :attempt attempt :delay delay
     :reason (retry-decision-reason decision)
     :clock clock :monotonic-units-per-second units)
    (%check-active-cancellation-token)
    (when (> delay 0d0)
      (%sleep sleeper delay))
    (%check-active-cancellation-token)
    delay))

(defun %invoke-retry-fallback
    (fallback condition event-handler operation attempt clock units)
  (if fallback
      (progn
        (%emit-resilience-event
         event-handler :fallback
         :operation operation :attempt attempt :condition condition
         :clock clock :monotonic-units-per-second units)
        (funcall fallback condition))
      (error condition)))
