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
    (let ((returned (multiple-value-list (funcall thunk))))
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
                                       :attempt attempt)))
      (apply #'values returned))))

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

(defun call-with-retry
    (policy thunk &key overall-timeout overall-deadline per-attempt-timeout
                         clock monotonic-units-per-second sleeper operation
                         retry-budget cancellation-token event-handler
                         fallback)
  "Call THUNK according to POLICY using cooperative injected boundaries.

OVERALL-TIMEOUT and OVERALL-DEADLINE bound the complete retry process.
PER-ATTEMPT-TIMEOUT bounds the checks around one invocation.  These checks
cannot interrupt arbitrary Lisp or an external call that does not cooperate;
use an operation-specific preemptive backend when actual interruption is
required.  A classifier receives `(condition-or-result attempt)` and must
explicitly opt in through a policy made with RETRY-SAFE-P."
  (check-type policy retry-policy)
  (check-type thunk function)
  (when retry-budget
    (check-type retry-budget retry-budget))
  (when cancellation-token
    (check-type cancellation-token cancellation-token))
  (when fallback
    (check-type fallback function))
  (when (and overall-timeout overall-deadline)
    (error "Specify OVERALL-TIMEOUT or OVERALL-DEADLINE, not both."))
  (when overall-timeout
    (setf overall-timeout
          (%ensure-non-negative-real overall-timeout "OVERALL-TIMEOUT")))
  (when overall-deadline
    (unless (%finite-real-p overall-deadline)
      (error "OVERALL-DEADLINE must be a finite real, got ~S."
             overall-deadline))
    (setf overall-deadline (float overall-deadline 1d0)))
  (when per-attempt-timeout
    (setf per-attempt-timeout
          (%ensure-non-negative-real per-attempt-timeout
                                     "PER-ATTEMPT-TIMEOUT")))
  (let* ((active-clock (%active-clock clock))
         (units (%active-monotonic-units-per-second
                 monotonic-units-per-second))
         (active-sleeper (%active-sleeper sleeper))
         (active-token (%active-cancellation-token cancellation-token))
         (active-handler (%active-event-handler event-handler))
         (started-at (%monotonic-seconds active-clock units))
         (requested-deadline
           (if overall-timeout
               (+ started-at overall-timeout)
               overall-deadline))
         (effective-deadline (%effective-deadline requested-deadline)))
    (let ((*resilience-clock* active-clock)
          (*resilience-monotonic-units-per-second* units)
          (*resilience-deadline* effective-deadline)
          (*resilience-cancellation-token* active-token)
          (*resilience-event-handler* active-handler))
      (labels ((handle-condition (attempt previous-delay condition)
                 (%emit-resilience-event
                  active-handler :attempt-failure
                  :operation operation :attempt attempt
                  :condition condition :clock active-clock
                  :monotonic-units-per-second units)
                 (let ((decision
                         (%retry-decision-for
                          policy attempt :condition condition)))
                   (if (retry-decision-retry-p decision)
                       (if (>= attempt (retry-policy-max-attempts policy))
                           (let ((exhausted
                                   (%make-retry-exhausted
                                    policy attempt condition nil
                                    (retry-decision-reason decision)
                                    operation)))
                             (%emit-resilience-event
                              active-handler :retry-exhausted
                              :operation operation :attempt attempt
                              :condition condition :reason
                              (retry-decision-reason decision)
                              :clock active-clock
                              :monotonic-units-per-second units)
                             (error exhausted))
                           (let ((delay
                                   (%retry-delay-or-deadline
                                    policy attempt previous-delay decision
                                    active-clock units effective-deadline
                                    active-sleeper operation condition nil
                                    retry-budget active-handler)))
                             (run-loop (1+ attempt) delay)))
                       (%invoke-retry-fallback
                        fallback condition active-handler operation attempt
                        active-clock units))))
               (run-loop (attempt previous-delay)
                 (handler-case
                     (progn
                       (%check-active-cancellation-token)
                       (%emit-resilience-event
                        active-handler :attempt-start
                        :operation operation :attempt attempt
                        :clock active-clock
                        :monotonic-units-per-second units)
                       (let* ((returned
                              (multiple-value-list
                               (%execute-attempt
                                thunk active-clock units effective-deadline
                                per-attempt-timeout operation attempt)))
                            (result (first returned))
                            (decision
                              (%retry-decision-for
                               policy attempt :result result)))
                       (if (retry-decision-retry-p decision)
                           (progn
                             (%emit-resilience-event
                              active-handler :attempt-failure
                              :operation operation :attempt attempt
                              :result result :reason
                              (retry-decision-reason decision)
                              :clock active-clock
                              :monotonic-units-per-second units)
                             (if (>= attempt (retry-policy-max-attempts policy))
                                 (let ((exhausted
                                         (%make-retry-exhausted
                                          policy attempt nil result
                                          (retry-decision-reason decision)
                                          operation)))
                                   (%emit-resilience-event
                                    active-handler :retry-exhausted
                                    :operation operation :attempt attempt
                                    :result result :reason
                                    (retry-decision-reason decision)
                                    :clock active-clock
                                    :monotonic-units-per-second units)
                                   (error exhausted))
                               (let ((delay
                                       (%retry-delay-or-deadline
                                        policy attempt previous-delay decision
                                        active-clock units effective-deadline
                                        active-sleeper operation nil result
                                        retry-budget active-handler)))
                                 (run-loop (1+ attempt) delay))))
                           (progn
                             (%emit-resilience-event
                              active-handler :attempt-success
                              :operation operation :attempt attempt
                              :result result :clock active-clock
                              :monotonic-units-per-second units)
                             (apply #'values returned)))))
                   ;; ATTEMPT-TIMEOUT is retryable only when the caller's
                   ;; classifier explicitly opts into retrying it.
                   (attempt-timeout (condition)
                     (handle-condition attempt previous-delay condition))
                   ;; The overall deadline is terminal for the complete
                   ;; operation; it is never converted into another attempt.
                   (deadline-exceeded (condition)
                     (error condition))
                   (resilience-cancelled (condition)
                     (error condition))
                   (retry-classifier-error (condition)
                     (error condition))
                   (error (condition)
                     (handle-condition attempt previous-delay condition)))))
        (handler-case
            (progn
              (%check-active-cancellation-token)
              (when (and effective-deadline
                         (>= (%monotonic-seconds active-clock units)
                             effective-deadline))
                (%signal-deadline-exceeded
                 active-clock units effective-deadline
                 :operation operation :stage :before-operation))
              (run-loop 1 nil))
          (retry-exhausted (condition)
            (%invoke-retry-fallback
             fallback condition active-handler operation
             (retry-exhausted-attempts condition)
             active-clock units))
          (deadline-exceeded (condition)
            (%emit-resilience-event
             active-handler :deadline-exceeded
             :operation operation :attempt
             (deadline-exceeded-attempt condition)
             :stage (deadline-exceeded-stage condition)
             :condition condition :clock active-clock
             :monotonic-units-per-second units)
            (%invoke-retry-fallback
             fallback condition active-handler operation nil
             active-clock units))
          (resilience-cancelled (condition)
            (%emit-resilience-event
             active-handler :cancelled
             :operation operation :condition condition
             :reason (resilience-cancelled-reason condition)
             :clock active-clock :monotonic-units-per-second units)
             (%invoke-retry-fallback
              fallback condition active-handler operation nil
             active-clock units))
          (retry-classifier-error (condition)
            (error condition)))))))

(defmacro with-retry
    ((policy &rest options) &body body)
  "Evaluate BODY through CALL-WITH-RETRY.

POLICY and OPTIONS are evaluated by CALL-WITH-RETRY in the surrounding
lexical environment.  The macro is the block-oriented counterpart of the
first-class CALL-WITH-RETRY function."
  `(call-with-retry ,policy (lambda () ,@body) ,@options))
