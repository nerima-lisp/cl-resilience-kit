(in-package #:cl-resilience-kit)

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
                       (if (and (null active-handler)
                                (or (not (retry-policy-retry-safe-p policy))
                                    (null (retry-policy-result-classifier
                                           policy))))
                           (%execute-attempt
                            thunk active-clock units effective-deadline
                            per-attempt-timeout operation attempt)
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
                                   (if (>= attempt
                                           (retry-policy-max-attempts policy))
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
                                   (apply #'values returned))))))
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
