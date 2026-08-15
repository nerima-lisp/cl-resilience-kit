(in-package #:resilience-kit)

(declaim (inline %call-with-retry-single-pass))

(defun %call-with-retry-single-pass
    (thunk per-attempt-timeout operation active-clock units effective-deadline
     active-token)
  (if effective-deadline
      (let ((*resilience-clock* active-clock)
            (*resilience-monotonic-units-per-second* units)
            (*resilience-deadline* effective-deadline)
            (*resilience-cancellation-token* active-token))
        (%check-active-cancellation-token)
        (let ((now (%monotonic-seconds active-clock units)))
          (when (>= now effective-deadline)
            (%signal-deadline-exceeded
             active-clock units effective-deadline
             :operation operation :stage :before-operation)))
        (if active-token
            (%execute-attempt*
             thunk active-clock units effective-deadline
             per-attempt-timeout operation 1)
            (%execute-attempt-without-cancellation*
             thunk active-clock units effective-deadline
             per-attempt-timeout operation 1)))
      (let ((*resilience-clock* active-clock)
            (*resilience-monotonic-units-per-second* units))
        (if active-token
            (let ((*resilience-cancellation-token* active-token))
              (%check-active-cancellation-token)
              (multiple-value-prog1
                  (funcall thunk)
                (%check-active-cancellation-token)))
            (funcall thunk)))))

(defun %call-with-retry-runtime
    (policy thunk per-attempt-timeout active-sleeper track-results-p
     max-attempts effective-deadline retry-budget fallback active-handler
     operation active-clock units)
  "Run the multi-attempt retry loop for %CALL-WITH-RETRY*.

Expects *RESILIENCE-CLOCK*, *RESILIENCE-MONOTONIC-UNITS-PER-SECOND*,
*RESILIENCE-DEADLINE*, *RESILIENCE-CANCELLATION-TOKEN*, and
*RESILIENCE-EVENT-HANDLER* already bound by the caller; this function does
not rebind them. TRACK-RESULTS-P and MAX-ATTEMPTS are the caller's
precomputed mirrors of what %RUN-RETRY-ATTEMPT and %RETRY-EXHAUSTED-P
re-derive from POLICY themselves, so they are accepted for interface
symmetry with the caller and are not consulted here."
  (declare (ignore track-results-p max-attempts))
  (labels ((run-loop (attempt previous-delay)
             (handler-case
                 (%run-retry-attempt
                  #'run-loop policy thunk attempt previous-delay
                  active-clock units effective-deadline
                  per-attempt-timeout active-sleeper operation
                  retry-budget active-handler)
               ;; ATTEMPT-TIMEOUT is retryable only when the caller's
               ;; classifier explicitly opts into retrying it.
               (attempt-timeout (condition)
                 (%handle-retry-condition
                  #'run-loop policy attempt previous-delay condition
                  active-clock units effective-deadline
                  active-sleeper operation retry-budget
                  active-handler fallback))
               ;; The overall deadline is terminal for the complete
               ;; operation; it is never converted into another attempt.
               (deadline-exceeded (condition)
                 (error condition))
               (resilience-cancelled (condition)
                 (error condition))
               (retry-classifier-error (condition)
                 (error condition))
               (error (condition)
                 (%handle-retry-condition
                  #'run-loop policy attempt previous-delay condition
                  active-clock units effective-deadline
                  active-sleeper operation retry-budget
                  active-handler fallback)))))
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
        (error condition)))))

(defun %call-with-retry*
    (policy thunk overall-timeout overall-deadline per-attempt-timeout
     clock monotonic-units-per-second sleeper operation retry-budget
     cancellation-token event-handler fallback)
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
         (active-token (%active-cancellation-token cancellation-token))
         (active-handler (%active-event-handler event-handler))
         (max-attempts (%retry-policy-max-attempts policy))
         (result-classifier (%retry-policy-result-classifier policy))
         (retry-safe-p (%retry-policy-retry-safe-p policy))
         (track-results-p
           (or active-handler
               (and retry-safe-p result-classifier)))
         (requested-deadline
           (cond (overall-timeout
                  (+ (%monotonic-seconds active-clock units)
                     overall-timeout))
                 (overall-deadline overall-deadline)
                 (t nil)))
         (effective-deadline (%effective-deadline requested-deadline)))
    (if (and (= max-attempts 1)
             (not retry-safe-p)
             (null active-handler)
             (null fallback)
             (null retry-budget))
        (%call-with-retry-single-pass
         thunk per-attempt-timeout operation active-clock units
         effective-deadline active-token)
        (let ((active-sleeper (%active-sleeper sleeper))
              (*resilience-clock* active-clock)
              (*resilience-monotonic-units-per-second* units)
              (*resilience-deadline* effective-deadline)
              (*resilience-cancellation-token* active-token)
              (*resilience-event-handler* active-handler))
          (%call-with-retry-runtime
           policy thunk per-attempt-timeout active-sleeper track-results-p
           max-attempts effective-deadline retry-budget fallback active-handler
           operation active-clock units)))))

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
  (%call-with-retry*
   policy thunk overall-timeout overall-deadline per-attempt-timeout
   clock monotonic-units-per-second sleeper operation retry-budget
   cancellation-token event-handler fallback))

