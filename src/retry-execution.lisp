(in-package #:resilience-kit)

(declaim (inline %call-with-retry-single-pass))

(defun %call-with-retry-single-pass
    (thunk per-attempt-timeout operation active-clock units effective-deadline
     active-token)
  (if effective-deadline
      (let ((*resilience-clock* active-clock)
            (*resilience-monotonic-units-per-second* units)
            (*resilience-deadline* effective-deadline))
        (if active-token
            (let ((*resilience-cancellation-token* active-token))
              (%check-active-cancellation-token)
              (let ((now (%monotonic-seconds active-clock units)))
                (when (>= now effective-deadline)
                  (%signal-deadline-exceeded
                   active-clock units effective-deadline
                   :operation operation :stage :before-operation)))
              (%execute-attempt*
               thunk active-clock units effective-deadline
               per-attempt-timeout operation 1))
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

(defmacro with-retry
    ((policy &rest options) &body body)
  "Evaluate BODY through CALL-WITH-RETRY.

POLICY and OPTIONS are evaluated by CALL-WITH-RETRY in the surrounding
lexical environment.  The macro is the block-oriented counterpart of the
first-class CALL-WITH-RETRY function."
  `(call-with-retry ,policy (lambda () ,@body) ,@options))
