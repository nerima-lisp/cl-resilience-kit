(in-package #:resilience-kit)

(defun current-deadline ()
  "Return the innermost active monotonic deadline in seconds, or NIL."
  *resilience-deadline*)

(defun deadline-remaining
    (&key clock monotonic-units-per-second)
  "Return remaining monotonic seconds, clamped at zero, or NIL.

The clock's monotonic value is converted using
MONOTONIC-UNITS-PER-SECOND.  A fake boundary clock whose values are already
seconds therefore uses 1; the default real clock uses
INTERNAL-TIME-UNITS-PER-SECOND."
  (let ((deadline (current-deadline)))
    (when deadline
      (let* ((active-clock (%active-clock clock))
             (units (%active-monotonic-units-per-second
                     monotonic-units-per-second))
             (now (%monotonic-seconds active-clock units)))
        (max 0d0 (- deadline now))))))

(defun deadline-exceeded-p
    (&key clock monotonic-units-per-second)
  "Return true when the active monotonic deadline has elapsed."
  (let ((deadline (current-deadline)))
    (when deadline
      (let* ((active-clock (%active-clock clock))
             (units (%active-monotonic-units-per-second
                     monotonic-units-per-second)))
        (>= (%monotonic-seconds active-clock units)
            deadline)))))

(defun call-with-deadline
    (thunk &key timeout deadline clock monotonic-units-per-second operation
                  cancellation-token event-handler)
  "Call THUNK under a cooperative monotonic deadline.

TIMEOUT is a relative number of seconds.  DEADLINE is an absolute monotonic
value in the same normalized seconds used by CURRENT-DEADLINE.  The function
checks before and after THUNK; it cannot interrupt arbitrary Lisp code or
force an external operation to stop.  Use an operation-specific backend when
preemptive interruption is required."
  (check-type thunk function)
  (when cancellation-token
    (check-type cancellation-token cancellation-token))
  (when (and timeout deadline)
    (error "Specify either TIMEOUT or DEADLINE, not both."))
  (when timeout
    (setf timeout (%ensure-non-negative-real timeout "TIMEOUT")))
  (when deadline
    (unless (%finite-real-p deadline)
      (error "DEADLINE must be a finite real, got ~S." deadline))
    (setf deadline (float deadline 1d0)))
  (let* ((active-clock (%active-clock clock))
         (units (%active-monotonic-units-per-second
                 monotonic-units-per-second))
         (active-token (%active-cancellation-token cancellation-token))
         (active-handler (%active-event-handler event-handler))
         (now (%monotonic-seconds active-clock units))
         (requested-deadline (cond (timeout (+ now timeout))
                                   (deadline deadline)
                                   (t nil)))
         (effective-deadline (%effective-deadline requested-deadline)))
    (when active-token
      (check-cancellation-token active-token))
    (let ((*resilience-clock* active-clock)
          (*resilience-monotonic-units-per-second* units)
          (*resilience-deadline* effective-deadline)
          (*resilience-cancellation-token* active-token)
          (*resilience-event-handler* active-handler))
      (%check-active-cancellation-token)
      (when (and effective-deadline (>= now effective-deadline))
        (%emit-resilience-event
         active-handler :deadline-exceeded
         :operation operation :stage :before-operation
         :clock active-clock :monotonic-units-per-second units)
        (%signal-deadline-exceeded active-clock units effective-deadline
                                   :operation operation
                                   :stage :before-operation))
    ;; Run post-call checks only after a normal return. A cleanup check that
    ;; signals here would otherwise mask the condition raised by THUNK.
    (multiple-value-prog1
        (funcall thunk)
      (%check-active-cancellation-token)
      (when (and effective-deadline
                 (>= (%monotonic-seconds active-clock units)
                     effective-deadline))
        (%emit-resilience-event
         active-handler :deadline-exceeded
         :operation operation :stage :operation
         :clock active-clock :monotonic-units-per-second units)
        (%signal-deadline-exceeded active-clock units effective-deadline
                                   :operation operation
                                   :stage :operation))))))

(defmacro with-deadline ((&rest options) &body body)
  "Evaluate BODY under CALL-WITH-DEADLINE's cooperative deadline.

Use keyword arguments explicitly, for example
`(WITH-DEADLINE (:TIMEOUT 2d0 :CLOCK CLOCK) ...)` or
`(WITH-DEADLINE (:DEADLINE ABSOLUTE-VALUE) ...)`."
  `(call-with-deadline (lambda () ,@body)
                       ,@options))
