(in-package #:resilience-kit)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defparameter +resilience-option-specs+
    '((:retry-policy retry-policy)
      (:circuit-breaker circuit-breaker)
      (:distributed-circuit-breaker distributed-circuit-breaker)
      (:bulkhead bulkhead)
      (:bulkhead-timeout bulkhead-timeout)
      (:rate-limiter rate-limiter)
      (:rate-limit-tokens rate-limit-tokens 1d0)
      (:rate-limit-wait-p rate-limit-wait-p)
      (:rate-limit-max-wait rate-limit-max-wait)
      (:rate-limit-signal-on-reject-p rate-limit-signal-on-reject-p t)
      (:overall-timeout overall-timeout)
      (:overall-deadline overall-deadline)
      (:per-attempt-timeout per-attempt-timeout)
      (:clock clock)
      (:monotonic-units-per-second monotonic-units-per-second)
      (:sleeper sleeper)
      (:operation operation)
      (:retry-budget retry-budget)
      (:cancellation-token cancellation-token)
      (:event-handler event-handler)
      (:fallback fallback)
      (:context context)
      (:metrics metrics)
      (:observer observer)
      (:lifecycle lifecycle)
      (:executor executor)
      (:executor-timeout executor-timeout)
      (:hard-timeout hard-timeout)
      (:hedge-after hedge-after)
      (:max-hedge-attempts max-hedge-attempts)
      (:hedge-safe-p hedge-safe-p)
      (:request-coalescer request-coalescer)
      (:idempotency-key idempotency-key)
      (:idempotency-fingerprint idempotency-fingerprint)))

  (defparameter +resilience-option-keys+
    (mapcar #'first +resilience-option-specs+)))

(defun %make-resilience-plan-initargs (thunk options)
  "Return validated initargs for %MAKE-RESILIENCE-PLAN.

The runtime entry points accept dynamic keyword plists, so the validation
contract must stay explicit here instead of relying on macro-time expansion."
  (check-type thunk function)
  (%validate-static-plist-options options +resilience-option-keys+
                                  'call-with-resilience)
  (let ((context (getf options :context))
        (lifecycle (getf options :lifecycle))
        (executor (getf options :executor)))
    (when context
      (check-type context resilience-context))
    (when lifecycle
      (check-type lifecycle resilience-lifecycle))
    (when executor
      (check-type executor resilience-executor)))
  (let ((initargs (list :thunk thunk)))
    (dolist (spec +resilience-option-specs+)
      (destructuring-bind (keyword variable &optional default) spec
        (let* ((missing (list keyword))
               (value (getf options keyword missing)))
          (declare (ignore variable))
          (cond
            ((not (eq value missing))
             (setf initargs (append initargs (list keyword value))))
            ((not (null (cddr spec)))
             (setf initargs (append initargs (list keyword default))))))))
    initargs))

(defun %make-resilience-plan-from-options (thunk options)
  "Build a resilience plan from the keyword plist OPTIONS.

The keyword boundary is shared by the value-returning and CPS entry points so
both paths construct the same data before entering the runtime pipeline."
  (apply #'%make-resilience-plan
         (%make-resilience-plan-initargs thunk options)))

(defun call-with-resilience/k (thunk on-success on-error &rest options)
  "Call THUNK with resilience OPTIONS and dispatch its result.

All values from a successful call are passed to ON-SUCCESS.  A condition of
type ERROR signaled by the resilient call is passed as the sole argument to
ON-ERROR.  Errors raised by either continuation are intentionally allowed to
escape, so continuation failures are not mistaken for operation failures."
  (check-type thunk function)
  (check-type on-success function)
  (check-type on-error function)
  (%run-resilience-plan/k
   (%make-resilience-plan-from-options thunk options)
   on-success
   on-error))

(defun call-with-resilience (thunk &rest options)
  "Compose local and distributed resilience controls around THUNK.

The v2 composition boundary combines retry, bulkhead, rate-limit, and
circuit-breaker controls with optional execution-boundary controls.  EXECUTOR,
HARD-TIMEOUT, HEDGE-AFTER, and REQUEST-COALESCER require the explicit
idempotency and cancellation choices documented by their respective APIs.
OPTIONS is a keyword property list; its values are validated while the
resilience plan is built.  The WITH-RESILIENCE macro additionally validates
literal options during compilation."
  (check-type thunk function)
  (%run-resilience-plan
   (%make-resilience-plan-from-options thunk options)))
