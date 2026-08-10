(in-package #:cl-resilience-kit)

(defun %plan-active-context (plan)
  "Build the dynamic context for PLAN without executing its operation."
  (let ((base-context
          (merge-resilience-context
           (current-resilience-context)
           (make-resilience-context
            :operation (resilience-plan-operation plan)
            :idempotency-key (resilience-plan-idempotency-key plan)))))
    (if (resilience-plan-context plan)
        (merge-resilience-context base-context
                                  (resilience-plan-context plan))
        base-context)))

(defun %run-resilience-base (plan active-token active-handler)
  "Run PLAN through the primitive resilience controls."
  (let ((operation-thunk
          (lambda ()
            (%call-with-resilience-core
             (resilience-plan-thunk plan)
             :retry-policy (resilience-plan-retry-policy plan)
             :circuit-breaker (resilience-plan-circuit-breaker plan)
             :bulkhead (resilience-plan-bulkhead plan)
             :bulkhead-timeout (resilience-plan-bulkhead-timeout plan)
             :rate-limiter (resilience-plan-rate-limiter plan)
             :rate-limit-tokens (resilience-plan-rate-limit-tokens plan)
             :rate-limit-wait-p (resilience-plan-rate-limit-wait-p plan)
             :rate-limit-max-wait (resilience-plan-rate-limit-max-wait plan)
             :rate-limit-signal-on-reject-p
             (resilience-plan-rate-limit-signal-on-reject-p plan)
             :overall-timeout (resilience-plan-overall-timeout plan)
             :overall-deadline (resilience-plan-overall-deadline plan)
             :per-attempt-timeout (resilience-plan-per-attempt-timeout plan)
             :clock (resilience-plan-clock plan)
             :monotonic-units-per-second
             (resilience-plan-monotonic-units-per-second plan)
             :sleeper (resilience-plan-sleeper plan)
             :operation (resilience-plan-operation plan)
             :retry-budget (resilience-plan-retry-budget plan)
             :cancellation-token active-token
             :event-handler active-handler
             :fallback (resilience-plan-fallback plan)))))
    (if (resilience-plan-distributed-circuit-breaker plan)
        (distributed-circuit-breaker-call
         (resilience-plan-distributed-circuit-breaker plan)
         operation-thunk
         :operation (resilience-plan-operation plan)
         :cancellation-token active-token
         :event-handler active-handler)
        (funcall operation-thunk))))

(defun %run-resilience-execution (plan active-token active-handler)
  "Select PLAN's outer execution boundary and run it."
  (let ((run-base
          (lambda ()
            (%run-resilience-base plan active-token active-handler))))
    (cond
      ((resilience-plan-request-coalescer plan)
       (call-with-request-coalescing
        (resilience-plan-request-coalescer plan) run-base
        :key (resilience-plan-idempotency-key plan)
        :idempotency-fingerprint
        (resilience-plan-idempotency-fingerprint plan)
        :executor (resilience-plan-executor plan)
        :hard-timeout (resilience-plan-hard-timeout plan)
        :timeout (or (resilience-plan-executor-timeout plan)
                     (resilience-plan-overall-timeout plan))
        :operation (resilience-plan-operation plan)
        :clock (resilience-plan-clock plan)
        :monotonic-units-per-second
        (resilience-plan-monotonic-units-per-second plan)))
      ((and (resilience-plan-max-hedge-attempts plan)
            (> (resilience-plan-max-hedge-attempts plan) 1))
       (call-with-hedging
        run-base
        :hedge-after (or (resilience-plan-hedge-after plan) 0d0)
        :max-attempts (resilience-plan-max-hedge-attempts plan)
        :executor (resilience-plan-executor plan)
        :hard-timeout (resilience-plan-hard-timeout plan)
        :hedge-safe-p (resilience-plan-hedge-safe-p plan)
        :idempotency-key (resilience-plan-idempotency-key plan)
        :cancellation-token active-token
        :operation (resilience-plan-operation plan)
        :clock (resilience-plan-clock plan)
        :monotonic-units-per-second
        (resilience-plan-monotonic-units-per-second plan)))
      ((resilience-plan-executor plan)
       (resilience-executor-call
        (resilience-plan-executor plan) run-base
        :hard-timeout (resilience-plan-hard-timeout plan)
        :timeout (or (resilience-plan-executor-timeout plan)
                     (resilience-plan-overall-timeout plan))
        :operation (resilience-plan-operation plan)
        :clock (resilience-plan-clock plan)
        :monotonic-units-per-second
        (resilience-plan-monotonic-units-per-second plan)))
      ((resilience-plan-hard-timeout plan)
       (%run-with-hard-timeout
        run-base
        (resilience-plan-hard-timeout plan)
        (resilience-plan-operation plan)
        :thread
        :clock (resilience-plan-clock plan)
        :monotonic-units-per-second
        (resilience-plan-monotonic-units-per-second plan)))
      (t
       (funcall run-base)))))

(defun %emit-resilience-operation-event
    (plan handler type context started &optional condition)
  "Emit the terminal composition event for PLAN."
  (let ((event-arguments
          (list :operation (resilience-plan-operation plan)
                :stage :composition
                :context context
                :duration (- (%now
                              :clock (resilience-plan-clock plan)
                              :monotonic-units-per-second
                              (resilience-plan-monotonic-units-per-second plan))
                             started)
                :clock (resilience-plan-clock plan)
                :monotonic-units-per-second
                (resilience-plan-monotonic-units-per-second plan))))
    (when condition
      (setf event-arguments
            (append event-arguments (list :condition condition))))
    (apply #'%emit-resilience-event handler type event-arguments)))

(defun %run-resilience-plan (plan)
  "Execute PLAN within its dynamic context and lifecycle boundary."
  (let* ((active-context (%plan-active-context plan))
         (active-handler
           (%combined-resilience-handler
            (resilience-plan-event-handler plan)
            (resilience-plan-metrics plan)
            (resilience-plan-observer plan)))
         (active-token
           (%active-cancellation-token
            (resilience-plan-cancellation-token plan)))
         (entered-p nil)
         (started (%now
                   :clock (resilience-plan-clock plan)
                   :monotonic-units-per-second
                   (resilience-plan-monotonic-units-per-second plan))))
    (when (resilience-plan-lifecycle plan)
      (enter-resilience-lifecycle
       (resilience-plan-lifecycle plan)
       :operation (resilience-plan-operation plan))
      (setf entered-p t))
    (unwind-protect
         (let ((*resilience-context* active-context)
               (*resilience-event-handler* active-handler))
           ;; Keep the success values in UNWIND-PROTECT's implicit value
           ;; preservation instead of collecting them in an &REST list.
           ;; The cleanup runs after HANDLER-CASE has left its handler
           ;; dynamic extent, so observer errors are not misclassified as
           ;; operation failures.
           (let ((operation-complete-p nil)
                 (operation-condition nil))
             (unwind-protect
                  (handler-case
                      (multiple-value-prog1
                          (%run-resilience-execution
                           plan active-token active-handler)
                        (setf operation-complete-p t))
                    (error (condition)
                      (setf operation-condition condition)
                      (values)))
               (if operation-complete-p
                   (%emit-resilience-operation-event
                    plan active-handler :operation-complete
                    active-context started)
                   (progn
                     (%emit-resilience-operation-event
                      plan active-handler :operation-failed
                      active-context started operation-condition)
                     (error operation-condition))))))
      (when entered-p
        (leave-resilience-lifecycle (resilience-plan-lifecycle plan))))))

(defun %run-resilience-plan/k (plan on-success on-error)
  "Execute PLAN and dispatch its terminal result to continuations.

The runtime owns operation error handling.  Continuations are invoked only
after that handler has exited, so a continuation error cannot be mistaken for
an operation failure and routed back to ON-ERROR."
  (let ((values nil)
        (condition nil)
        (failed-p nil))
    (handler-case
        (setf values (multiple-value-list (%run-resilience-plan plan)))
      (error (caught-condition)
        (setf condition caught-condition
              failed-p t)))
    (if failed-p
        (funcall on-error condition)
        (apply on-success values))))
