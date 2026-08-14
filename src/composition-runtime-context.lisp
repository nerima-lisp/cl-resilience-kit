(in-package #:resilience-kit)

(defun %plan-active-context (plan)
  "Build the dynamic context for PLAN without executing its operation."
  (let* ((operation (resilience-plan-operation plan))
         (idempotency-key (resilience-plan-idempotency-key plan))
         (context (resilience-plan-context plan))
         (current-context (current-resilience-context))
         (base-context
           (if (or operation idempotency-key)
               (%merge-resilience-context-operation
                current-context
                operation
                idempotency-key)
               current-context)))
    (if context
        (merge-resilience-context base-context context)
        base-context)))

(defun %emit-resilience-operation-event
    (plan handler metrics metrics-only-p type context started
     &optional condition)
  "Emit the terminal composition event for PLAN."
  (let* ((operation (resilience-plan-operation plan))
         (clock (resilience-plan-clock plan))
         (units (resilience-plan-monotonic-units-per-second plan))
         (finished-at (%monotonic-seconds clock units))
         (duration (- finished-at started)))
    (if metrics-only-p
        (%record-resilience-event* metrics
                                   type
                                   operation
                                   duration)
        (%emit-resilience-event* handler
                                 type
                                 operation
                                 nil
                                 :composition
                                 condition
                                 nil
                                 nil
                                 nil
                                 finished-at
                                 context
                                 nil
                                 duration
                                 clock
                                 units))))
